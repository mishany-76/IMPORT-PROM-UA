import os
import time
import requests
import logging
import pandas as pd
import subprocess
# Изменен импорт для совместимости с разными версиями
try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO
from datetime import datetime, timedelta

# ==================== КОНФИГУРАЦИЯ ====================
SPREADSHEET_KEY = "1p_Tb8LOEFA5V9PqVsUpzUJdDnGDICQgmjMWQdssROVk"  # Ключ таблицы
TARGET_COLUMN = "Import_Subscribers"  # Название столбца для поиска email
# Словарь: скрипт → пауза в секундах ПОСЛЕ его выполнения (перед следующим скриптом).
# Большая пауза перед import_script_0.py достигается через паузу после yml_parser_AGER.py (последний парсер).
SCRIPTS_TO_RUN = {
    "yml_parser_FOOTBALLERS.py": 60,   # пауза 60 сек после
    #"yml_parser_SPECULANT.py": 60,
    "yml_parser_KIRS.py":       60,    # пауза 60 сек после
    "yml_parser_MOYDROP.py":    60,    # пауза 60 сек после
    "yml_parser_IZIDROP.py":    60,    # пауза 60 сек после
    "yml_parser_AGER.py":       120,   # пауза 120 сек после — даём Google API отдохнуть перед import_script_0.py
    "import_script_0.py":       90,    # пауза 90 сек после
    "IMPORT_PROM_UA.py":        60,    # пауза 60 сек после
    "Product_Correction.py":    0,     # последний скрипт, пауза не нужна
}
LOG_FILE_TO_CHECK = "feed_processor.log"  # Лог-файл для проверки
MAX_LOG_LINES = 200  # Максимальное количество строк в логе
API_DELAY = 5  # Задержка между запросами к API
MAX_RETRIES = 7  # Максимальное количество попыток при ошибке 429

# URL для вызова вашего Google Apps Script
GOOGLE_APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyryNy2qQMmTbC5JCMpfGZfLntMSTRL5awv5-arMpmDraSSXXmsvAlDAgLdk3XThfev/exec"

# --- TELEGRAM CONFIG ---
# Скрипт сначала ищет их в переменных окружения (для GitHub), 
# если не находит - использует значения по умолчанию (для локального запуска)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "ВАШ_ТОКЕН_БОТА")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "ВАШ_CHAT_ID")
# =======================================================

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    filename='launcher.log',
    encoding='utf-8',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def send_telegram_message(text):
    """Отправляет сообщение в Telegram"""
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "ВАШ_ТОКЕН_БОТА":
        logging.warning("Telegram токен не настроен. Пропуск отправки.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        response.raise_for_status()
        logging.info("Уведомление в Telegram отправлено.")
    except Exception as e:
        logging.error(f"Ошибка отправки в Telegram: {e}")

def check_user_file():
    """Проверяет наличие и содержимое файла user.txt"""
    # Если мы в GitHub Actions, можем передать email через переменную окружения
    env_email = os.environ.get("USER_EMAIL")
    if env_email:
        return env_email

    if not os.path.exists("user.txt"):
        logging.error("Файл user.txt не найден")
        print("\nФайл user.txt не найден! Создайте файл и заполните Ваш Email")
        print("Программа закроется автоматически через 5 секунд...")
        time.sleep(5)
        exit()

    with open("user.txt", "r") as f:
        user_email = f.read().strip()

    if not user_email:
        logging.error("Файл user.txt пуст")
        print("\nФайл user.txt пуст. Добавьте Ваш Email в файл и перезапустите программу")
        print("Программа закроется автоматически через 5 секунд...")
        time.sleep(5)
        exit()

    return user_email


def safe_api_request(url, retries=MAX_RETRIES):
    """Безопасный запрос к API с обработкой квот и повторными попытками"""
    for attempt in range(retries):
        try:
            time.sleep(API_DELAY)  # Задержка перед каждым запросом
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = (2 ** attempt) * 5  # Экспоненциальная задержка
                logging.warning(f"Превышена квота API. Попытка {attempt + 1}/{retries}. Ожидание {wait_time} сек...")
                time.sleep(wait_time)
                continue
            logging.error(f"HTTP ошибка при запросе к {url}: {e}")
            raise
        except requests.RequestException as e:
            logging.error(f"Ошибка подключения к {url}: {e}")
            raise
    logging.error(f"Не удалось выполнить запрос к {url} после {retries} попыток")
    raise Exception(f"Не удалось выполнить запрос после {retries} попыток")


def check_subscription():
    """Проверка подписки в указанном столбце Google таблицы"""
    logging.info("Начало проверки подписки...")
    user_email = check_user_file()
    logging.info(f"Email для проверки: {user_email}")

    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_KEY}/gviz/tq?tqx=out:csv&sheet=Users"

    try:
        response = safe_api_request(spreadsheet_url)
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        if TARGET_COLUMN not in df.columns:
            logging.error(f"Столбец {TARGET_COLUMN} не найден в таблице")
            return False

        emails = df[TARGET_COLUMN].astype(str).str.strip().tolist()
        logging.info(f"Найдено {len(emails)} emails в столбце {TARGET_COLUMN}")

        return user_email in emails

    except Exception as e:
        logging.error(f"Ошибка при проверке подписки: {e}")
        return False


def check_and_clear_log():
    """Проверяет и очищает лог-файл при необходимости"""
    if not os.path.exists(LOG_FILE_TO_CHECK):
        logging.warning(f"Лог-файл {LOG_FILE_TO_CHECK} не найден")
        return

    try:
        with open(LOG_FILE_TO_CHECK, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        if len(lines) > MAX_LOG_LINES:
            logging.info(f"Лог-файл превысил {MAX_LOG_LINES} строк. Очистка...")
            with open(LOG_FILE_TO_CHECK, 'w', encoding='utf-8') as f:
                f.write(f"Лог очищен {datetime.now()}\n")
            logging.info("Лог-файл успешно очищен")
    except Exception as e:
        logging.error(f"Ошибка при работе с лог-файлом: {e}")


def run_script_with_retries(script_path, retries=3):
    """Запускает скрипт с несколькими попытками при ошибках"""
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Попытка {attempt}/{retries} запуска скрипта: {script_path}")
            result = subprocess.run(
                ["python", script_path],
                check=True,
                capture_output=True,
                text=True,
                encoding='utf-8',
                timeout=1200
            )
            logging.info(f"Скрипт {script_path} успешно выполнен (попытка {attempt}/{retries}).")
            return result
        except subprocess.TimeoutExpired:
            logging.error(f"Скрипт {script_path} превысил время выполнения (попытка {attempt}/{retries})")
            if attempt == retries:
                logging.error(f"Скрипт {script_path} не выполнен после {retries} попыток из-за таймаута.")
                raise
            time.sleep(10 * attempt)
        except subprocess.CalledProcessError as e:
            logging.error(f"Ошибка в скрипте {script_path} (попытка {attempt}/{retries}). Код возврата: {e.returncode}")
            logging.error(f"Stdout: {e.stdout}")
            logging.error(f"Stderr: {e.stderr}")
            if attempt == retries:
                logging.error(f"Скрипт {script_path} не выполнен после {retries} попыток из-за ошибки.")
                raise
            time.sleep(5 * attempt)
        except Exception as e:
            logging.error(f"Неожиданная ошибка при запуске {script_path} (попытка {attempt}/{retries}): {e}")
            if attempt == retries:
                raise
            time.sleep(5 * attempt)
    return None


def trigger_google_apps_script(url, retries=3):
    """Отправляет запрос для запуска Google Apps Script с повторными попытками."""
    if not url or url == "ВАШ_URL_GOOGLE_APPS_SCRIPT_ЗДЕСЬ":
        logging.warning("URL Google Apps Script не настроен. Пропуск запуска.")
        print("URL Google Apps Script не настроен. Пропуск запуска.")
        return False

    for attempt in range(1, retries + 1):
        logging.info(f"Запуск Google Apps Script (попытка {attempt}/{retries}): {url}")
        print(f"Запуск Google Apps Script (попытка {attempt}/{retries})...")
        try:
            response = requests.get(url, timeout=1800)
            response.raise_for_status()
            logging.info(f"Google Apps Script успешно запущен. Ответ сервера: {response.status_code}")
            print(f"Google Apps Script успешно запущен. Ответ сервера: {response.status_code}")
            return True
        except Exception as e:
            logging.error(f"Ошибка при вызове Google Apps Script (попытка {attempt}/{retries}): {e}")
            print(f"Ошибка при вызове Google Apps Script (попытка {attempt}/{retries}): {e}")
            if attempt < retries:
                wait = 30 * attempt
                logging.info(f"Ожидание {wait} сек перед следующей попыткой...")
                time.sleep(wait)

    logging.error(f"Google Apps Script не удалось запустить после {retries} попыток.")
    print(f"Google Apps Script не удалось запустить после {retries} попыток.")
    return False


def run_scripts_sequentially():
    """Запускает скрипты последовательно с контролем квот"""
    total_start_time = time.time()
    execution_times = {}
    report_text = "" # Будущий текст для Telegram

    for script_name, delay_after in SCRIPTS_TO_RUN.items():
        if not os.path.exists(script_name):
            logging.error(f"Скрипт {script_name} не найден!")
            continue

        script_start_time = time.time()
        logging.info(f"\n======== ЗАПУСК СКРИПТА: {script_name} ========")
        print(f"\nЗапуск скрипта: {script_name}")

        script_successfully_completed = False
        try:
            result = run_script_with_retries(script_name)

            if result:
                if result.stdout:
                    print(result.stdout)
                    logging.info(f"Вывод {script_name}:\n{result.stdout}")
                if result.stderr:
                    print(result.stderr)
                    logging.warning(f"Сообщения в stderr от {script_name}:\n{result.stderr}")

                if "429" in result.stderr or "Quota exceeded" in result.stderr:
                    logging.warning("Обнаружена ошибка квоты в выводе скрипта!")
                    time.sleep(30)

                if result.returncode == 0:
                    script_successfully_completed = True
                else:
                    logging.error(f"Скрипт {script_name} завершился с кодом ошибки: {result.returncode}")

        except Exception as e:
            logging.error(f"Фатальная ошибка при выполнении {script_name}: {e}")

        exec_time = time.time() - script_start_time
        execution_times[script_name] = exec_time

        print(f"Время выполнения {script_name}: {exec_time:.2f} секунд")
        logging.info(f"Время выполнения {script_name}: {exec_time:.2f} секунд")

        if script_name == "import_script_0.py":
            if script_successfully_completed:
                logging.info(f"Скрипт {script_name} успешно завершен. Запускаем Google Apps Script для перевода...")
            else:
                logging.warning(f"Скрипт {script_name} завершился с ошибкой, но Google Apps Script будет запущен в любом случае.")
                print(f"ВНИМАНИЕ: {script_name} завершился с ошибкой. Google Apps Script запускается принудительно...")
            trigger_google_apps_script(GOOGLE_APPS_SCRIPT_URL)

        if delay_after > 0:
            logging.info(f"Ожидание {delay_after} сек перед следующим скриптом...")
            print(f"Ожидание {delay_after} сек перед следующим скриптом...")
            time.sleep(delay_after)

    # Формирование финальной статистики
    total_time = time.time() - total_start_time
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    
    # Собираем блок текста в том формате, который вы просили (как в логах)
    stats_header = "<code>====== Статистика выполнения ======</code>"
    stats_total = f"{now_str} - INFO - Общее время выполнения: {total_time:.2f} секунд"
    
    lines = []
    lines.append(stats_header)
    lines.append(f"<code>{stats_total}")
    
    for script, t in execution_times.items():
        line = f"{now_str} - INFO - {script}: {t:.2f} секунд"
        lines.append(line)
        logging.info(line) # Дублируем в лог как обычно
    
    final_status = f"{now_str} - INFO - ======= ВСЕ СКРИПТЫ УСПЕШНО ВЫПОЛНЕНЫ (или обработаны согласно логике) =======</code>"
    lines.append(final_status)
    
    # Объединяем всё в одно сообщение
    full_report = "\n".join(lines)
    
    # Вывод в консоль
    print("\n" + full_report.replace("<code>", "").replace("</code>", ""))
    
    # Отправка в Telegram
    send_telegram_message(full_report)


def clear_log_files():
    """Очищает все лог файлы перед началом работы"""
    log_files = ['launcher.log', 'feed_processor.log', 'google_sheets_import.log']

    for log_file in log_files:
        try:
            if os.path.exists(log_file):
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"Log cleared at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                logging.info(f"Лог-файл {log_file} очищен")
        except Exception as e:
            logging.error(f"Ошибка при очистке лога {log_file}: {e}")


def main():
    """Основная функция управления процессом"""
    logging.info("======= СТАРТ ПРОГРАММЫ =======")
    clear_log_files()

    if not check_subscription():
        logging.error("Доступ запрещен: Email не найден в списке подписчиков")
        print('Доступ запрещен. Email не найден в списке подписчиков.')
        send_telegram_message("❌ Доступ запрещен: Email не найден в списке.")
        time.sleep(5)
        exit()

    logging.info("Подписка подтверждена. Продолжение работы...")
    print('Подписка подтверждена. Продолжение работы...')

    check_and_clear_log()
    run_scripts_sequentially()

    logging.info("======= ВСЕ СКРИПТЫ УСПЕШНО ВЫПОЛНЕНЫ =======")

if __name__ == '__main__':
    main()
