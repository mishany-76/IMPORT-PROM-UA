import gspread
from google.oauth2.service_account import Credentials
import json
import re
import logging
import time
from googleapiclient.discovery import build # Импортируем функцию build для создания сервисного клиента

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- КОНФИГУРАЦИЯ ---
SERVICE_ACCOUNT_FILE = 'key_sheet.json' # Укажите путь к вашему файлу ключа
SOURCE_SPREADSHEET_ID = '1xU-JluwmBI66mnUaQlhXy4Csz41Fezgt-Dyw_7OocTA' # Замените на ID исходной таблицы
TARGET_SPREADSHEET_ID = '1o6hic1hfDGfL6yynHjJD0cM8U_QaK_i_TERt19CQvOA' # Замените на ID целевой таблицы

# Базовые названия столбцов характеристик без суффикса, как они ДОЛЖНЫ БЫТЬ в целевой таблице,
# если в исходнике найдены характеристики. Используется для формирования структуры и маппинга данных.
BASE_TARGET_CHARACTERISTIC_COLUMNS = [
    'Назва_Характеристики',
    'Одиниця_виміру_Характеристики',
    'Значення_Характеристики',
]

# Столбцы, которые должны присутствовать в целевой таблице
# в дополнение к (столбцам из исходника с учетом правил характеристик).
# Используется только для формирования структуры целевой таблицы.
# ОБНОВЛЕНО: Добавлены новые столбцы согласно требованиям пользователя.
ADDITIONAL_TARGET_COLUMNS = [
    'Ярлик',
    'Де_знаходиться_товар',
    'Тип_товару',
]


# --- КОНФИГУРАЦИЯ КЛЮЧЕВЫХ СТОЛБЦОВ И СОРТИРОВКИ ПО ЛИСТАМ ---
# Определите ключевые столбцы (для идентификации/сравнения) и столбец для сортировки
# для каждого листа, который должен синхронизироваться по данным.
# Ключи словаря - названия листов.
# Значения - словари с ключами 'key_cols' (список имен ключевых столбцов) и 'sort_col' (имя столбца для сортировки исходника, или None).
# Если лист не указан здесь, синхронизация ДАННЫХ для него будет пропущена, только структура скопируется как есть (без правил характеристик).
SHEET_DATA_CONFIG = {
    'Export Products Sheet': {
        'key_cols': ['Код_товару', 'Ідентифікатор_товару'],
        'sort_col': 'Особисті_нотатки',
    },
    'Export Groups Sheet': {
         'key_cols': ['Номер_групи', 'Назва_групи', 'Ідентифікатор_групи', 'Номер_батьківської_групи'],
         'sort_col': None, # Например, если сортировка для групп не нужна
    },
    # Добавьте сюда другие листы, если они требуют синхронизации данных
    # Для листов не указанных здесь, будет выполнена только синхронизация структуры заголовков.
}


# --- АВТОРИЗАЦИЯ ---
def get_google_sheet_client(credentials):
    """Авторизуется в Google Sheets API и возвращает gspread Client, используя готовые учетные данные."""
    try:
        client = gspread.authorize(credentials)
        logging.info("Авторизация gspread Client успешна.")
        return client
    except Exception as e:
        logging.error(f"Ошибка при авторизации gspread Client: {e}")
        raise

def get_sheets_api_v4_service(credentials):
    """Создает и возвращает клиент Sheets API v4 (googleapiclient), используя готовые учетные данные."""
    try:
        service = build('sheets', 'v4', credentials=credentials)
        logging.info("Клиент Google Sheets API v4 (googleapiclient) успешно создан.")
        return service
    except Exception as e:
        logging.error(f"Ошибка при создании клиента Google Sheets API v4: {e}")
        raise


# --- УТИЛИТЫ ДЛЯ РАБОТЫ С ТАБЛИЦАМИ ---
def get_sheet_by_name(spreadsheet, sheet_name):
    """Получает лист по имени, создает, если не существует."""
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        logging.info(f"Лист '{sheet_name}' найден в таблице '{spreadsheet.title}'.")
        return sheet
    except gspread.WorksheetNotFound:
        logging.warning(f"Лист '{sheet_name}' не найден в таблице '{spreadsheet.title}'. Создаем новый...")
        try:
            # gspread создает лист с небольшим начальным размером, при записи он расширится
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            logging.info(f"Лист '{sheet_name}' успешно создан.")
            return sheet
        except Exception as e:
            logging.error(f"Ошибка при создании листа '{sheet_name}': {e}")
            # При ошибке создания листа - выбрасываем исключение, т.к. на этот лист нужно синхронизировать
            raise

def get_headers(worksheet):
    """Получает заголовки столбцов из первой строки листа."""
    try:
        headers = worksheet.row_values(1)
        if not headers:
            return []
        # Очистка заголовков от возможных пробелов и пустых строк в конце
        headers = [header.strip() for header in headers]
        while headers and not headers[-1]:
             headers.pop()
        return headers
    except Exception as e:
        logging.error(f"Ошибка при чтении заголовков листа '{worksheet.title}': {e}")
        raise

def find_column_index(headers, column_name):
    """Находит индекс столбца по имени (0-based). Возвращает -1, если не найден."""
    try:
        return headers.index(column_name)
    except ValueError:
        return -1

# --- Собственная функция для преобразования индекса столбца в букву ---
def get_column_letter(col_index_0based):
    """Преобразует 0-базовый индекс столбца в его буквенное представление (A, B, AA, etc.)."""
    letter = ''
    while col_index_0based >= 0:
        remainder = col_index_0based % 26
        letter = chr(ord('A') + remainder) + letter
        col_index_0based = col_index_0based // 26 - 1
    return letter


def build_target_headers_with_char_rules(source_headers, sheet_title):
    """
    Формирует список требуемых заголовков для целевой таблицы,
    применяя правила переименования характеристик и сохраняя ОРИГИНАЛЬНЫЙ ОТНОСИТЕЛЬНЫЙ порядок.
    Характеристические блоки добавляются ТОЛЬКО если характеристики найдены в исходном листе.
    """
    max_n = 0
    char_pattern = re.compile(r'^(Назва|Одиниця_виміру|Значення)_Характеристики(?:_(\d+))?$')

    # Определяем максимальное N и собираем не-характеристичные заголовки в порядке их следования
    source_has_any_characteristic_headers = False # Флаг: были ли вообще НАЙДЕНЫ характеристики в исходнике

    # Первый проход для определения max_n и наличия характеристик
    for header in source_headers:
        header_stripped = header.strip()
        if not header_stripped:
            continue
        match = char_pattern.match(header_stripped)
        if match:
            source_has_any_characteristic_headers = True # Найдена хоть одна характеристика в этом листе
            n_str = match.group(2)
            n = int(n_str) if n_str else 1 # N = 1 для заголовков без суффикса
            max_n = max(max_n, n)

    # Определяем конечное количество наборов характеристик в целевой.
    # Добавляем блоки характеристик ТОЛЬКО если они были найдены в ИСХОДНОМ листе
    # И если в конфигурации определены базовые названия.
    final_char_sets_in_target = 0
    if source_has_any_characteristic_headers and BASE_TARGET_CHARACTERISTIC_COLUMNS:
         # Если характеристики найдены, но max_n оказался 0 (например, только базовые без _N или пустые),
         # но при этом есть хотя бы одна характеристика, устанавливаем max_n в 1.
         if max_n == 0:
              final_char_sets_in_target = 1
         else:
              final_char_sets_in_target = max_n

    elif source_has_any_characteristic_headers and not BASE_TARGET_CHARACTERISTIC_COLUMNS:
         logging.warning("В исходнике найдены характеристики, но BASE_TARGET_CHARACTERISTIC_COLUMNS в конфигурации пуст. Блоки характеристик не будут добавлены в целевую структуру для этого листа.")
         # final_char_sets_in_target остается 0


    # Строим целевые заголовки, сохраняя относительный порядок из исходника
    final_target_headers = []
    added_char_block_for_set = set() # Отслеживаем, блоки каких наборов характеристик уже добавлены

    for source_index, source_header in enumerate(source_headers):
        header_stripped = source_header.strip()
        if not header_stripped:
            # Пропускаем пустые заголовки
            continue

        match = char_pattern.match(header_stripped)

        if match:
            # Это заголовок характеристики
            base_name_part = match.group(1)
            n_str = match.group(2)
            n = int(n_str) if n_str else 1 # N = 1 для заголовков без суффикса

            # Мы должны добавить ВЕСЬ блок BASE_TARGET_CHARACTERISTIC_COLUMNS для набора N
            # когда мы встречаем ЛЮБОЙ заголовок из набора N ВПЕРВЕЕ
            # И если мы вообще должны добавлять блоки характеристик (final_char_sets_in_target > 0)
            if n <= final_char_sets_in_target and n not in added_char_block_for_set:
                # Добавляем ВЕСЬ блок BASE_TARGET_CHARACTERISTIC_COLUMNS для этого набора (без _N)
                # в текущую позицию в списке целевых заголовков.
                # Проверяем, что BASE_TARGET_CHARACTERISTIC_COLUMNS не пуст перед добавлением
                if BASE_TARGET_CHARACTERISTIC_COLUMNS:
                     final_target_headers.extend(BASE_TARGET_CHARACTERISTIC_COLUMNS)
                     added_char_block_for_set.add(n)
                # Пропускаем сам исходный заголовок характеристики (т.к. он заменяется блоком без _N)

        else:
            # Это не-характеристичный заголовок. Просто добавляем его в список целевых заголовков.
            final_target_headers.append(header_stripped)


    # ИСПРАВЛЕНО: Добавляем любые ADDITIONAL_TARGET_COLUMNS, которые не уже включены,
    # только для листов, отличных от 'Export Groups Sheet'.
    if sheet_title != 'Export Groups Sheet':
        seen_final = set(final_target_headers)
        for additional_header in ADDITIONAL_TARGET_COLUMNS:
            if additional_header.strip() and additional_header.strip() not in seen_final:
                final_target_headers.append(additional_header.strip())
                seen_final.add(additional_header.strip())
    else:
        logging.info(f"Для листа '{sheet_title}' дополнительные столбцы 'Ярлик', 'Де_знаходиться_товар', 'Тип_товару' не будут добавлены.")


    return final_target_headers # Это список заголовков для целевого листа, в нужном порядке


def ensure_target_structure(worksheet, source_headers):
    """
    Проверяет и обновляет заголовки целевого листа, чтобы они соответствовали
    структуре, определенной на основе source_headers с правилами характеристик.
    Возвращает актуальный список заголовков целевой таблицы.
    """
    # ИСПРАВЛЕНО: Передаем sheet_title в build_target_headers_with_char_rules
    required_target_headers = build_target_headers_with_char_rules(source_headers, worksheet.title)

    logging.info(f"Чтение текущих заголовков целевого листа '{worksheet.title}'...")
    current_target_headers = get_headers(worksheet)

    logging.debug(f"Требуемые заголовки целевого листа: {required_target_headers}")
    logging.debug(f"Текущие заголовки целевого листа: {current_target_headers}")


    # Проверяем, нужно ли обновлять заголовки
    # Сравниваем текущие заголовки с требуемыми. Если не совпадают, обновляем.
    if current_target_headers != required_target_headers:
        logging.warning(f"Заголовки в целевом листе '{worksheet.title}' не соответствуют требуемой структуре или порядку. Обновляем заголовки...")
        try:
             num_required_cols = len(required_target_headers)
             if num_required_cols == 0:
                  # Этот случай может возникнуть, если в исходнике нет заголовков или они все были отфильтрованы,
                  # и в ADDITIONAL_TARGET_COLUMNS тоже пусто.
                  logging.warning("Результирующий список требуемых заголовков оказался пуст. Пропускаем обновление заголовков.")
                  # Очистим первую строку в целевом, если она есть
                  current_values = worksheet.get_all_values()
                  current_rows = len(current_values)
                  if current_rows > 0:
                       try:
                           worksheet.delete_rows(1, 1)
                           logging.info(f"Заголовки в листе '{worksheet.title}' успешно очищены.")
                       except Exception as e:
                            logging.error(f"Ошибка при очистке заголовков в листе '{worksheet.title}': {e}")

                  return required_target_headers # Возвращаем пустой список заголовков

             # Определяем букву последнего столбца.
             end_column_letter = get_column_letter(num_required_cols - 1)
             # Формируем динамический диапазон
             range_name = f'A1:{end_column_letter}1'

             logging.info(f"Запись заголовков в целевой лист '{worksheet.title}' в диапазон '{range_name}'...")
             worksheet.update([required_target_headers], range_name=range_name)
             logging.info(f"Заголовки в листе '{worksheet.title}' успешно обновлены.")
             return required_target_headers # Возвращаем новый актуальный список заголовков

        except Exception as e:
             logging.error(f"Ошибка при записи обновленных заголовков в лист '{worksheet.title}': {e}")
             raise # Останавливаем обработку этого листа при ошибке записи заголовков

    else:
        logging.info(f"Заголовки в целевом листе '{worksheet.title}' уже соответствуют требуемой структуре.")
        return current_target_headers # Возвращаем текущие заголовки, т.к. они верны


def build_column_mapping(source_headers, target_headers):
    """
    Создает словарь соответствия индексов столбцов: source_index -> target_index.
    Маппит исходные характеристики с _N на базовые имена в целевой.
    Маппит остальные столбцы по имени.
    Использует target_headers, сформированные с правилами характеристик.
    """
    mapping = {}
    target_header_to_index = {header: index for index, header in enumerate(target_headers)}

    char_pattern = re.compile(r'^(Назва|Одиниця_виміру|Значення)_Характеристики(?:_(\d+))?$')
    # Паттерн для поиска базовых имен характеристик
    base_char_pattern = re.compile(r'^(Назва|Одиниця_виміру|Значення)_Характеристики$')


    # Определяем, какие столбцы в target_headers являются базовыми характеристиками,
    # чтобы найти начало и структуру блоков в целевой таблице.
    target_base_char_indices = [] # Список индексов в target_headers, где находятся базовые характеристики
    for idx, header in enumerate(target_headers):
         if header in BASE_TARGET_CHARACTERISTIC_COLUMNS:
              target_base_char_indices.append(idx)

    # Проверяем, формируют ли эти индексы последовательные блоки
    is_structured_char_block = False
    first_char_target_index = -1
    num_target_char_sets = 0

    if target_base_char_indices and BASE_TARGET_CHARACTERISTIC_COLUMNS:
         base_len = len(BASE_TARGET_CHARACTERISTIC_COLUMNS)
         # Проверяем, что количество найденных базовых характеристик кратно размеру базового набора
         if base_len > 0 and len(target_base_char_indices) > 0 and len(target_base_char_indices) % base_len == 0:
              # Проверяем, что они идут последовательно, формируя блоки и соответствуют базовым именам
              all_blocks_sequential_and_match = True
              for i in range(0, len(target_base_char_indices), base_len):
                   # Начальный индекс блока в target_headers
                   current_block_start_idx_in_target_indices = i
                   start_idx_in_target_headers = target_base_char_indices[current_block_start_idx_in_target_indices]

                   # Проверяем, что следующие base_len индексов последовательны
                   expected_sequential_indices = list(range(start_idx_in_target_headers, start_idx_in_target_headers + base_len))
                   actual_indices_in_this_block = target_base_char_indices[i : i + base_len]

                   if actual_indices_in_this_block != expected_sequential_indices:
                        all_blocks_sequential_and_match = False
                        break

                   # Проверяем, что заголовки по этим индексам в target_headers соответствуют BASE_TARGET_CHARACTERISTIC_COLUMNS
                   # Убедимся, что индексы не выходят за пределы target_headers
                   headers_in_block_in_target = []
                   for idx in actual_indices_in_this_block:
                       if idx < len(target_headers):
                           headers_in_block_in_target.append(target_headers[idx])
                       else:
                           all_blocks_sequential_and_match = False # Индекс вышел за пределы
                           break # Выходим из внутреннего цикла

                   if headers_in_block_in_target != BASE_TARGET_CHARACTERISTIC_COLUMNS:
                       all_blocks_sequential_and_match = False
                       break


              if all_blocks_sequential_and_match:
                   is_structured_char_block = True
                   first_char_target_index = target_base_char_indices[0]
                   num_target_char_sets = len(target_base_char_indices) // base_len
                   logging.debug(f"Обнаружена структурированная область характеристик в целевых заголовках. Начало: {first_char_target_index}, Наборов: {num_target_char_sets}")
              else:
                   logging.warning("Базовые названия характеристик найдены в целевых заголовках, но структура блоков нарушена. Маппинг характеристик может быть некорректен.")


         else:
              logging.warning("Количество базовых названий характеристик в целевых заголовках не кратно размеру базового набора, или размер базового набора 0. Маппинг характеристик может быть некорректен.")

    else:
         # Базовые названия характеристик отсутствуют в целевых заголовках или в конфигурации.
         pass # first_char_target_index и num_target_char_sets остаются в начальном значении 0/-1


    for source_index, source_header in enumerate(source_headers):
        source_header_stripped = source_header.strip()
        if not source_header_stripped:
             continue # Пропускаем пустые заголовки в исходнике

        # Проверяем, является ли заголовок характеристикой (с суффиксом или без)
        match = char_pattern.match(source_header_stripped)
        if match:
            base_name = match.group(1) + '_Характеристики'
            n_str = match.group(2)
            n = int(n_str) if n_str else 1 # N = 1 для заголовков без суффикса

            # Маппируем характеристику только если обнаружена структурированная область в целевой
            # и номер набора (n) не превышает количество наборов в целевой.
            if is_structured_char_block and base_name in BASE_TARGET_CHARACTERISTIC_COLUMNS and n <= num_target_char_sets:
                try:
                    # Ищем базовое имя в списке базовых названий характеристик, чтобы определить смещение внутри набора
                    base_index_in_set = BASE_TARGET_CHARACTERISTIC_COLUMNS.index(base_name)
                    # Индекс в целевой таблице = Начало блока характеристик + (номер набора - 1) * размер набора + смещение внутри набора
                    target_index = first_char_target_index + (n - 1) * len(BASE_TARGET_CHARACTERISTIC_COLUMNS) + base_index_in_set

                    # Убедимся, что рассчитанный target_index действительно соответствует базовому имени характеристики в target_headers
                    if target_index < len(target_headers) and target_headers[target_index] == base_name:
                         mapping[source_index] = target_index
                    else:
                          # Это может произойти, если структура в target_headers не идеальна, но is_structured_char_block = True
                          logging.warning(f"Рассчитанный целевой индекс {target_index} для '{source_header_stripped}' не соответствует базовому имени в целевых заголовках или выходит за пределы. Пропущено маппинг.")


                except ValueError:
                     # Этого не должно происходить, если BASE_TARGET_CHARACTERISTIC_COLUMNS корректны
                     logging.error(f"Критическая ошибка маппинга: Базовое имя характеристики '{base_name}' не найдено в BASE_TARGET_CHARACTERISTIC_COLUMNS при маппинге исходного '{source_header_stripped}'.")
            elif source_header_stripped in target_header_to_index:
                 # Если это базовая характеристика без _N (n=1), и она присутствует в целевых заголовках (но не в блоке),
                 # маппим ее напрямую. Этот случай менее вероятен при правильной структуре, но для надежности.
                 if base_char_pattern.match(source_header_stripped) and n == 1:
                      mapping[source_index] = target_header_to_index[source_header_stripped]
                 # Иначе (характеристика с _N, но нет блока в целевой, или номер набора > num_target_char_sets), пропускаем.
                 # logging.debug(f"Пропущено маппинг характеристики '{source_header_stripped}'. Нет соответствующего блока в целевой.")

        else:
            # Если не характеристика, ищем прямое совпадение в целевых заголовках
            if source_header_stripped in target_header_to_index:
                mapping[source_index] = target_header_to_index[source_header_stripped]
            # else: столбец исходника не маппится на целевой, пропускаем его. Это нормально.

    return mapping


# Передаем оба объекта client (gspread и googleapiclient) в функцию синхронизации одного листа
def synchronize_single_sheet_with_data(gspread_client, sheets_service, source_sheet, target_spreadsheet):
    """Синхронизирует данные и структуру с одного листа исходной таблицы (на месте)."""
    source_sheet_title = source_sheet.title
    logging.info(f"--- Начинаем синхронизацию для листа: '{source_sheet_title}' ---")

    try:
        # Получаем или создаем целевой лист с тем же именем через gspread_client
        target_sheet = get_sheet_by_name(target_spreadsheet, source_sheet_title)

        # --- Чтение и анализ заголовков ---
        logging.info(f"Чтение заголовков исходного листа '{source_sheet_title}'...")
        source_headers = get_headers(source_sheet)
        if not source_headers:
            logging.warning(f"Исходный лист '{source_sheet_title}' пуст или не содержит заголовков. Пропускаем синхронизацию для этого листа.")
            # Если исходный лист пуст или без заголовков, мы все равно создадим целевой лист, но оставим его без заголовков и без данных.
            logging.info(f"--- Синхронизация для листа '{source_sheet_title}' завершена (исходник без заголовков) ---")
            return

        # --- Проверка и обновление структуры целевой таблицы (заголовков) ---
        # Обновляем структуру независимо от наличия конфигурации данных
        updated_target_headers = ensure_target_structure(target_sheet, source_headers)

        # --- Определение ключевых столбцов и столбца сортировки для этого листа ---
        sheet_config = SHEET_DATA_CONFIG.get(source_sheet_title)

        if not sheet_config:
             logging.info(f"Для листа '{source_sheet_title}' нет конфигурации данных в SHEET_DATA_CONFIG. Синхронизация данных пропущена.")
             logging.info(f"--- Синхронизация для листа '{source_sheet_title}' завершена (нет конфига данных) ---")
             return

        key_cols_names = sheet_config.get('key_cols', [])
        sort_col_name = sheet_config.get('sort_col')

        if not key_cols_names:
             logging.warning(f"В конфигурации для листа '{source_sheet_title}' не указаны ключевые столбцы ('key_cols'). Синхронизация данных пропущена.")
             logging.info(f"--- Синхронизация для листа '{source_sheet_title}' завершена (нет key_cols в конфиге) ---")
             return

        # Получаем индексы ключевых столбцов в исходнике и целевой
        source_key_indices = []
        missing_source_key_cols = []
        for col_name in key_cols_names:
             idx = find_column_index(source_headers, col_name)
             if idx == -1:
                  missing_source_key_cols.append(col_name)
             source_key_indices.append(idx) # Индекс может быть -1

        target_key_indices = []
        missing_target_key_cols = []
        for col_name in key_cols_names:
             # Ищем индекс в обновленных заголовках целевой таблицы
             idx = find_column_index(updated_target_headers, col_name)
             if idx == -1:
                  missing_target_key_cols.append(col_name)
             target_key_indices.append(idx) # Индекс может быть -1


        if missing_source_key_cols:
             logging.error(f"Ключевые столбцы для обработки данных отсутствуют в исходном листе '{source_sheet_title}': {missing_source_key_cols}. Синхронизация данных пропущена.")
             logging.info(f"--- Синхронизация для листа '{source_sheet_title}' завершена (нет key_cols в исходнике) ---")
             return # Нет ключевых колонок в исходнике, не можем синхронизировать данные

        if missing_target_key_cols:
            # Этого не должно произойти, если ensure_target_structure отработал корректно
            # и build_target_headers_with_char_rules включил эти колонки, если они были в исходнике.
            logging.error(f"Ключевые столбцы для обработки данных отсутствуют в целевом листе '{source_sheet_title}' после обновления структуры: {missing_target_key_cols}. Критическая ошибка. Синхронизация данных пропущена.")
            return # Нет ключевых колонок в целевой, не можем синхронизировать данные

        # Получаем индекс столбца сортировки в исходнике
        source_sort_col_index = find_column_index(source_headers, sort_col_name) if sort_col_name else -1


        # Создаем маппинг столбцов из исходной в целевую
        logging.info("Построение маппинга столбцов между исходным и целевым листами...")
        col_mapping = build_column_mapping(source_headers, updated_target_headers)
        #logging.debug(f"Маппинг столбцов (source_index -> target_index): {col_mapping}") # Для отладки


        # --- Чтение данных ---
        logging.info(f"Чтение данных из исходного листа '{source_sheet_title}'...")
        source_data = source_sheet.get_all_values()[1:] # Пропускаем заголовки
        logging.info(f"Прочитано {len(source_data)} строк из исходного листа.")

        # Если нет данных в исходнике (кроме заголовков), синхронизация данных не нужна,
        # но если в целевой есть данные, которые нужно удалить (старые, без ключей, дубликаты),
        # то удаление нужно выполнить.
        if not source_data:
             logging.warning(f"В исходном листе '{source_sheet_title}' нет данных. Будет выполнено только удаление устаревших/дублирующихся строк из целевого листа.")
             # Продолжаем выполнение, чтобы прочитать целевую и удалить лишнее


        logging.info(f"Чтение данных из целевого листа '{source_sheet_title}'...")
        target_data = target_sheet.get_all_values()[1:] # Пропускаем заголовки
        logging.info(f"Прочитано {len(target_data)} строк из целевого листа.")

        # Если в исходнике нет данных И в целевой нет данных (кроме заголовков), то ничего не делаем
        if not source_data and not target_data:
             logging.info(f"Нет данных ни в исходном, ни в целевом листе '{source_sheet_title}'. Синхронизация данных не требуется.")
             logging.info(f"--- Синхронизация для листа '{source_sheet_title}' завершена (нет данных) ---")
             return


        # --- Сортировка исходных данных ---
        if sort_col_name and source_sort_col_index != -1:
            # Делаем сортировку безопасной для строк разной длины
            source_data.sort(key=lambda row: row[source_sort_col_index].strip() if len(row) > source_sort_col_index else '') # Сортируем по значению, удаляя пробелы
            logging.info(f"Исходные данные отсортированы по '{sort_col_name}'.")

        elif sort_col_name and source_sort_col_index == -1:
             logging.warning(f"Столбец сортировки '{sort_col_name}' указан в конфиге, но отсутствует в исходнике. Сортировка исходных данных пропущена.")


        # --- Подготовка данных для синхронизации на месте и идентификация дубликатов в целевой ---
        # Создаем маппинг УНИКАЛЬНЫХ товаров в целевой таблице для быстрого поиска
        target_product_map = {} # Key: product_key tuple, Value: target_row_index (0-based) первой встречи
        target_row_data_by_index = {} # Key: target_row_index (0-based), Value: target_row_data list (оригинал из чтения, дополненный до нужной длины)

        # Инициализируем список строк для удаления. Сюда попадут:
        # 1. Строки в целевой без валидного ключа.
        # 2. Дубликаты в целевой (оставляем первое вхождение).
        # 3. Строки в целевой, отсутствующие в исходнике (будут добавлены позже).
        rows_to_delete_indices_1based = []

        max_key_idx_target = max(target_key_indices) if target_key_indices else -1
        required_cols_len_target = len(updated_target_headers) # Используем updated_target_headers после ensure_target_structure

        seen_target_keys = set() # Для отслеживания уникальных ключей в целевой таблице

        for row_index_0based, row in enumerate(target_data):
            target_row_index_1based = row_index_0based + 2 # +1 for 0-based to 1-based, +1 for header row

            # Ensure row has enough columns for key indices before processing
            # Строки, слишком короткие для ключевых столбцов, не могут быть идентифицированы.
            # Их ключи не будут добавлены в target_product_map.

            # Extract key values from target row
            target_key_values = []
            has_any_key_value = False
            # Проверяем, что строка достаточно длинная для всех ключевых столбцов перед доступом
            if len(row) > max_key_idx_target:
                 for key_idx in target_key_indices:
                      # Безопасный доступ к значению, учитывая возможно короткие строки
                      key_value = row[key_idx].strip() if key_idx != -1 and key_idx < len(row) else ''
                      target_key_values.append(key_value)
                      if key_value:
                           has_any_key_value = True
             # else: строка слишком короткая для ключевых столбцов, has_any_key_value = False

            product_key = tuple(target_key_values)

            # Идентификация дубликатов в целевой и строк без ключа
            if not has_any_key_value:
                 # Строка без валидного ключа - помечаем на удаление
                 logging.debug(f"Строка {target_row_index_1based} в целевой таблице не имеет валидного ключа. Помечена на удаление.")
                 rows_to_delete_indices_1based.append(target_row_index_1based)
                 # Не добавляем такие строки в target_product_map или target_row_data_by_index
            elif product_key in seen_target_keys:
                 # Этот ключ уже встречался - это дубликат, помечаем на удаление
                 logging.debug(f"Строка {target_row_index_1based} в целевой таблице является дубликатом ключа {product_key}. Помечена на удаление.")
                 rows_to_delete_indices_1based.append(target_row_index_1based)
                 # Не добавляем этот дубликат в target_product_map или target_row_data_by_index
            else:
                 # Это первое вхождение уникального ключа в целевой
                 seen_target_keys.add(product_key)
                 target_product_map[product_key] = row_index_0based
                 # Создаем копию строки, дополненную до нужной длины заголовков
                 processed_row_data = list(row)
                 if len(processed_row_data) < required_cols_len_target:
                      processed_row_data.extend([''] * (required_cols_len_target - len(processed_row_data)))
                 elif len(processed_row_data) > required_cols_len_target:
                      processed_row_data = processed_row_data[:required_cols_len_target]
                 target_row_data_by_index[row_index_0based] = processed_row_data # Store a copy


        logging.info(f"Идентификация дубликатов и строк без ключа в целевой таблице завершена. Найдено {len([idx for idx in rows_to_delete_indices_1based if idx <= len(target_data) + 1])} строк для удаления на этом этапе.")


        # --- Идентификация операций (Обновления, Добавления) ---
        # Используем теперь target_product_map, который содержит только уникальные ключи из целевой

        # update_operations: List of dictionaries, each with 'range' (A1 notation) and 'values' (list of lists)
        update_operations_batch_format = []
        source_product_keys_seen = set() # Этот набор будет использоваться для идентификации строк в целевой, отсутствующих в исходнике
        new_rows_to_add_data = [] # List of new rows (list of values)

        max_key_idx_source = max(source_key_indices) if source_key_indices else -1
        source_num_cols = len(source_headers)

        # Получаем индексы новых/изменяемых столбцов в исходнике и целевой
        availability_col_source_idx = find_column_index(source_headers, 'Наявність')
        # ИСПРАВЛЕНО: Используем 'Особисті_нотатки' для определения поставщика
        supplier_col_source_idx = find_column_index(source_headers, 'Особисті_нотатки')
        quantity_col_source_idx = find_column_index(source_headers, 'Кількість')

        label_col_target_idx = find_column_index(updated_target_headers, 'Ярлик')
        location_col_target_idx = find_column_index(updated_target_headers, 'Де_знаходиться_товар')
        type_col_target_idx = find_column_index(updated_target_headers, 'Тип_товару')

        if supplier_col_source_idx == -1:
            logging.warning("Столбец 'Особисті_нотатки' не найден в исходной таблице. Функции 'Ярлик', 'Де_знаходиться_товар' и 'Тип_товару' не будут работать корректно.")


        for source_row_index, source_row in enumerate(source_data):
             source_row_1based = source_row_index + 2 # +1 for 0-based to 1-based, +1 for header row

             # Ensure row has enough columns for key indices before processing
             # Строки в исходнике слишком короткие для ключей игнорируем
             if len(source_row) <= max_key_idx_source:
                  # logging.warning(f"Строка {source_row_1based} в исходной таблице слишком короткая для ключевых столбцов. Пропущена.")
                  continue # Skip processing this row

             # Extract key values from source row
             source_key_values = []
             has_any_key_value_source = False # Переименовал, чтобы не путать с target
             for key_idx in source_key_indices:
                  # Убедимся, что индекс не выходит за пределы строки исходника перед доступом
                  key_value = source_row[key_idx].strip() if key_idx != -1 and key_idx < len(source_row) else ''
                  source_key_values.append(key_value)
                  if key_value:
                       has_any_key_value_source = True

             # Пропускаем строки из исходника с пустыми ключевыми столбцами (не можем их сопоставить)
             if not has_any_key_value_source:
                  # logging.warning(f"Строка {source_row_1based} в исходной таблице имеет пустые ключевые столбцы {key_cols_names}. Пропущена.")
                  continue

             product_key = tuple(source_key_values)
             source_product_keys_seen.add(product_key) # Добавляем ключ в набор "видимых" в исходнике

             # Определяем поставщика для текущей строки
             current_supplier = ''
             if supplier_col_source_idx != -1 and supplier_col_source_idx < len(source_row):
                 current_supplier = source_row[supplier_col_source_idx].strip().upper()


             if product_key in target_product_map:
                 # Этот продукт существует как в исходнике, так и в целевой -> потенциальное ОБНОВЛЕНИЕ
                 target_row_index_0based = target_product_map[product_key]
                 # Получаем ДАННЫЕ целевой строки (скопированные ранее и дополненные)
                 target_row_data = target_row_data_by_index[target_row_index_0based]
                 target_row_number_1based = target_row_index_0based + 2 # 1-базовый номер строки для A1 диапазона

                 # Compare source and target cells based on mapping and non-empty source values
                 for source_col_index in range(source_num_cols):
                     if source_col_index in col_mapping:
                          target_col_index_0based = col_mapping[source_col_index]

                          # Ensure source row has enough columns for the current index
                          # Ensure target_row_data also has enough columns (должно быть так после дополнения)
                          if source_col_index < len(source_row) and target_col_index_0based < len(target_row_data):
                              source_value = source_row[source_col_index] # Get value (keep original formatting for now)

                              # 1. Изменение столбца "Наявність"
                              if source_col_index == availability_col_source_idx:
                                  if source_value.strip().upper() == 'TRUE' or source_value.strip() == '+':
                                      source_value = '!'
                                  # else: 'FALSE' или '-' остаются как есть

                              # ИСПРАВЛЕНО: Обновляем ячейку только если исходное значение не пустое И отличается от целевого
                              if source_col_index == quantity_col_source_idx:
                                  # Для столбца 'Кількість', обновляем всегда, если значения отличаются, даже если новое значение пустое
                                  if source_value != target_row_data[target_col_index_0based]:
                                      target_col_letter = get_column_letter(target_col_index_0based)
                                      cell_range_a1 = f"{source_sheet_title}!{target_col_letter}{target_row_number_1based}"
                                      update_operations_batch_format.append({
                                          'range': cell_range_a1,
                                          'values': [[source_value]]
                                      })
                              else:
                                  # Для всех остальных столбцов сохраняем старую логику:
                                  # обновляем только если исходное значение не пустое И отличается от целевого
                                  if source_value.strip() and source_value != target_row_data[target_col_index_0based]:
                                      target_col_letter = get_column_letter(target_col_index_0based)
                                      cell_range_a1 = f"{source_sheet_title}!{target_col_letter}{target_row_number_1based}"
                                      update_operations_batch_format.append({
                                          'range': cell_range_a1,
                                          'values': [[source_value]]
                                      })
                          # else: source row too short or no mapping or target_row_data too short (unlikely)

                 # Применяем изменения к новым столбцам для существующих строк
                 # Эти столбцы должны обновляться ТОЛЬКО для листа 'Export Products Sheet'
                 if source_sheet_title != 'Export Groups Sheet':
                     # 2. Вставка "Топ продаж" в "Ярлик"
                     if label_col_target_idx != -1 and current_supplier != 'FOOTBALLERS':
                         new_label_value = 'Топ продаж'
                         if target_row_data[label_col_target_idx] != new_label_value:
                             target_col_letter = get_column_letter(label_col_target_idx)
                             cell_range_a1 = f"{source_sheet_title}!{target_col_letter}{target_row_number_1based}"
                             update_operations_batch_format.append({
                                 'range': cell_range_a1,
                                 'values': [[new_label_value]]
                             })

                     # 3. Вставка "Де_знаходиться_товар"
                     if location_col_target_idx != -1 and current_supplier != 'FOOTBALLERS':
                         location_map = {
                             'AGER': 'Одеса',
                             'IZIDROP': 'Одеса',
                             'MOYDROP': 'Одеса',
                             'SPECULANT': 'Полтава',
                             'KIRS': 'Дніпро',
                         }
                         new_location_value = location_map.get(current_supplier, '')
                         if target_row_data[location_col_target_idx] != new_location_value:
                             target_col_letter = get_column_letter(location_col_target_idx)
                             cell_range_a1 = f"{source_sheet_title}!{target_col_letter}{target_row_number_1based}"
                             update_operations_batch_format.append({
                                 'range': cell_range_a1,
                                 'values': [[new_location_value]]
                             })

                     # 4. Вставка "r" в "Тип_товару"
                     if type_col_target_idx != -1 and current_supplier != 'FOOTBALLERS':
                         new_type_value = 'r'
                         if target_row_data[type_col_target_idx] != new_type_value:
                             target_col_letter = get_column_letter(type_col_target_idx)
                             cell_range_a1 = f"{source_sheet_title}!{target_col_letter}{target_row_number_1based}"
                             update_operations_batch_format.append({
                                 'range': cell_range_a1,
                                 'values': [[new_type_value]]
                             })


             else:
                 # Этот продукт есть в исходнике, но нет в целевой -> ДОБАВИТЬ
                 # Создаем новую строку на основе данных исходника по маппингу
                 new_row = [''] * required_cols_len_target # Создаем новую пустую строку нужной длины
                 # Populate the new row based on mapping and non-empty source values
                 for source_col_index in range(source_num_cols):
                     if source_col_index in col_mapping:
                          target_col_index_0based = col_mapping[source_col_index]
                          # Ensure source row has enough columns for the current index
                          # Ensure new_row also has enough columns (it should after initialization)
                          if source_col_index < len(source_row) and target_col_index_0based < len(new_row):
                               source_value = source_row[source_col_index]

                               # 1. Изменение столбца "Наявність" для новых строк
                               if source_col_index == availability_col_source_idx:
                                   if source_value.strip().upper() == 'TRUE' or source_value.strip() == '+':
                                       source_value = '!'
                                   # else: 'FALSE' или '-' остаются как есть

                               if source_value.strip(): # Добавляем только непустые значения из исходника
                                   new_row[target_col_index_0based] = source_value
                          # else: source row too short or source value is empty

                 # Применяем изменения к новым столбцам для новых строк
                 # Эти столбцы должны добавляться ТОЛЬКО для листа 'Export Products Sheet'
                 if source_sheet_title != 'Export Groups Sheet':
                     # 2. Вставка "Топ продаж" в "Ярлик"
                     if label_col_target_idx != -1 and current_supplier != 'FOOTBALLERS':
                         new_row[label_col_target_idx] = 'Топ продаж'

                     # 3. Вставка "Де_знаходиться_товар"
                     if location_col_target_idx != -1 and current_supplier != 'FOOTBALLERS':
                         location_map = {
                             'AGER': 'Одеса',
                             'IZIDROP': 'Одеса',
                             'MOYDROP': 'Одеса',
                             'SPECULANT': 'Полтава',
                             'KIRS': 'Дніпро',
                         }
                         new_row[location_col_target_idx] = location_map.get(current_supplier, '')

                     # 4. Вставка "r" в "Тип_товару"
                     if type_col_target_idx != -1 and current_supplier != 'FOOTBALLERS':
                         new_row[type_col_target_idx] = 'r'

                 new_rows_to_add_data.append(new_row)


        logging.info(f"Идентификация операций завершена. Найдено {len(update_operations_batch_format)} обновлений ячеек, {len(new_rows_to_add_data)} добавлений.")

        # --- Идентификация строк в целевой, отсутствующих в исходнике (для удаления) ---
        # Строки в целевой, которые НЕ БЫЛИ найдены в исходнике (среди source_product_keys_seen).
        # Строки без ключа и дубликаты в целевой уже были добавлены в rows_to_delete_indices_1based ранее.

        # Итерируем по уникальным ключам, которые были найдены в целевой таблице.
        for product_key, target_row_index_0based in target_product_map.items():
            # Если уникальный ключ из целевой НЕ найден в наборе ключей из исходника
            if product_key not in source_product_keys_seen:
                 # Помечаем на удаление строку в целевой, соответствующую этому ключу (первое вхождение)
                 target_row_index_1based = target_row_index_0based + 2
                 logging.debug(f"Уникальный ключ из целевой {product_key} не найден в исходнике. Строка {target_row_index_1based} помечена на удаление.")
                 rows_to_delete_indices_1based.append(target_row_index_1based)


        logging.info(f"Найдено всего {len(rows_to_delete_indices_1based)} строк для удаления (включая дубликаты в целевой, строки без ключа и отсутствующие в исходнике).")

        # --- Выполнение операций (Обновления, Удаления, Добавления) ---
        # Важно: удаляем строки СНИЗУ ВВЕРХ, чтобы индексы не сбивались.


        # 1. Выполнение обновлений ячеек (ПАКЕТНО)
        if update_operations_batch_format:
            logging.info(f"Отправка пакетного запроса на обновление {len(update_operations_batch_format)} ячеек...")

            # Perform the batch update with retry logic
            batch_max_retries = 5
            batch_retry_delay = 10 # Увеличена задержка для пакетных операций

            for attempt in range(batch_max_retries):
                try:
                    # Подготавливаем тело запроса для values_batch_update
                    body = {
                        'value_input_option': 'USER_ENTERED', # Как интерпретировать введенные значения
                        'data': update_operations_batch_format # Список диапазонов и значений
                    }

                    # --- Исправленный вызов API для пакетного обновления ---
                    # Используем sheets_service (googleapiclient), который был создан в main
                    sheets_service.spreadsheets().values().batchUpdate(
                        spreadsheetId=TARGET_SPREADSHEET_ID,
                        body=body
                    ).execute() # <-- Обязательно вызываем execute()

                    logging.info(f"Пакетное обновление {len(update_operations_batch_format)} ячеек завершено успешно.")
                    break # Успех, выходим из цикла повторных попыток

                except Exception as e: # Ловим любые ошибки при вызове API, включая APIError
                     # Проверка на временные ошибки по статусу или тексту ошибки
                     if attempt < batch_max_retries - 1 and (
                         (hasattr(e, 'resp') and e.resp.status in [429, 500, 502, 503, 504]) or
                         ("temporarily unavailable" in str(e).lower()) or
                         ("bad gateway" in str(e).lower()) or
                         ("backend error" in str(e).lower()) or
                         ("-1" in str(e)) # gspread might wrap some errors as -1
                     ):
                          logging.warning(f"API Error (вероятно временная) при пакетном обновлении: {e}. Повторная попытка через {batch_retry_delay} сек...")
                          time.sleep(batch_retry_delay)
                     else:
                          logging.error(f"Критическая ошибка при пакетном обновлении после {attempt + 1} попыток: {e}")
                          raise # При критической ошибке при обновлении останавливаем обработку листа

            # else: блок for, если не было break (все попытки исчерпаны и не было критической ошибки)
            else:
                 logging.error(f"Не удалось выполнить пакетное обновление после {batch_max_retries} попыток для листа '{source_sheet_title}'.")
                 # При неудаче после всех попыток логируем и продолжаем к следующему шагу (удаление/добавление).


        else:
            logging.info("Нет операций обновления ячеек для выполнения.")


        # 2. Выполнение удалений (с конца)
        if rows_to_delete_indices_1based:
            logging.info(f"Выполнение {len(rows_to_delete_indices_1based)} удалений строк...")
            # Сортируем индексы по убыванию
            rows_to_delete_indices_1based.sort(reverse=True)

            delete_max_retries = 5
            delete_retry_delay = 5

            for row_index_1based in rows_to_delete_indices_1based:
                for delete_attempt in range(delete_max_retries):
                    try:
                        logging.debug(f"Попытка удаления строки {row_index_1based}...")
                        target_sheet.delete_rows(row_index_1based) # Используем gspread_client метод
                        logging.debug(f"Удалена строка {row_index_1based}.")
                        # --- Добавляем задержку 1.1 секунды после успешного удаления ---
                        time.sleep(1.1)
                        break # Успех, переходим к следующему удалению
                    except gspread.exceptions.APIError as e:
                         # Добавил проверку на код 429 (Too Many Requests)
                         if delete_attempt < delete_max_retries - 1 and ('429' in str(e) or '500' in str(e) or '502' in str(e) or '503' in str(e) or '-1' in str(e) or 'temporarily unavailable' in str(e).lower() or 'bad gateway' in str(e).lower() or 'backend error' in str(e).lower()):
                              logging.warning(f"API Error (вероятно временная) при удалении строки {row_index_1based}: {e}. Повторная попытка через {delete_retry_delay} сек...")
                              time.sleep(delete_retry_delay)
                         else:
                              logging.error(f"Критическая ошибка API при удалении строки {row_index_1based} после {delete_attempt + 1} попыток: {e}")
                              # Логируем ошибку, но продолжаем попытки удаления других строк.
                              break # Останавливаем повторные попытки для этой строки, переходим к следующей

                    except Exception as e:
                         logging.error(f"Неожиданная ошибка при удалении строки {row_index_1based}: {e}")
                         # Логируем и продолжаем.
                         break # Останавливаем повторные попытки для этой строки, переходим к следующей

            # else: блок for, если не было break (все попытки для строки исчерпаны)
            else:
                 logging.error(f"Не удалось удалить строку после {delete_max_retries} попыток для листа '{source_sheet_title}'.")


        logging.info("Выполнение удалений завершено.")


        # 3. Выполнение добавлений новых строк
        if new_rows_to_add_data:
            logging.info(f"Выполнение {len(new_rows_to_add_data)} добавлений строк...")

            # retry логика для append_rows
            max_retries_append = 5
            retry_delay_seconds_append = 5

            for attempt in range(max_retries_append):
                try:
                    logging.info(f"Попытка записи новых данных {attempt + 1}/{max_retries_append}...")
                    # Убедимся, что все строки имеют нужную длину перед записью
                    required_cols_len = len(updated_target_headers)
                    data_to_write = [] # Данные для текущей попытки append_rows
                    for row in new_rows_to_add_data:
                         processed_row = list(row)
                         if len(processed_row) < required_cols_len:
                              processed_row.extend([''] * (required_cols_len - len(processed_row)))
                         elif len(processed_row) > required_cols_len:
                              processed_row = processed_row[:required_cols_len]
                         data_to_write.append(processed_row)

                    if data_to_write: # Вызываем append_rows только если есть данные
                         target_sheet.append_rows(data_to_write, value_input_option='USER_ENTERED') # Используем gspread_client метод
                         logging.info(f"Успешно записано {len(data_to_write)} новых строк в целевой лист '{source_sheet_title}'.")
                    else:
                         logging.info(f"Нет данных для добавления в целевой лист '{source_sheet_title}'.")

                    break # Успех, выходим из цикла повторных попыток

                except gspread.exceptions.APIError as e:
                    # Добавил проверку на код 429 (Too Many Requests)
                    if attempt < max_retries_append - 1 and ('429' in str(e) or '500' in str(e) or '502' in str(e) or '503' in str(e) or '-1' in str(e) or 'temporarily unavailable' in str(e).lower() or 'bad gateway' in str(e).lower() or 'backend error' in str(e).lower()):
                         logging.warning(f"API Error (вероятно временная) при добавлении строк: {e}. Повторная попытка через {retry_delay_seconds_append} сек...")
                         time.sleep(retry_delay_seconds_append)
                    else:
                         logging.error(f"Критическая ошибка API при добавлении строк после {attempt + 1} попыток: {e}")
                         raise # При критической ошибке при добавлении строк останавливаем обработку листа

                except Exception as e:
                    logging.error(f"Неожиданная ошибка при добавлении строк: {e}")
                    raise # При неожиданной ошибке останавливаем обработку листа

            # else: блок for, если не было break (все попытки исчерпаны и не было критической ошибки)
            else:
                 logging.error(f"Не удалось добавить строки после {max_retries_append} попыток для листа '{source_sheet_title}'.")
                 # При неудаче после всех попыток логируем и продолжаем к следующему листу.


        logging.info(f"--- Синхронизация для листа '{source_sheet_title}' завершена ---")

    except Exception as e:
        logging.error(f"Произошла ошибка при синхронизации листа '{source_sheet_title}'. Error: {e}")
        import traceback
        logging.error(traceback.format_exc())
        # Продолжаем к следующему листу после логирования ошибки


def synchronize_all_sheets():
    """Основная функция для синхронизации всех листов (структуры и данных) из исходной таблицы в целевую."""
    logging.info("--- Запуск скрипта синхронизации Google Таблиц (структура и данные на месте) ---")
    try:
        # --- 1. Загрузка учетных данных сервисного аккаунта ---
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=scopes)
            logging.info("Учетные данные сервисного аккаунта успешно загружены.")
        except FileNotFoundError:
            logging.error(f"Файл ключа сервисного аккаунта не найден: {SERVICE_ACCOUNT_FILE}")
            return # Останавливаем выполнение, если файл не найден
        except Exception as e:
            logging.error(f"Ошибка при загрузке учетных данных сервисного аккаунта: {e}")
            return # Останавливаем выполнение при других ошибках загрузки


        # --- 2. Авторизация gspread Client с использованием учетных данных ---
        try:
            gspread_client = get_google_sheet_client(credentials)
        except Exception as e:
             # Ошибка уже логируется внутри get_google_sheet_client
             return # Останавливаем выполнение, если gspread авторизация не удалась


        # --- 3. Создание клиента Sheets API v4 (googleapiclient) с использованием тех же учетных данных ---
        try:
            sheets_service = get_sheets_api_v4_service(credentials)
        except Exception as e:
            # Ошибка уже логируется внутри get_sheets_api_v4_service
            import traceback
            logging.error(traceback.format_exc()) # Дополнительно логируем трейсбек
            return # Останавливаем выполнение, если не удалось создать googleapiclient сервис


        # Открываем таблицы (используем gspread_client)
        logging.info(f"Открытие исходной таблицы (ID: {SOURCE_SPREADSHEET_ID})...")
        source_spreadsheet = gspread_client.open_by_key(SOURCE_SPREADSHEET_ID)
        logging.info(f"Открытие целевой таблицы (ID: {TARGET_SPREADSHEET_ID})...")
        target_spreadsheet = gspread_client.open_by_key(TARGET_SPREADSHEET_ID)

        # Получаем список листов в исходной таблице (используем source_spreadsheet, полученный через gspread_client)
        logging.info("Получение списка листов из исходной таблицы...")
        source_worksheets = source_spreadsheet.worksheets()
        logging.info(f"Найдено {len(source_worksheets)} листов в исходной таблице.")

        if not source_worksheets:
            logging.warning("Исходная таблица не содержит листов. Нечего синхронизировать.")
            return

        # Итерируемся по каждому листу и синхронизируем его
        for i, source_sheet in enumerate(source_worksheets):
            # Передаем оба объекта client (gspread_client и sheets_service) в функцию синхронизации одного листа
            try:
                 synchronize_single_sheet_with_data(gspread_client, sheets_service, source_sheet, target_spreadsheet)
            except Exception as e:
                 logging.error(f"Произошла НЕОБРАБОТАННАЯ ошибка при синхронизации листа '{source_sheet.title}'. Продолжаем с следующим листом (если есть). Error: {e}")
                 import traceback
                 logging.error(traceback.format_exc())


            # Добавляем небольшую паузу между обработкой листов для снижения нагрузки на API
            if i < len(source_worksheets) - 1:
                 time.sleep(2) # Пауза 2 секунды между листами

    except gspread.exceptions.APIError as e:
        logging.error(f"Глобальная ошибка Google Sheets API (gspread): {e.response.text}")
        logging.error("Проверьте права доступа сервисного аккаунта к таблицам.")
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка в основном процессе: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
         logging.info("--- Скрипт синхронизации Google Таблиц (структура и данные на месте) завершен ---")

# --- ЗАПУСК СКРИПТА ---
if __name__ == '__main__':
    synchronize_all_sheets()