import pandas as pd
import json
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='google_sheets_import.log',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# Конфигурация
CONFIG = {
    "SERVICE_ACCOUNT_FILE": "key_sheet.json",
    "MAPPING_FILE": "column_mapping.json",
    "SUPPLIERS": {
        "AGER": "1ls3HlH3cs3f-7GwBbnp7I7tYLxt9p-F80MmrUjsyk6M",
        "FOOTBALLERS": "1F1VtMQHMMd_uON81exgVmpOz2xVZNm9aHSVucVBZZW0",
        "IZIDROP": "101xN35FXrwYYb74NnguQlJ0csYv_L9K4uRzlXo2hBVY",
        "MOYDROP": "10PRDnJY5MUCpJEZWRmwTHtyMBp_9ltnbFaqR8UYk3Vs",
        #"SPECULANT": "10GesfoS_QWL_oFFlk-W9HBuOzHIuKd7ylF9h2v-xfMs",
        "KIRS": "1oMAMDBpr6HXHbvOicAupWTl5c36AZXPNj1-mA_tatzg"
    },
    "OUTPUT_SPREADSHEET_ID": "1xU-JluwmBI66mnUaQlhXy4Csz41Fezgt-Dyw_7OocTA",
    "DELAY_BETWEEN_REQUESTS": 1.1,
    "MAX_RETRIES": 3,
    "BATCH_SIZE": 200
}


class GoogleSheetsManager:
    def __init__(self):
        self.service = self._authenticate()
        self.last_request_time = datetime.now()
        self.spreadsheet_id = CONFIG["OUTPUT_SPREADSHEET_ID"]

    def _authenticate(self):
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = service_account.Credentials.from_service_account_file(
                CONFIG["SERVICE_ACCOUNT_FILE"], scopes=scopes)
            return build('sheets', 'v4', credentials=credentials)
        except Exception as e:
            logger.error(f"Ошибка аутентификации: {e}")
            raise

    def _wait_if_needed(self):
        elapsed = (datetime.now() - self.last_request_time).total_seconds()
        if elapsed < CONFIG["DELAY_BETWEEN_REQUESTS"]:
            time.sleep(CONFIG["DELAY_BETWEEN_REQUESTS"] - elapsed)
        self.last_request_time = datetime.now()

    def get_sheet_data(self, spreadsheet_id, sheet_name):
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                self._wait_if_needed()
                # Запрашиваем данные, чтобы получить форматированные значения, как они отображаются в таблице
                result = self.service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=sheet_name,
                    valueRenderOption='FORMATTED_VALUE'  # Изменено на FORMATTED_VALUE
                ).execute()

                values = result.get('values', [])

                if not values:
                    logger.warning(f"Лист {sheet_name} пуст или не найден в spreadsheets.values().get()")
                    return pd.DataFrame()

                raw_headers = values[0]  # Исходные заголовки из первой строки листа
                data = values[1:]

                # --- ДИАГНОСТИЧЕСКИЙ ЛОГ ВНУТРИ get_sheet_data ---
                logger.info(
                    f"GET_SHEET_DATA DEBUG ({sheet_name}): Количество исходных заголовков (len(raw_headers)): {len(raw_headers)}")
                logger.info(
                    f"GET_SHEET_DATA DEBUG ({sheet_name}): Первые 50 исходных заголовков: {raw_headers[:50]}")  # Логируем часть, чтобы не переполнять
                if len(raw_headers) > 50:
                    logger.info(
                        f"GET_SHEET_DATA DEBUG ({sheet_name}): Последние 50 исходных заголовков: {raw_headers[-50:]}")

                # Подсчитаем дубликаты в raw_headers для диагностики
                from collections import Counter
                header_counts = Counter(raw_headers)
                duplicate_headers_in_raw = {header: count for header, count in header_counts.items() if count > 1}
                if duplicate_headers_in_raw:
                    logger.info(
                        f"GET_SHEET_DATA DEBUG ({sheet_name}): Найдены дублирующиеся заголовки в raw_headers: {duplicate_headers_in_raw}")
                else:
                    logger.info(
                        f"GET_SHEET_DATA DEBUG ({sheet_name}): Дублирующихся заголовков в raw_headers НЕ найдено.")
                # --- КОНЕЦ ДИАГНОСТИЧЕСКОГО ЛОГА ---

                # Создаем уникальные заголовки для pandas, если они действительно дублируются в raw_headers
                # Это стандартное поведение pandas, но мы сделаем это явно для контроля
                processed_headers = []
                counts = {}
                for header_val in raw_headers:
                    if header_val is None: header_val = ''  # Заменяем None на пустую строку, если есть
                    str_header_val = str(header_val)  # Убедимся, что это строка
                    if str_header_val in counts:
                        counts[str_header_val] += 1
                        processed_headers.append(f"{str_header_val}.{counts[str_header_val] - 1}")
                    else:
                        counts[str_header_val] = 1
                        processed_headers.append(str_header_val)

                # --- ДИАГНОСТИЧЕСКИЙ ЛОГ ПОСЛЕ ОБРАБОТКИ ЗАГОЛОВКОВ ---
                logger.info(
                    f"GET_SHEET_DATA DEBUG ({sheet_name}): Количество обработанных заголовков (len(processed_headers)): {len(processed_headers)}")
                logger.info(
                    f"GET_SHEET_DATA DEBUG ({sheet_name}): Первые 50 обработанных заголовков: {processed_headers[:50]}")
                if len(processed_headers) > 50:
                    logger.info(
                        f"GET_SHEET_DATA DEBUG ({sheet_name}): Последние 50 обработанных заголовков: {processed_headers[-50:]}")
                # --- КОНЕЦ ДИАГНОСТИЧЕСКОГО ЛОГА ---

                normalized_data = []
                for row_index, row_values in enumerate(data):
                    # Важно: теперь нормализуем по длине processed_headers
                    current_row_normalized = list(row_values)  # Копируем, чтобы не изменять исходный список
                    if len(current_row_normalized) < len(processed_headers):
                        current_row_normalized.extend([''] * (len(processed_headers) - len(current_row_normalized)))
                    elif len(current_row_normalized) > len(processed_headers):
                        current_row_normalized = current_row_normalized[:len(processed_headers)]
                    normalized_data.append(current_row_normalized)

                if not normalized_data and not processed_headers:  # Если нет ни данных, ни заголовков
                    return pd.DataFrame()
                if not processed_headers and normalized_data:  # Если есть данные, но нет заголовков (маловероятно после values[0])
                    logger.warning(
                        f"GET_SHEET_DATA WARNING ({sheet_name}): Есть данные, но не удалось определить заголовки.")
                    return pd.DataFrame(normalized_data)  # Pandas сам сгенерирует числовые заголовки

                df = pd.DataFrame(normalized_data, columns=processed_headers)
                logger.info(
                    f"GET_SHEET_DATA INFO ({sheet_name}): DataFrame создан. Количество столбцов в DataFrame: {len(df.columns)}")
                logger.info(
                    f"GET_SHEET_DATA INFO ({sheet_name}): Столбцы DataFrame: {df.columns.tolist()[:50]}")  # Логируем часть столбцов DataFrame
                if len(df.columns) > 50:
                    logger.info(
                        f"GET_SHEET_DATA INFO ({sheet_name}): Последние 50 столбцов DataFrame: {df.columns.tolist()[-50:]}")

                return df

            except Exception as e:
                logger.warning(f"GET_SHEET_DATA ({sheet_name}): Попытка {attempt + 1} не удалась: {e}")
                if attempt == CONFIG["MAX_RETRIES"] - 1:
                    logger.error(f"GET_SHEET_DATA ({sheet_name}): Не удалось прочитать лист: {e}")
                    return pd.DataFrame()
                time.sleep(2 ** attempt)

        return pd.DataFrame()

    def write_sheet_data(self, spreadsheet_id, sheet_name, df):
        try:
            if df.empty:
                logger.info(f"Пустой DataFrame, нет данных для записи в {sheet_name}")
                return True, None

            required_rows = len(df) + 1
            self._ensure_sheet_capacity(spreadsheet_id, sheet_name, required_rows)

            self._wait_if_needed()
            existing_data = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()

            existing_values = existing_data.get('values', [])

            if not existing_values:
                logger.info(f"Таблица {sheet_name} пустая, записываем новые данные")
                for i in range(0, len(df), CONFIG["BATCH_SIZE"]):
                    batch = df.iloc[i:i + CONFIG["BATCH_SIZE"]]
                    self._write_batch(spreadsheet_id, sheet_name, batch, i == 0)
                return True, None

            headers = existing_values[0] if existing_values else []

            # Обновленная логика определения ключевого столбца для групп
            if "Export Groups Sheet" in sheet_name:
                id_column = "Ідентифікатор_групи" if "Ідентифікатор_групи" in df.columns else \
                    "Номер_групи" if "Номер_групи" in df.columns else \
                        df.columns[0]

                # Для групп проверяем также по названию группы
                name_column = "Назва_групи" if "Назва_групи" in df.columns else None
            else:
                id_column = "Ідентифікатор_товару" if "Ідентифікатор_товару" in df.columns else \
                    "Код_товару" if "Код_товару" in df.columns else df.columns[0]

            existing_data_dict = {}
            for i in range(1, len(existing_values)):
                row = existing_values[i]
                if len(row) > 0:
                    # Для групп используем комбинацию идентификатора и названия, если доступно
                    if "Export Groups Sheet" in sheet_name and name_column:
                        id_idx = headers.index(id_column) if id_column in headers else 0
                        name_idx = headers.index(name_column) if name_column in headers else -1

                        if id_idx < len(row) and name_idx < len(row):
                            row_key = (row[id_idx], row[name_idx])
                        elif id_idx < len(row):
                            row_key = row[id_idx]
                        else:
                            continue
                    else:
                        id_idx = headers.index(id_column) if id_column in headers else 0
                        if id_idx < len(row):
                            row_key = row[id_idx]
                        else:
                            continue

                    existing_data_dict[row_key] = {
                        'row_idx': i + 1,
                        'data': row
                    }

            # Определяем критические поля в зависимости от типа листа
            if "Export Groups Sheet" in sheet_name:
                critical_fields = ["Назва_групи", "Назва_групи_укр", "Ідентифікатор_групи"]
            else:
                critical_fields = ["Назва_позиції", "Назва_позиції_укр", "Опис", "Опис_укр", "Наявність", "Ціна", "Кількість", "Знижка"]

            critical_indices = [headers.index(field) for field in critical_fields if field in headers]

            # Для групп используем уникальность по идентификатору и названию
            if "Export Groups Sheet" in sheet_name and name_column:
                df_unique = df.drop_duplicates(subset=[id_column, name_column], keep='last')
            else:
                df_unique = df.drop_duplicates(subset=[id_column], keep='last')

            new_rows = pd.DataFrame(columns=df.columns)
            updates = []
            deleted_rows = set(existing_data_dict.keys())

            for idx, row in df_unique.iterrows():
                row_id = str(row[id_column])
                # Для групп создаем ключ для сравнения
                if "Export Groups Sheet" in sheet_name and name_column:
                    row_name = str(row[name_column]) if name_column in row and pd.notna(row[name_column]) else ""
                    row_key = (row_id, row_name)
                else:
                    row_key = row_id

                if row_key in existing_data_dict:
                    existing_row = existing_data_dict[row_key]['data']
                    row_idx = existing_data_dict[row_key]['row_idx']

                    deleted_rows.discard(row_key)

                    update_needed = False
                    update_values = existing_row.copy()

                    for col_idx in critical_indices:
                        if col_idx < len(headers):
                            df_col = headers[col_idx]
                            if df_col in row:
                                new_value = str(row[df_col]) if pd.notna(row[df_col]) else ''
                                old_value = str(existing_row[col_idx]) if col_idx < len(existing_row) else ''

                                if new_value != old_value:
                                    update_needed = True
                                    if col_idx < len(update_values):
                                        update_values[col_idx] = new_value
                                    else:
                                        update_values.extend([''] * (col_idx - len(update_values) + 1))
                                        update_values[col_idx] = new_value

                    if update_needed:
                        updates.append((row_idx, update_values))
                else:
                    new_rows = pd.concat([new_rows, pd.DataFrame([row])], ignore_index=True)

            # Остальная часть функции остается без изменений
            for row_idx, row_data in updates:
                range_name = f"{sheet_name}!A{row_idx}"

                self._wait_if_needed()
                self.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body={'values': [row_data]}
                ).execute()

            if not new_rows.empty:
                start_row = len(existing_values) + 1
                range_name = f"{sheet_name}!A{start_row}"

                for i in range(0, len(new_rows), CONFIG["BATCH_SIZE"]):
                    batch = new_rows.iloc[i:i + CONFIG["BATCH_SIZE"]]
                    values = batch.fillna('').values.tolist()

                    self._wait_if_needed()
                    self.service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        body={'values': values}
                    ).execute()

                    start_row += len(values)
                    range_name = f"{sheet_name}!A{start_row}"

            if deleted_rows:
                rows_to_delete = sorted([existing_data_dict[row_id]['row_idx'] for row_id in deleted_rows],
                                        reverse=True)

                logger.info(f"Удаление товаров/групп, отсутствующих у поставщика: {len(deleted_rows)} позиций")
                for row_id in deleted_rows:
                    logger.info(f"Позиция с ключом {row_id} отсутствует у поставщика и будет удалена")

                for row_idx in rows_to_delete:
                    request = {
                        'deleteDimension': {
                            'range': {
                                'sheetId': self._get_sheet_id(sheet_name),
                                'dimension': 'ROWS',
                                'startIndex': row_idx - 1,
                                'endIndex': row_idx
                            }
                        }
                    }

                    self._wait_if_needed()
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': [request]}
                    ).execute()

                    logger.info(f"Удалена строка {row_idx} (позиция отсутствует у поставщика)")

            sheet_type = "products" if "Export Products Sheet" in sheet_name else "groups"
            self._remove_duplicates(spreadsheet_id, sheet_name, sheet_type)

            logger.info(
                f"Данные успешно обновлены в {sheet_name}: {len(updates)} обновлено, {len(new_rows)} добавлено, {len(deleted_rows)} удалено")
            return True, None

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Ошибка записи в {sheet_name}: {error_msg}")
            return False, error_msg

    def _get_sheet_id(self, sheet_name):
        try:
            sheets_metadata = self.service.spreadsheets().get(
                spreadsheetId=CONFIG["OUTPUT_SPREADSHEET_ID"]
            ).execute()

            for sheet in sheets_metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']

            logger.error(f"Лист {sheet_name} не найден")
            raise ValueError(f"Лист {sheet_name} не найден")

        except Exception as e:
            logger.error(f"Ошибка получения ID листа: {e}")
            raise

    def _column_index_to_letter(self, index):
        result = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result = chr(65 + remainder) + result
        return result if result else "A"

    def _remove_duplicates(self, spreadsheet_id, sheet_name, sheet_type):
        try:
            self._wait_if_needed()
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()

            values = result.get('values', [])
            if not values or len(values) <= 1:
                logger.info(f"Нет данных для удаления дубликатов в {sheet_name}")
                return

            headers = values[0]

            if sheet_type == "products":
                id_col = "Ідентифікатор_товару" if "Ідентифікатор_товару" in headers else "Код_товару"
                if id_col not in headers:
                    logger.warning(f"Столбец {id_col} не найден в заголовках")
                    return

                id_idx = headers.index(id_col)
                critical_fields = {
                    "Наявність": headers.index("Наявність") if "Наявність" in headers else None,
                    "Ціна": headers.index("Ціна") if "Ціна" in headers else None,
                    "Кількість": headers.index("Кількість") if "Кількість" in headers else None,
                    "Знижка": headers.index("Знижка") if "Знижка" in headers else None
                }

                seen_ids = {}
                rows_to_delete = []
                rows_to_update = {}

                for i in range(1, len(values)):
                    row = values[i]
                    if len(row) > id_idx:
                        current_id = row[id_idx].strip()
                        if current_id in seen_ids:
                            original_idx = seen_ids[current_id]
                            original_row = values[original_idx]

                            update_needed = False
                            if original_idx not in rows_to_update:
                                rows_to_update[original_idx] = original_row.copy()

                            for field, idx in critical_fields.items():
                                if idx is not None and idx < len(row):
                                    if idx < len(row) and row[idx].strip():
                                        if idx >= len(rows_to_update[original_idx]):
                                            rows_to_update[original_idx].extend(
                                                [''] * (idx - len(rows_to_update[original_idx]) + 1))
                                        rows_to_update[original_idx][idx] = row[idx]
                                        update_needed = True

                            rows_to_delete.append(i + 1)
                        else:
                            seen_ids[current_id] = i

                for idx, updated_row in rows_to_update.items():
                    range_name = f"{sheet_name}!A{idx + 1}"

                    self._wait_if_needed()
                    self.service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        body={'values': [updated_row]}
                    ).execute()

                    logger.info(
                        f"Обновлена оригинальная строка {idx + 1} с данными из дубликатов: {updated_row[id_idx]}")

                for row_idx in sorted(rows_to_delete, reverse=True):
                    request = {
                        'deleteDimension': {
                            'range': {
                                'sheetId': self._get_sheet_id(sheet_name),
                                'dimension': 'ROWS',
                                'startIndex': row_idx - 1,
                                'endIndex': row_idx
                            }
                        }
                    }

                    self._wait_if_needed()
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': [request]}
                    ).execute()

                if rows_to_delete:
                    logger.info(f"Удалено {len(rows_to_delete)} дубликатов в {sheet_name}")

            else:
                group_num_col = "Ідентифікатор_групи"
                group_name_col = "Назва_групи"

                if group_num_col not in headers or group_name_col not in headers:
                    logger.warning("Не найдены необходимые столбцы для групп")
                    return

                num_idx = headers.index(group_num_col)
                name_idx = headers.index(group_name_col)

                seen_groups = {}
                rows_to_delete = []

                for i in range(1, len(values)):
                    row = values[i]
                    if len(row) > max(num_idx, name_idx):
                        group_key = (row[num_idx].strip(), row[name_idx].strip())
                        if group_key in seen_groups:
                            rows_to_delete.append(i + 1)
                        else:
                            seen_groups[group_key] = i

                for row_idx in sorted(rows_to_delete, reverse=True):
                    request = {
                        'deleteDimension': {
                            'range': {
                                'sheetId': self._get_sheet_id(sheet_name),
                                'dimension': 'ROWS',
                                'startIndex': row_idx - 1,
                                'endIndex': row_idx
                            }
                        }
                    }

                    self._wait_if_needed()
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': [request]}
                    ).execute()

                if rows_to_delete:
                    logger.info(f"Удалено {len(rows_to_delete)} дубликатов в {sheet_name}")

        except Exception as e:
            logger.error(f"Ошибка при удалении дубликатов в {sheet_name}: {e}")

    def _clear_sheet(self, spreadsheet_id, sheet_name):
        self._wait_if_needed()
        self.service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=sheet_name,
            body={}
        ).execute()

    def _write_batch(self, spreadsheet_id, sheet_name, batch_df, is_first_batch):
        cleaned_df = batch_df.copy()

        for col in cleaned_df.columns:
            if cleaned_df[col].dtype == 'object':
                cleaned_df[col] = cleaned_df[col].apply(
                    lambda x: '' if pd.isna(x) else str(x) if isinstance(x, (pd.Series, list, dict)) else x)

        if is_first_batch:
            values = [cleaned_df.columns.tolist()]
        else:
            values = []

        for _, row in cleaned_df.iterrows():
            row_list = []
            for col in cleaned_df.columns:
                value = row[col]
                if pd.isna(value):
                    row_list.append('')
                elif isinstance(value, (pd.Series, list, dict)):
                    row_list.append(str(value))
                else:
                    row_list.append(value)
            values.append(row_list)

        if is_first_batch:
            range_name = f"{sheet_name}!A1"
        else:
            start_row = self._get_next_empty_row(spreadsheet_id, sheet_name)
            range_name = f"{sheet_name}!A{start_row}"

        body = {'values': values}

        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                self._wait_if_needed()
                logger.info(f"Записываем пакет данных в {sheet_name} по диапазону {range_name}: {len(values)} строк")

                self.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                return
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1} записи не удалась: {str(e)}")
                if attempt == CONFIG["MAX_RETRIES"] - 1:
                    logger.error(f"Все попытки записи не удались. Последняя ошибка: {str(e)}")
                    raise Exception(f"Ошибка API при записи данных: {str(e)}")
                time.sleep(2 ** attempt)

    def _get_next_empty_row(self, spreadsheet_id, sheet_name):
        self._wait_if_needed()
        result = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:A"
        ).execute()

        values = result.get('values', [])
        return len(values) + 1

    def _ensure_sheet_capacity(self, spreadsheet_id, sheet_name, required_rows):
        try:
            self._wait_if_needed()
            sheets_info = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields='sheets.properties'
            ).execute()

            target_sheet = None
            for sheet in sheets_info.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    target_sheet = sheet
                    break

            if target_sheet:
                current_rows = target_sheet['properties'].get('gridProperties', {}).get('rowCount', 0)
                logger.info(f"Лист {sheet_name} имеет {current_rows} строк, требуется {required_rows}")

                if current_rows < required_rows:
                    new_row_count = max(required_rows + 1000, int(required_rows * 1.0))
                    logger.info(f"Расширяем лист {sheet_name} до {new_row_count} строк")

                    request = {
                        'updateSheetProperties': {
                            'properties': {
                                'sheetId': target_sheet['properties']['sheetId'],
                                'gridProperties': {
                                    'rowCount': new_row_count
                                }
                            },
                            'fields': 'gridProperties.rowCount'
                        }
                    }

                    self._wait_if_needed()
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': [request]}
                    ).execute()

                    logger.info(f"Лист {sheet_name} успешно расширен до {new_row_count} строк")
                    return True

            return False
        except Exception as e:
            logger.error(f"Ошибка при расширении листа {sheet_name}: {e}")
            return False


def load_mapping():
    try:
        with open(CONFIG["MAPPING_FILE"], 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки маппинга: {e}")
        raise


def get_template_columns():
    products_columns = [
        "Код_товару", "Назва_позиції", "Назва_позиції_укр", "Пошукові_запити", "Пошукові_запити_укр",
        "Опис", "Опис_укр", "Тип_товару", "Ціна", "Валюта", "Одиниця_виміру", "Мінімальний_обсяг_замовлення",
        "Оптова_ціна", "Мінімальне_замовлення_опт", "Посилання_зображення", "Наявність", "Кількість",
        "Номер_групи", "Назва_групи", "Посилання_підрозділу", "Можливість_поставки", "Термін_поставки",
        "Спосіб_пакування", "Спосіб_пакування_укр", "Унікальний_ідентифікатор", "Ідентифікатор_товару",
        "Ідентифікатор_підрозділу", "Ідентифікатор_групи", "Виробник", "Країна_виробник", "Знижка",
        "ID_групи_різновидів", "Особисті_нотатки", "Продукт_на_сайті", "Термін_дії_знижки_від",
        "Термін_дії_знижки_до", "Ціна_від", "Ярлик", "HTML_заголовок", "HTML_заголовок_укр",
        "HTML_опис", "HTML_опис_укр", "Код_маркування_(GTIN)", "Номер_пристрою_(MPN)",
        "Вага,кг", "Ширина,см", "Висота,см", "Довжина,см", "Де_знаходиться_товар",
        "Товар_на_видалення", "Причина_видалення_товару"
    ]

    for i in range(1, 25):
        products_columns.extend([
            f"Назва_Характеристики_{i}" if i > 1 else "Назва_Характеристики",
            f"Одиниця_виміру_Характеристики_{i}" if i > 1 else "Одиниця_виміру_Характеристики",
            f"Значення_Характеристики_{i}" if i > 1 else "Значення_Характеристики"
        ])

    groups_columns = [
        "Номер_групи", "Назва_групи", "Назва_групи_укр", "Ідентифікатор_групи",
        "Номер_батьківської_групи", "Ідентифікатор_батьківської_групи", "HTML_заголовок_групи",
        "HTML_заголовок_групи_укр", "HTML_опис_групи", "HTML_опис_групи_укр",
        "Опис_групи_до_списку_товарних_позицій", "Опис_групи_до_списку_товарних_позицій_укр",
        "Опис_групи_після_списку_товарних_позицій", "Опис_групи_після_списку_товарних_позицій_укр",
        "Посилання_зображення_групи"
    ]

    return products_columns, groups_columns


def transform_supplier_data(supplier_df, supplier_name, sheet_type, mapping):
    logger.info(f"Преобразование данных {supplier_name} для {sheet_type}")

    products_cols, groups_cols = get_template_columns()
    template_cols = products_cols if sheet_type == "products" else groups_cols

    mapping_key = "products_mapping" if sheet_type == "products" else "groups_mapping"
    supplier_mapping = mapping.get(mapping_key, {}).get(supplier_name, {})

    if not supplier_mapping:
        logger.warning(f"Нет маппинга для {supplier_name} ({sheet_type})")
        return pd.DataFrame(columns=template_cols)

    data_dict = {col: [] for col in template_cols}

    for _, row in supplier_df.iterrows():
        row_dict = row.to_dict() if isinstance(row, pd.Series) else row
        row_data = {col: '' for col in template_cols}

        # Специальная обработка для IZIDROP если это группы
        if supplier_name == "IZIDROP" and sheet_type == "groups":
            if 'parentId' in row_dict:
                row_dict['parentId'] = '' if pd.isna(row_dict['parentId']) else row_dict['parentId']

        # Добавляем обработку новых полей для каждого поставщика
        if supplier_name == "AGER":
            row_data['Ідентифікатор_товару'] = row_dict.get('id', '')
            row_data['Код_товару'] = row_dict.get('vendorCode', '')
        elif supplier_name == "IZIDROP":
            row_data['Ідентифікатор_товару'] = row_dict.get('id', '')
            row_data['Код_товару'] = row_dict.get('barcode', '')
        elif supplier_name in ["MOYDROP", "SPECULANT", "KIRS"]:
            row_data['Ідентифікатор_товару'] = row_dict.get('id', '')
            row_data['Код_товару'] = row_dict.get('vendorCode', '')
        # Для FOOTBALLERS идентификаторы должны мапиться через основной цикл, так как они есть в column_mapping.json

        # Основной цикл маппинга на основе column_mapping.json
        for supplier_col, target_col in supplier_mapping.items():
            if supplier_col not in row_dict:
                continue

            value = row_dict[supplier_col]
            if pd.isna(value) or value == '':
                continue

            if isinstance(target_col, list) and supplier_col.startswith("param_"):
                # Логика для param_ характеристик (используется AGER, SPECULANT, KIRS, etc.)
                char_name = target_col[
                    2]  # Например, "Бренд" из ["Назва_Характеристики", "Значення_Характеристики", "Бренд"]

                # Ищем первый свободный слот для характеристик в row_data
                for idx in range(1, 25):  # Проверяем слоты от _1 до _24
                    name_col_target = f"Назва_Характеристики_{idx}" if idx > 1 else "Назва_Характеристики"
                    value_col_target = f"Значення_Характеристики_{idx}" if idx > 1 else "Значення_Характеристики"
                    # Единицы измерения для param_ характеристик не задаются явным образом через эту структуру в вашем маппинге,
                    # но можно добавить, если необходимо, или они должны быть частью `value`

                    if not row_data[name_col_target]:  # Если слот для имени характеристики пуст
                        row_data[name_col_target] = char_name
                        str_value = str(value)
                        if str_value.startswith("'") and not str_value.startswith("''"):
                            str_value = str_value[1:]
                        row_data[value_col_target] = str_value
                        break  # Характеристика размещена, переходим к следующему param_ из маппинга
            else:  # Прямое сопоставление
                if target_col in row_data:
                    str_value = str(value)
                    if str_value.startswith("'") and not str_value.startswith("''"):
                        str_value = str_value[1:]
                    row_data[target_col] = str_value

        # Специальная обработка для повторяющихся характеристик "FOOTBALLERS"
        if supplier_name == "FOOTBALLERS" and sheet_type == "products":
            # Первый набор характеристик (например, "Назва_Характеристики" без суффикса)
            # уже обработан основным циклом маппинга, так как он есть в column_mapping.json
            # и будет помещен в целевые поля "Назва_Характеристики", "Значення_Характеристики" и т.д. (слот _1).

            # Теперь ищем характеристики с суффиксами ".1", ".2" и т.д. в исходных данных (row_dict)
            # и размещаем их в целевые слоты _2, _3 и т.д. (в row_data).
            current_char_slot_index = 2  # Начинаем заполнять со второго слота характеристик в целевой таблице

            for i in range(1, 24):  # Проверяем исходные характеристики с суффиксом от ".1" до ".23"
                # Это позволит заполнить целевые слоты от _2 до _24

                # Формируем имена столбцов в исходных данных (как их переименовывает pandas при чтении дубликатов)
                source_name_key = f"Назва_Характеристики.{i}"
                source_unit_key = f"Одиниця_виміру_Характеристики.{i}"
                source_value_key = f"Значення_Характеристики.{i}"

                # Формируем имена целевых столбцов в row_data
                target_name_col = f"Назва_Характеристики_{current_char_slot_index}"
                target_unit_col = f"Одиниця_виміру_Характеристики_{current_char_slot_index}"
                target_value_col = f"Значення_Характеристики_{current_char_slot_index}"

                # Проверяем, существует ли столбец с названием характеристики в исходных данных (row_dict) и не пустой ли он
                if source_name_key in row_dict and pd.notna(row_dict[source_name_key]) and str(
                        row_dict[source_name_key]).strip() != '':
                    # Убедимся, что не выходим за пределы 24 слотов для характеристик в целевой таблице
                    if current_char_slot_index <= 24:
                        row_data[target_name_col] = str(row_dict[source_name_key])

                        if source_value_key in row_dict and pd.notna(row_dict[source_value_key]):
                            row_data[target_value_col] = str(row_dict[source_value_key])
                        else:
                            row_data[target_value_col] = ''  # Явно устанавливаем пустое значение, если нет данных

                        if source_unit_key in row_dict and pd.notna(row_dict[source_unit_key]):
                            row_data[target_unit_col] = str(row_dict[source_unit_key])
                        else:
                            row_data[target_unit_col] = ''  # Явно устанавливаем пустое значение, если нет данных

                        current_char_slot_index += 1  # Переходим к следующему слоту в целевой таблице
                    else:
                        # Достигнут лимит слотов характеристик в целевой таблице
                        logger.warning(
                            f"Достигнут лимит в 24 характеристики для товара {row_dict.get('Код_товару', '')} от FOOTBALLERS. Последующие характеристики не будут добавлены.")
                        break
                else:
                    # Если очередной столбец Назва_Характеристики.i не найден или пуст,
                    # предполагаем, что больше характеристик для данного товара у поставщика нет.
                    break

        # Установка значений по умолчанию и других полей для продуктов
        if sheet_type == "products":
            if not row_data['Валюта']:
                row_data['Валюта'] = 'UAH'
            if not row_data['Одиниця_виміру']:
                row_data['Одиниця_виміру'] = 'шт.'

            # Обработка поля "Наявність" из исходных данных, если оно есть в row_dict (уже обработано основным циклом, если есть в маппинге)
            # Если поле "available" есть в row_dict, но не в маппинге, и нужно его учесть:
            if 'available' in row_dict and not row_data['Наявність']:  # Если 'Наявність' еще не заполнено
                available_value = str(row_dict['available']).lower().strip()
                if available_value in ['true', '+', '1', 'yes', 'да', 'в наличии', 'є в наявності']:
                    row_data['Наявність'] = '+'
                elif available_value in ['false', '-', '0', 'no', 'нет', 'немає в наявності']:
                    row_data['Наявність'] = '-'
                # Если другие значения, оставляем пустым или как есть, если уже заполнено

            # Обновление поля "Наявність" на основе поля "Кількість" если "Наявність" все еще не определено
            if not row_data['Наявність'] and 'Кількість' in row_data and row_data['Кількість'] != '':
                try:
                    quantity = int(
                        float(str(row_data['Кількість']).replace(',', '.')))  # Преобразуем количество в число
                    if quantity > 0:
                        row_data['Наявність'] = '+'
                    else:
                        row_data['Наявність'] = '-'
                except ValueError:
                    logger.warning(
                        f"Не удалось преобразовать Кількість '{row_data['Кількість']}' в число для товара {row_data.get('Код_товару', '')}")

            row_data['Особисті_нотатки'] = supplier_name

        for col in template_cols:
            data_dict[col].append(row_data.get(col, ''))

    result_df = pd.DataFrame(data_dict, columns=template_cols)

    if sheet_type == "groups":
        result_df['Джерело_даних'] = supplier_name

    logger.info(f"Данные {supplier_name} ({sheet_type}) успешно преобразованы, получено строк: {len(result_df)}")
    return result_df


def process_supplier(gsheets, spreadsheet_id, supplier_name, mapping):
    logger.info(f"Обработка поставщика {supplier_name}")

    try:
        gsheets._wait_if_needed()
        spreadsheet = gsheets.service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields='sheets.properties'
        ).execute()

        sheet_names = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
        logger.info(f"Найденные листы для {supplier_name}: {sheet_names}")

        products_df = None
        groups_df = None

        for sheet_name in sheet_names:
            sheet_lower = sheet_name.lower()

            if products_df is None and any(x in sheet_lower for x in ["product", "товар", "export"]):
                df = gsheets.get_sheet_data(spreadsheet_id, sheet_name)
                if not df.empty:
                    products_df = df
                    logger.info(f"Найден лист товаров: {sheet_name} ({len(df)} строк)")

            if groups_df is None:
                if supplier_name == "IZIDROP":
                    if sheet_name == "Export Groups Sheet":
                        raw_data = gsheets.service.spreadsheets().values().get(
                            spreadsheetId=spreadsheet_id,
                            range=sheet_name
                        ).execute()

                        values = raw_data.get('values', [])
                        if values:
                            data = []
                            headers = values[0] if len(values) > 0 else []

                            id_idx = headers.index('id') if 'id' in headers else 0
                            name_idx = headers.index('name') if 'name' in headers else 1
                            parent_idx = headers.index('parentId') if 'parentId' in headers else -1

                            for row in values[1:]:
                                item = {
                                    'id': row[id_idx] if len(row) > id_idx else '',
                                    'name': row[name_idx] if len(row) > name_idx else ''
                                }
                                if parent_idx != -1 and len(row) > parent_idx:
                                    item['parentId'] = row[parent_idx]
                                else:
                                    item['parentId'] = ''
                                data.append(item)

                            groups_df = pd.DataFrame(data)
                            logger.info(f"Обработан лист групп IZIDROP: {sheet_name} ({len(groups_df)} строк)")
                else:
                    if any(x in sheet_lower for x in ["group", "груп", "category"]):
                        df = gsheets.get_sheet_data(spreadsheet_id, sheet_name)
                        if not df.empty:
                            groups_df = df
                            logger.info(f"Найден лист групп: {sheet_name} ({len(df)} строк)")

        results = {}

        if products_df is not None:
            results["products"] = transform_supplier_data(
                products_df, supplier_name, "products", mapping)
        else:
            logger.warning(f"Нет данных товаров для {supplier_name}")
            results["products"] = pd.DataFrame()

        if groups_df is not None:
            if supplier_name == "IZIDROP":
                if 'parentId' not in groups_df.columns:
                    groups_df['parentId'] = ''
                groups_df['parentId'] = groups_df['parentId'].fillna('')

            results["groups"] = transform_supplier_data(
                groups_df, supplier_name, "groups", mapping)
            logger.debug(f"Пример преобразованных групп:\n{results['groups'].head()}")
        else:
            logger.warning(f"Нет данных групп для {supplier_name}")
            results["groups"] = pd.DataFrame()

        logger.info(f"Итоговые данные: товаров - {len(results['products'])}, групп - {len(results['groups'])}")
        return results

    except Exception as e:
        logger.error(f"Критическая ошибка при обработке {supplier_name}: {str(e)}", exc_info=True)
        return {"products": pd.DataFrame(), "groups": pd.DataFrame()}


def validate_dataframe(df, sheet_type):
    logger.info(f"Validating {sheet_type} DataFrame")

    rows, cols = df.shape
    logger.info(f"DataFrame size: {rows} rows x {cols} columns")

    for col in df.columns:
        max_len = df[col].astype(str).map(len).max()
        if max_len > 1000:
            logger.warning(f"Column '{col}' has very long values (max length: {max_len})")

        if df[col].dtype == 'object':
            unusual_chars = df[col].astype(str).str.contains('[^\\w\\s.,;:()\\[\\]{}"\'<>?!@#$%^&*+=\\-/\\\\\\\\]',
                                                             regex=True)
        if unusual_chars.any():
            unusual_count = unusual_chars.sum()
            logger.warning(f"Column '{col}' has {unusual_count} cells with unusual characters")

            # Check for duplicate column names (would cause issues with pandas)
        if len(df.columns) != len(set(df.columns)):
            logger.warning("DataFrame has duplicate column names!")
            for col in df.columns:
                if list(df.columns).count(col) > 1:
                    logger.warning(f"Column '{col}' appears multiple times")

        #logger.info(f"Validation complete for {sheet_type} DataFrame")
    return True


def main():
    try:
        logger.info("=== Начало работы скрипта ===")

        # Инициализация
        gsheets = GoogleSheetsManager()
        mapping = load_mapping()

        # Сбор данных от всех поставщиков
        all_products = []
        all_groups = []

        for supplier_name, spreadsheet_id in CONFIG["SUPPLIERS"].items():
            logger.info(f"Обработка поставщика {supplier_name}")
            supplier_data = process_supplier(gsheets, spreadsheet_id, supplier_name, mapping)

            if supplier_data.get("products") is not None:
                products_df = supplier_data["products"]
                logger.info(f"Получено продуктов от {supplier_name}: {len(products_df)}")

                # Валидация данных перед добавлением
                if validate_dataframe(products_df, "products"):
                    all_products.append(products_df)
                else:
                    logger.error(f"Ошибка валидации данных продуктов для {supplier_name}")

            if supplier_data.get("groups") is not None:
                groups_df = supplier_data["groups"]
                logger.info(f"Получено групп от {supplier_name}: {len(groups_df)}")

                # Валидация данных перед добавлением
                if validate_dataframe(groups_df, "groups"):
                    groups_with_source = groups_df.assign(Джерело_даних=supplier_name)
                    all_groups.append(groups_with_source)
                else:
                    logger.error(f"Ошибка валидации данных групп для {supplier_name}")

        # Объединение данных
        products_cols, groups_cols = get_template_columns()

        merged_products = pd.concat(all_products, ignore_index=True) if all_products \
            else pd.DataFrame(columns=products_cols)
        merged_groups = pd.concat(all_groups, ignore_index=True) if all_groups \
            else pd.DataFrame(columns=groups_cols)

        logger.info(f"Объединено продуктов: {len(merged_products)}")
        logger.info(f"Объединено групп: {len(merged_groups)}")

        # Проверка наличия обязательных столбцов
        required_product_columns = ["Код_товару", "Ідентифікатор_товару", "Назва_позиції", "Ціна", "Наявність"]
        for column in required_product_columns:
            if column not in merged_products.columns:
                logger.error(f"В объединенных продуктах отсутствует обязательный столбец: {column}")
                raise ValueError(f"Отсутствует обязательный столбец: {column}")

        required_group_columns = ["Номер_групи", "Назва_групи"]
        for column in required_group_columns:
            if column not in merged_groups.columns:
                logger.error(f"В объединенных группах отсутствует обязательный столбец: {column}")
                raise ValueError(f"Отсутствует обязательный столбец: {column}")

        # Запись результатов
        logger.info("Запись данных продуктов...")
        success, error = gsheets.write_sheet_data(
            CONFIG["OUTPUT_SPREADSHEET_ID"],
            "Export Products Sheet",
            merged_products
        )
        if not success:
            raise Exception(f"Ошибка записи продуктов: {error}")

        logger.info("Запись данных групп...")
        success, error = gsheets.write_sheet_data(
            CONFIG["OUTPUT_SPREADSHEET_ID"],
            "Export Groups Sheet",
            merged_groups
        )
        if not success:
            raise Exception(f"Ошибка записи групп: {error}")

        logger.info("=== Скрипт успешно завершен ===")

    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

