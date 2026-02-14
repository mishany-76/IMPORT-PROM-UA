import gspread

# ----------------------------------------------------
# Настройки: замените значения на свои
# ----------------------------------------------------
SPREADSHEET_ID = '1o6hic1hfDGfL6yynHjJD0cM8U_QaK_i_TERt19CQvOA'
WORKSHEET_NAME = 'Export Products Sheet'
CREDENTIALS_FILE = 'key_sheet.json'

# Названия столбцов (должны совпадать с названиями в вашей таблице)
PRODUCT_CODE_COL_NAME = 'Код_товару'
PRODUCT_TYPE_COL_NAME = 'Тип_товару'
MIN_ORDER_QTY_COL_NAME = 'Мінімальний_обсяг_замовлення'

# ----------------------------------------------------
# Список товаров для обновления
# Заполните этот список кодами товаров и новыми значениями
# ----------------------------------------------------
PRODUCTS_TO_UPDATE = [
    {
        'product_code': '1434',  # Замените на реальный код товара
        'new_product_type': 'w',
        'new_min_order_qty': '50'
    },
    {
        'product_code': '1788',  # Замените на реальный код товара
        'new_product_type': 'w',
        'new_min_order_qty': '50'
    },
{
        'product_code': '2341',  # Замените на реальный код товара
        'new_product_type': 'w',
        'new_min_order_qty': '20'
    },
    # Добавьте сюда столько товаров, сколько вам нужно, по тому же шаблону
    # {
    #     'product_code': 'КОД_ТОВАРА_3',
    #     'new_product_type': 'НОВОЕ_ЗНАЧЕНИЕ_ДЛЯ_ТИПА_ТОВАРА_3',
    #     'new_min_order_qty': 'НОВОЕ_ЗНАЧЕНИЕ_ДЛЯ_МИНИМАЛЬНОГО_ОБЪЕМА_3'
    # },
]

# ----------------------------------------------------
# Основная логика скрипта
# ----------------------------------------------------

def update_multiple_products():
    """
    Ищет несколько товаров по коду и обновляет два указанных столбца.
    """
    try:
        # Авторизация и открытие таблицы
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(WORKSHEET_NAME)

        print(f"Подключено к листу: {WORKSHEET_NAME}")

        # Получение заголовков
        headers = worksheet.row_values(1)

        try:
            # Находим индексы нужных столбцов
            product_code_col_index = headers.index(PRODUCT_CODE_COL_NAME) + 1
            product_type_col_index = headers.index(PRODUCT_TYPE_COL_NAME) + 1
            min_order_qty_col_index = headers.index(MIN_ORDER_QTY_COL_NAME) + 1
        except ValueError as e:
            print(f"Ошибка: Не удалось найти столбец с именем {e}. Проверьте названия столбцов.")
            return

        # Находим все значения в столбце "Код_товару"
        product_codes_list = worksheet.col_values(product_code_col_index)
        all_updates = []

        # Итерируемся по списку товаров, которые нужно обновить
        for product_data in PRODUCTS_TO_UPDATE:
            product_code = product_data['product_code']
            new_product_type = product_data['new_product_type']
            new_min_order_qty = product_data['new_min_order_qty']

            try:
                # Находим индекс строки для текущего товара
                row_index_to_update = product_codes_list.index(product_code) + 1
                print(f"Найден товар с кодом '{product_code}' в строке {row_index_to_update}.")

                # Создаем пакетное обновление для двух ячеек
                update_range_type = gspread.utils.rowcol_to_a1(row_index_to_update, product_type_col_index)
                update_range_qty = gspread.utils.rowcol_to_a1(row_index_to_update, min_order_qty_col_index)

                all_updates.append({
                    'range': update_range_type,
                    'values': [[new_product_type]]
                })
                all_updates.append({
                    'range': update_range_qty,
                    'values': [[new_min_order_qty]]
                })

            except ValueError:
                print(f"Предупреждение: Товар с кодом '{product_code}' не найден. Пропускаем его.")
                continue

        if all_updates:
            # Отправляем все обновления одним пакетом
            worksheet.batch_update(all_updates)
            print("Все данные успешно обновлены.")
        else:
            print("Не найдено товаров для обновления. Никаких изменений не внесено.")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Ошибка: Таблица с ID '{SPREADSHEET_ID}' не найдена. Проверьте ID.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Ошибка: Лист с названием '{WORKSHEET_NAME}' не найден. Проверьте название.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == '__main__':
    update_multiple_products()