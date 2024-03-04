import sqlite3
import sys
import time
import gspread
from tqdm import tqdm
import re
from utils import format_date, format_number
from utils import get_gspread_client, get_current_timestamp


def prepare_data_for_table(transactions_data, service_mapping, sku_to_artikul_dict=None):
    all_values = []

    print("\nНачало подготовки данных для таблицы...")
    # conn = sqlite3.connect('/home/k/kdm05mtg/ozongoogle/accruals.db')
    conn = sqlite3.connect('accruals.db')

    cursor = conn.cursor()

    columns = ['operation_id', 'operation_date', 'operation_type_name', 'posting_number', 'order_date',
               'warehouse_id', 'artikul', 'sku', 'item_name', 'accruals_for_sale', 'delivery_charge',
               'return_delivery_charge', 'sale_commission', 'delivery_schema'] + list(service_mapping.keys()) + [
                  'amount']
    cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS accruals (
                {' TEXT, '.join(columns)} TEXT
            )
        ''')
    conn.commit()
    cursor.execute(f'DELETE FROM accruals')
    conn.commit()

    for transaction in tqdm(transactions_data, desc="Обработка транзакций"):
        sku = transaction.get('items')[0].get('sku', '') if transaction.get('items') else ''
        artikul = sku_to_artikul_dict.get(sku, '')
        item_name = transaction.get('items')[0].get('name', '') if transaction.get('items') else ''
        row = [
            transaction.get('operation_id', ''),
            transaction.get('operation_date', ''),
            transaction.get('operation_type_name', ''),
            transaction.get('posting', {}).get('posting_number', ''),
            transaction.get('posting', {}).get('order_date', ''),
            transaction.get('posting', {}).get('warehouse_id', ''),
            artikul,
            sku,
            item_name,
            transaction.get('accruals_for_sale', 0),
            transaction.get('delivery_charge', 0),
            transaction.get('return_delivery_charge', 0),
            format_number(transaction.get('sale_commission', 0)),
            transaction.get('posting', {}).get('delivery_schema', '')
        ]
        services_data = {service['name']: service['price'] for service in transaction.get('services', [])}
        for service_name in service_mapping.keys():
            row.append(format_number(services_data.get(service_name, 0)))
        row.append(format_number(transaction.get('amount', 0)))
        all_values.append(row)
        placeholders = ', '.join(['?'] * len(row))
        cursor.execute(f'INSERT INTO accruals VALUES ({placeholders})', row)
    conn.commit()
    try:
        cursor.execute('SELECT * FROM accruals')
        all_values = cursor.fetchall()
        print("\nПодготовка данных для таблицы завершена.")
        return all_values
    except sqlite3.Error as e:
        print(f"\nОшибка при чтении из базы данных: {e}")
        return None
    finally:
        conn.close()


def fbo_table(transactions_data):
    print("\nНачало подготовки данных для таблицы...")
    all_values = []
    status_mapping = {
        "awaiting_packaging": "ожидает упаковки",
        "awaiting_deliver": "ожидает отгрузки",
        "delivering": "доставляется",
        "delivered": "доставлено",
        "cancelled": "отменено"
    }
    for index, transaction in enumerate(tqdm(transactions_data, desc="Обработка транзакций")):
        created_at_formatted = format_date(transaction.get('created_at', ''))
        in_process_at = format_date(transaction.get('in_process_at', ''))

        product = transaction.get('products', [{}])[0]
        financial_data_product = transaction.get('financial_data', {}).get('products', [{}])[0]
        row = [
            transaction.get('analytics_data', {}).get('city', ''),
            transaction.get('analytics_data', {}).get('warehouse_name', ''),
            created_at_formatted,
            in_process_at,
            transaction.get('order_id', ''),
            transaction.get('order_number', ''),
            transaction.get('posting_number', ''),
            product.get('name', ''),
            product.get('offer_id', ''),
            product.get('price', ''),
            product.get('quantity', ''),
            product.get('sku', ''),
            status_mapping.get(transaction.get('status', ''), ''),
            product.get('currency_code', ''),
            financial_data_product.get('client_price', ''),
            financial_data_product.get('commission_amount', ''),
            financial_data_product.get('commission_percent', ''),
            financial_data_product.get('currency_code', ''),
            financial_data_product.get('old_price', ''),
            financial_data_product.get('payout', ''),
            financial_data_product.get('price', ''),
            transaction.get('moment', ''),
            financial_data_product.get('price', ''),
            financial_data_product.get('product_id', ''),
            financial_data_product.get('total_discount_percent', ''),
            financial_data_product.get('total_discount_value', '')
        ]
        if index % 10000 == 0:
            print(f"\nОбработано {index} записей из {len(transactions_data)}")
        all_values.append(row)
    return all_values


def adv_table(transactions_data, fields, value_mappings=None):
    print("\nНачало подготовки данных для таблицы...")
    all_values = []
    for transaction in tqdm(transactions_data, desc="Обработка данных"):
        row = []
        for field in fields:
            value = transaction.get(field, '')
            if field == 'status' and value_mappings:
                value = value_mappings.get(value, value)
            if isinstance(value, str):
                value = value.replace('.', ',')
            row.append(value)
        all_values.append(row)

    print("\nПодготовка данных для таблицы завершена.")
    all_values.reverse()
    return all_values


def update_sheet(transactions_data, worksheet_name, headers, format_headers=None, sku_to_artikul=None,
                 service_mapping=None, fields=None, value_mappings=None):
    sheet_name = sys.argv[1]

    print(f"\nНачало работы функции update_sheet над листом {worksheet_name}...\n")

    report_time_row = ["Дата и время формирования отчета", f"{get_current_timestamp()[0]}"]
    print("\nЗаголовки и время отчета подготовлены.")
    
    if re.match(r"Выгрузка FBO \(([1|2])\)", worksheet_name):
        all_values = fbo_table(transactions_data)
    elif re.match(r"Выгрузка (ТРАФАРЕТЫ|ПОИСК) \(([12])\)", worksheet_name):
        all_values = adv_table(transactions_data, fields, value_mappings)
    else:
        all_values = prepare_data_for_table(transactions_data, service_mapping, sku_to_artikul)

    client = get_gspread_client()
    max_retries = 5
    retry_interval = 120
    for attempt in range(max_retries):
        try:
            print(f"\nПопытка {attempt + 1} из {max_retries}...")

            print(f"\nОткрытие таблицы '{sheet_name}'.")
            sheet = client.open(sheet_name)

            print(f"\nПолучение листа '{worksheet_name}'.")
            worksheet = sheet.worksheet(worksheet_name)

            print("\nОчистка листа.")
            worksheet.clear()

            print("\nСчитаем количество строк...")
            worksheet.insert_row(report_time_row, 1)
            worksheet.insert_row(headers, 2)
            num_rows = len(all_values)
            chunk_size = max(1, num_rows // 10)
            print(f"\nДанные будут добавлены в {min(11, num_rows)} частей.\n")
            total_chunks = -(-num_rows // chunk_size)
            for i in range(0, num_rows, chunk_size):
                chunk = all_values[i:i + chunk_size]
                worksheet.append_rows(chunk, 'USER_ENTERED')

                sys.stdout.write(f"\rДобавлено частей: {(i // chunk_size) + 1}/{total_chunks}")
                sys.stdout.flush()
            if format_headers:
                if re.match(r"Выгрузка НАЧИСЛЕНИЙ\((1|2)\)", worksheet_name):
                    worksheet.format('B:B', {"numberFormat": {"type": "DATE"}})
                    worksheet.format('E:E', {"numberFormat": {"type": "DATE_TIME"}})
                    num_format = {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}
                    columns_to_format = ['J', 'K', 'L', 'M', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
                                         'Z'] + \
                                        ['AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM',
                                         'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AV']

                    for col in columns_to_format:
                        worksheet.format(f'{col}:{col}', num_format)
                elif re.match(r"Выгрузка ОСТАТКИ \((1|2)\)", worksheet_name):
                    worksheet.format('C:C', {"numberFormat": {"type": "DATE"}})
                    worksheet.format('D:D', {"numberFormat": {"type": "DATE_TIME"}})
                elif re.match(r"Выгрузка (ТРАФАРЕТЫ|ПОИСК) \((1|2)\)", worksheet_name):
                    worksheet.format('A:A', {"numberFormat": {"type": "DATE"}})
            print("\nОбновление таблицы завершено успешно.\n")
            break
        except gspread.exceptions.APIError as e:
            print(f"\nОшибка при попытке {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"\nСледующая попытка через {retry_interval} секунд.")
                time.sleep(retry_interval)
            else:
                print("\nПревышено максимальное количество попыток. Операция не выполнена.")
                break
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Таблица '{sheet_name}' не найдена. Проверьте названия и права доступа.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Лист '{worksheet_name}' не найден в таблице.")

