import re
import sys
import time
from utils import get_gspread_client, get_headers, get_current_timestamp
import requests
import gspread
import json


def get_stock_on_warehouses():
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    headers = get_headers()
    all_data = []
    offset = 0
    limit = 1000

    while True:
        body = {
            "limit": limit,
            "offset": offset,
            "warehouse_type": "ALL"
        }

        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            warehouse_data = response.json()
            if ('result' not in warehouse_data
                    or 'rows' not in warehouse_data['result'] or not warehouse_data['result']['rows']):
                break
            all_data.extend(warehouse_data['result']['rows'])
            offset += limit
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    return all_data


def extract_sku_artikul_pairs(data):
    unique_pairs = set()
    for item in data:
        sku = item.get('sku')
        artikul = item.get('item_code')
        if sku is not None and artikul is not None:
            unique_pairs.add((sku, artikul))

    arg = sys.argv[1]
    print(f'Полученный аргумент: {arg}')
    match = re.search(r'(1|2)', arg)
    if match:
        print(f'Результат поиска регулярного выражения: {match.group()}')
    else:
        print('Соответствий не найдено')

    filename = '/home/k/kdm05mtg/ozongoogle/sku_artikul_pairs-1.json' if match and match.group() == '1' else \
        '/home/k/kdm05mtg/ozongoogle/sku_artikul_pairs-2.json'

    print(f'Сохранение в файл: {filename}')
    print(f'Уникальные пары для сохранения: {unique_pairs}')

    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(list(unique_pairs), file, ensure_ascii=False, indent=4)


def update_leftovers_sheet(rows_data):
    sheet_name = sys.argv[1]
    worksheet_name = f'Выгрузка ОСТАТКИ ({next(num for num in "12" if num in sheet_name)})'

    headers = ["SKU", "Название склада", "Артикул", "Название товара", "Товары в пути", "Доступный к продаже товар",
               "Резерв"]

    current_datetime = get_current_timestamp()[1].strftime("%d.%m.%Y %H:%M:%S")
    all_values = [
                     ["Отчёт по остаткам и товарам в пути на склады Ozon", "", "", "", "", "", ""],
                     ["Дата и время формирования отчёта", current_datetime, "", "", "", "", ""],
                     ["Склады:", "Все склады", "", "", "", "", ""],
                     headers
                 ] + [list(row.values()) + [""] * (len(headers) - len(row.values())) for row in rows_data]

    client = get_gspread_client()

    extract_sku_artikul_pairs(rows_data)
    max_retries = 5
    retry_interval = 120

    for attempt in range(max_retries):
        try:
            sheet = client.open(sheet_name)
            worksheet = sheet.worksheet(worksheet_name)

            worksheet.clear()

            main_data_range = 'A1:' + gspread.utils.rowcol_to_a1(len(all_values), len(headers))
            main_data_update = {'range': main_data_range, 'values': all_values}

            additional_updates = [
                {'range': 'I1', 'values': [['Остатки']]}
            ]

            worksheet.batch_update([main_data_update] + additional_updates)
            worksheet.update_acell('H1', '=SUM(F:F)')

            header_range = 'A4:' + gspread.utils.rowcol_to_a1(4, len(headers))
            worksheet.format(header_range, {'textFormat': {'bold': True}})
            data_range = 'A4:' + gspread.utils.rowcol_to_a1(len(all_values), len(headers))
            worksheet.set_basic_filter(data_range)
            
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
            print(f"\nТаблица '{sheet_name}' не найдена. Проверьте названия и права доступа.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Лист '{worksheet_name}' не найден в таблице.")


def main():
    leftovers_data = get_stock_on_warehouses()

    if isinstance(leftovers_data, list):
        update_leftovers_sheet(leftovers_data)
    else:
        print("\nОшибка извлечения данных:", leftovers_data)