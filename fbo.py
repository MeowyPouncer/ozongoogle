import sys
from datetime import datetime, timedelta, timezone
import requests
from tqdm import tqdm
from data_processors import update_sheet
from utils import get_headers, get_current_timestamp


def get_fbo_list():
    print("\nНачало выполнения функции get_fbo_list")
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    headers = get_headers()

    all_data = []
    limit = 1000
    current_datetime = get_current_timestamp()[1]
    start_date = (current_datetime - timedelta(days=60)).replace(tzinfo=timezone.utc)
    total_days = (current_datetime - start_date).days

    print(
        f"\nСбор данных с {start_date.strftime('%d.%m.%Y')} по {current_datetime.strftime('%d.%m.%Y')} "
        f"(всего {total_days} дней)")

    pbar = tqdm(total=total_days)
    while start_date <= current_datetime:
        end_date = start_date + timedelta(days=10)
        if end_date > current_datetime:
            end_date = current_datetime

        end_date = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)
        since_date = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
        to_date = end_date.strftime("%Y-%m-%dT23:59:59.999Z")

        sys.stdout.write(
            f"\nОбработка данных с {datetime.fromisoformat(since_date.rstrip('Z')).strftime('%d.%m.%Y')} по "
            f"{datetime.fromisoformat(to_date.rstrip('Z')).strftime('%d.%m.%Y')}\n")
        sys.stdout.flush()

        offset = 0
        while True:
            body = {
                "dir": "ASC",
                "filter": {
                    "since": since_date,
                    "to": to_date,
                    "status": ""
                },
                "limit": limit,
                "offset": offset,
                "translit": False,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }

            try:
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                fbo_data = response.json()
                if not fbo_data.get('result'):
                    break
                all_data.extend(fbo_data['result'])
                offset += limit
            except requests.exceptions.RequestException as e:
                error_message = f"{e.response.status_code} {e.response.reason}: {e.response.text} for url: {e.request.url}"
                print("Ошибка при выполнении запроса:", error_message)
                return {"error": error_message}

        pbar.update((end_date - start_date).days)
        start_date = end_date + timedelta(seconds=1)

    pbar.close()
    all_data.reverse()
    print("\nЗавершение работы функции get_fbo_list")
    return all_data


def main():
    fbo_headers = [
        "Город доставки", "Название склада отправки", "Дата создания",
        "Дата начала обработки", "ID заказа",
        "Номер заказа", "Номер отправления", "Название товара", "Артикул",
        "Цена товара", "Количество товара", "SKU", "Статус отправления", "Валюта цен",
        "Цена для клиента", "Размер комиссии", "Процент комиссии", "Валюта комиссии",
        "Цена до скидки", "Выплата продавцу", "Стоимость доставки", "Дата и время доставки",
        "Цена товара с учетом скидок", "ID товара",
        "Процент скидки", "Сумма скидки"
    ]
    trans_data = get_fbo_list()
    worksheet_name = f'Выгрузка FBO ({next(num for num in "12" if num in sys.argv[1])})'
    update_sheet(trans_data, worksheet_name, fbo_headers)
