import os
import sys
import requests
import json
from datetime import timedelta

from utils import get_current_timestamp


def get_auth_token():
    url = "https://performance.ozon.ru:443/api/client/token"

    client_id = os.getenv('OZON_PERF_CLIENT_ID')
    client_secret = os.getenv('OZON_PERF_CLIENT_SECRET')

    if not client_id or not client_secret:
        raise Exception("Необходимо задать переменные окружения OZON_PERF_CLIENT_ID и OZON_PERF_CLIENT_SECRET")

    payload = json.dumps({
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    })

    response = requests.request("POST", url, data=payload)

    return response.json()['token_type'], response.json()['access_token']


def get_campaigns_id():
    url = "https://performance.ozon.ru:443/api/client/campaign"

    token_type, access_token = get_auth_token()
    auth_header = f"{token_type} {access_token}"

    headers = {'Authorization': auth_header}

    response = requests.get(url, headers=headers)

    data = json.loads(response.text)

    search_promo_ids = []
    sku_ids = []

    for item in data["list"]:
        if item["advObjectType"] == "SEARCH_PROMO":
            search_promo_ids.append(item["id"])
        elif item["advObjectType"] == "SKU":
            sku_ids.append(item["id"])

    return search_promo_ids, sku_ids


def get_campaigns_report(campaign_ids):
    url = "https://performance.ozon.ru:443/api/client/statistics/daily/json"
    all_data = []
    token_type, access_token = get_auth_token()
    auth_header = f"{token_type} {access_token}"

    headers = {'Authorization': auth_header}

    date_from = (get_current_timestamp()[1] - timedelta(days=60)).strftime('%Y-%m-%d')
    date_to = get_current_timestamp()[1].strftime('%Y-%m-%d')

    querystring = {
        'campaignIds': campaign_ids,
        'dateFrom': date_from,
        'dateTo': date_to
    }

    response = requests.get(url, params=querystring, headers=headers)

    if response.status_code == 200:
        all_data = response.json()['rows']
        print(f"\nСтрок успешно извлечено: {len(all_data)} rows")
    else:
        print(f"\nОшибка извлечения данных: {response.text}")

    return all_data


def fetch_data_last_two_months():
    base_url = "https://performance.ozon.ru:443/api/client/statistics/campaign/product/json"

    token_type, access_token = get_auth_token()
    auth_header = f"{token_type} {access_token}"

    headers = {'Authorization': auth_header}

    end_date = get_current_timestamp()[1]
    start_date = end_date - timedelta(days=60)

    total_days = (end_date - start_date).days + 1

    days_fetched = 0

    all_data = []
    current_date = start_date

    while current_date <= end_date:
        formatted_date = current_date.strftime("%Y-%m-%d")
        url = f"{base_url}?dateFrom={formatted_date}&dateTo={formatted_date}"

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            daily_data = response.json()
            for item in daily_data.get('rows', []):
                item['date'] = formatted_date
            all_data.extend(daily_data.get('rows', []))
            days_fetched += 1
            progress = (days_fetched / total_days) * 100

            sys.stdout.write(f"\rВыгружаем данные: {days_fetched} из {total_days} дня ({progress:.2f}%) завершено.")
            sys.stdout.flush()
        else:
            print(f"Ошибка выгрузки данных {formatted_date}: {response.text}")

        current_date += timedelta(days=1)

    return all_data
