import os
from datetime import datetime, timezone, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'placeholder.json', scope)
    client = gspread.authorize(creds)
    return client


def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%d-%m-%Y, %H:%M')
    except ValueError:
        return ''


def format_number(value):
    if isinstance(value, (float, int)):
        return f"{value:.2f}".replace('.', ',')
    return value


def get_current_timestamp():
    utc_now = datetime.now(timezone.utc)
    msk_now = utc_now + timedelta(hours=3)
    return f"{utc_now.strftime('%d.%m.%Y %H:%M')} UTC / {msk_now.strftime('%d.%m.%Y %H:%M')} MSK", utc_now


def format_headers(headers, worksheet):
    header_range = 'A2:' + gspread.utils.rowcol_to_a1(2, len(headers))
    worksheet.format(header_range, {'textFormat': {'bold': True}})


def get_headers():
    # client_id = os.getenv('OZON_CLIENT_ID')
    # api_key = os.getenv('OZON_API_KEY')
    client_id = '1020252'
    api_key = 'f65c3673-9a24-4ce8-9653-e241011f3c40'
    if not client_id or not api_key:
        raise Exception("Необходимо задать переменные окружения OZON_CLIENT_ID и OZON_API_KEY")
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key
    }
    return headers
