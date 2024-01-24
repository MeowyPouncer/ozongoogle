import sys
from campaigns import get_campaigns_report, get_campaigns_id
from data_processors import update_sheet


def main():
    fields = [
        'date', 'id', 'title', 'moneySpent', 'views', 'clicks',
        'avgBid', 'orders', 'ordersMoney'
    ]
    search_headers = [
        "Дата",
        "Идентификатор кампании",
        "Название кампании",
        "Расход",
        "Показы",
        "Клики",
        "Средняя ставка",
        "Заказы",
        "Заказы в рублях",
    ]
    search_promo_id, sku_id = get_campaigns_id()
    report_data = get_campaigns_report(search_promo_id)
    worksheet_name = f'Выгрузка ПОИСК ({next(num for num in "12" if num in sys.argv[1])})'

    update_sheet(report_data, worksheet_name, search_headers, fields=fields, format_headers=True)
