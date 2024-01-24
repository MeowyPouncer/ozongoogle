import sys

from campaigns import fetch_data_last_two_months
from data_processors import update_sheet


def main():
    fields = [
        'date', 'id', 'title', 'status', 'dailyBudget', 'budget',
        'moneySpent', 'views', 'clicks', 'avgBid', 'viewPrice',
        'ctr', 'clickPrice', 'orders', 'ordersMoney', 'drr'
    ]
    status_mapping = {
        "running": "активна",
        "inactive": "неактивна",
        "archived": "в архиве",
        "stopped": "приостановлена",
        "cancelled": "отменена"
    }
    stencil_headers = [
        "Дата",
        "Идентификатор кампании",
        "Название кампании",
        "Статус",
        "Бюджет в день",
        "Общий бюджет",
        "Расход",
        "Показы",
        "Клики",
        "Средняя ставка",
        "Цена за просмотр",
        "CTR",
        "Цена за клик",
        "Заказы",
        "Заказы в рублях",
        "ДРР"
    ]
    trans_data = fetch_data_last_two_months()
    worksheet_name = f'Выгрузка ТРАФАРЕТЫ ({next(num for num in "12" if num in sys.argv[1])})'
    update_sheet(trans_data, worksheet_name, stencil_headers, fields=fields,
                 value_mappings=status_mapping, format_headers=True)
