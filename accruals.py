import sys
import re
from datetime import datetime, timedelta
from data_processors import update_sheet
from utils import get_headers, get_current_timestamp
import requests
import json
from tqdm import tqdm


def fetch_transactions(url, headers, from_date, to_date, all_transactions):
    page = 1
    total_pages = 1

    print(
        f"\nВыгружаем транзакции с {datetime.fromisoformat(from_date.rstrip('Z')).strftime('%d.%m.%Y')} по "
        f"{datetime.fromisoformat(to_date.rstrip('Z')).strftime('%d.%m.%Y')}...")

    with tqdm(total=total_pages, desc="Загрузка страниц") as pbar:
        while True:
            body = {
                "filter": {
                    "date": {
                        "from": from_date,
                        "to": to_date
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all"
                },
                "page": page,
                "page_size": 1000
            }

            try:
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                data = response.json()
                operations = data.get('result', {}).get('operations', [])
                page_count = data.get('result', {}).get('page_count', 0)

                if page == 1:
                    total_pages = page_count
                    pbar.total = total_pages

                if not operations:
                    break

                all_transactions.extend(operations)
                pbar.update(1)
                page += 1
            except requests.exceptions.RequestException as e:
                print("Ошибка при запросе:", e)
                return {"error": str(e)}

    print(f"\nВыгрузка данных транзакций с {datetime.fromisoformat(from_date.rstrip('Z')).strftime('%d.%m.%Y')} по "
          f"{datetime.fromisoformat(to_date.rstrip('Z')).strftime('%d.%m.%Y')} завершена.")


def get_transactions(from_date_start, to_date_end):
    url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
    headers = get_headers()

    all_transactions = []

    fetch_transactions(url, headers, from_date_start, to_date_end, all_transactions)

    return all_transactions


def main():
    arg = sys.argv[1]
    match = re.search(r'(1|2)', arg)

    filename = '/home/k/kdm05mtg/ozongoogle/sku_artikul_pairs-1.json' if match and match.group() == '1' else \
        '/home/k/kdm05mtg/ozongoogle/sku_artikul_pairs-2.json'

    with open(filename, 'r', encoding='utf-8') as file:
        sku_artikul_pairs = json.load(file)
        sku_to_artikul = {pair[0]: pair[1] for pair in sku_artikul_pairs}

    current_datetime = get_current_timestamp()[1]
    first_month_start = (current_datetime - timedelta(days=60)).strftime("%Y-%m-%dT00:00:00.000Z")
    first_month_end = (current_datetime - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00.000Z")
    second_month_start = (current_datetime - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
    second_month_end = current_datetime.strftime("%Y-%m-%dT00:00:00.000Z")

    trans_data = get_transactions(first_month_start, first_month_end)
    trans_data.extend(get_transactions(second_month_start, second_month_end))
    trans_data.reverse()

    service_map = {
        "MarketplaceNotDeliveredCostItem": "Возврат невостребованного товара от покупателя на склад",
        "MarketplaceReturnAfterDeliveryCostItem": "Возврат от покупателя на склад после доставки",
        "MarketplaceDeliveryCostItem": "Доставка товара до покупателя",
        "MarketplaceSaleReviewsItem": "Приобретение отзывов на платформе",
        "ItemAdvertisementForSupplierLogistic": "Доставка товаров на склад Ozon — кросс-докинг",
        "MarketplaceServiceStorageItem": "Размещения товаров",
        "MarketplaceMarketingActionCostItem": "Продвижение товаров",
        "MarketplaceServiceItemInstallment": "Продвижение и продажа в рассрочку",
        "MarketplaceServiceItemMarkingItems": "Обязательная маркировка товаров",
        "MarketplaceServiceItemFlexiblePaymentSchedule": "Гибкий график выплат",
        "MarketplaceServiceItemReturnFromStock": "Комплектация товаров для вывоза продавцом",
        "ItemAdvertisementForSupplierLogisticSeller": "Транспортно-экспедиционные услуги",
        "MarketplaceServiceItemDelivToCustomer": "Последняя миля",
        "MarketplaceServiceItemDirectFlowTrans": "Магистраль",
        "MarketplaceServiceItemDropoffFF": "Обработка отправления (FF)",
        "MarketplaceServiceItemDropoffPVZ": "Обработка отправления (PVZ)",
        "MarketplaceServiceItemDropoffSC": "Обработка отправления (SC)",
        "MarketplaceServiceItemFulfillment": "Сборка заказа",
        "MarketplaceServiceItemPickup": "Pick-up",
        "MarketplaceServiceItemReturnAfterDelivToCustomer": "Обработка возврата после доставки покупателю",
        "MarketplaceServiceItemReturnFlowTrans": "Обратная магистраль",
        "MarketplaceServiceItemReturnNotDelivToCustomer": "Обработка отмененных или невостребованных товаров",
        "MarketplaceServiceItemReturnPartGoodsCustomer": "Обработка невыкупленных товаров",
        "MarketplaceRedistributionOfAcquiringOperation": "Оплата эквайринга",
        "MarketplaceReturnStorageServiceAtThePickupPointFbsItem": "Краткосрочное размещение возврата FBS",
        "MarketplaceReturnStorageServiceInTheWarehouseFbsItem": "Долгосрочное размещение возврата FBS",
        "MarketplaceServiceItemDeliveryKGT": "Доставка крупногабаритного товара (КГТ)",
        "MarketplaceServiceItemDirectFlowLogistic": "Логистика",
        "MarketplaceServiceItemReturnFlowLogistic": "Обратная логистика",
        "MarketplaceServicePremiumCashbackIndividualPoints": "Услуга продвижения «Бонусы продавца»",
        "MarketplaceServicePremiumPromotion": "Услуга продвижение Premium, фиксированная комиссия",
        "OperationMarketplaceWithHoldingForUndeliverableGoods": "Удержание за недовложение товара",
        "MarketplaceServiceItemDropoffPPZ": "Услуга drop-off в пункте приёма заказов"
    }
    trans_headers = [
                        "ID", "Дата", "Тип начисления", "Номер отправления или идентификатор услуги",
                        "Дата принятия заказа в обработку или оказания услуги", "Склад отгрузки", "Артикул",
                        "SKU", "Название товара или услуги", "Стоимость товаров", "Плата за доставку",
                        "Плата за возврат", "Комиссия за продажу", "Схема доставки"
                    ] + list(service_map.values()) + ["Итого"]
    arg = sys.argv[1]
    match = re.search(r'(1|2)', arg)
    if match:
        number = match.group(1)
    else:
        number = 'UNKNOWN'
    
    worksheet_name = f'Выгрузка НАЧИСЛЕНИЙ({number})'

    update_sheet(trans_data, worksheet_name, trans_headers, format_headers=True,
                 sku_to_artikul=sku_to_artikul, service_mapping=service_map)
