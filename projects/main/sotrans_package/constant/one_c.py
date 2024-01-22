def get_doc_alias() -> dict[str,str]:
    """
    Notes:
        Функция возвращает словать "тип документа: алиас документа".

    Returns:
        (dict[str,str]): Словарь с типами и алиасами документов 1С.
    """

    return {
        "start_balance": "stb",
        "income_balance": "inb",
        "expend_balance": "exb",
        "end_balance": "enb",
        "transit_balance": "trb",
        "loss_demand": "ldb"
    }


def get_doc_names() -> dict[str, dict[str, str]]:
    """
    Notes:
        Функция принимает на вход тип документа и возвращает словарь {рус: англ} наименований.

    Returns:
        (dict[str, dict[str, str]]): Словарь {рус: англ} наименований.
    """

    return {
        # Start balance
        "start_balance": {
            "начальный остаток": "start_balance"
        },

        # Income Balance
        "income_balance": {
            "ввод остатков товаров": "product_entering_balances",
            "перемещение товаров": "product_movement",
            "поступление товаров": "product_receipt",
            "пересортица товаров": "product_resort",
            "возврат товаров поставщику": "product_return_to_dealer",
            "инвентаризация товаров": "product_inventory",
            "поступление дополнительных расходов": "receipt_of_additional_expenses",
            "разукомплектация": "bundle_separation",
            "комплектация": "bundle",
            "корректировка": "income_correction",
            "корректировка поступления": "income_correction"
        },

        # Expend balance
        "expend_balance": {
            "перемещение товаров": "product_movement",
            "реализация товаров": "product_sale",
            "возврат от покупателя": "product_return_from_customer",
            "пересортица товаров": "product_resort",
            "списание товаров": "product_writeoff",
            "закрытие кассовой смены": "close_cash_register",
            "инвентаризация товаров": "product_inventory",
            "разукомплектация": "bundle_separation",
            "комплектация": "bundle",
            "корректировка реализации": "product_sale_inplementation",
            "корректировка": "product_sale_inplementation"
        },

        # End balance
        "end_balance": {
            "конечный остаток": "end_balance"
        },

        # Transit balance
        "transit_balance": {
            "товары в пути": "transit_balance"
        }
    }
