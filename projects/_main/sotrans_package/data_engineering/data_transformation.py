import sys
import string
import pandas as pd

sys.path.insert(
    0,
    r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\projects\_main\sotrans_package",
)
from _settings._constant._shop import Shop
from _settings._constant._dealer import Dealer
from _settings._constant._brand import Brand
from data_analysis.data_validation import DataValidation


class DataTransformation:
    """
    Notes:
        Data Transformation (преобразование данных): подстраивание разобщённых
        данных под потребности конечных пользователей. Этот этап включает в себя
        устранение ошибок и дублирование данных, их нормализацию и преобразование в
        нужный формат.

    Attributes:
        pass

    Methods:
        pass
    """

    class Value:
        """
        Notes:
            Класс обработки ошибок в записях.
        """

        class Contradiction:
            """
            Notes:
                Методы очистки записей, которые ссылаются на один объект и противоречат друг-другу.

            Attributes:
                pass

            Methods:
                pass
            """

            pass

        class Duplicate:
            """
            Notes:
                Методы очистки записей, задублированных по каким либо причинам.

            Attributes:
                pass

            Methods:
                pass
            """

            pass

        class Unique:
            """
            Notes:
                Методы очистки записей, которые должны быть уникальными: паспорт, страховой полис...

            Attributes:
                pass

            Methods:
                pass
            """

            pass

    class Feature:
        """
        Notes:
            Класс обработки ошибок в признаках.
        """

        class Abnormal:
            """
            Notes:
                Методы обработки признаков с аномальными значениями.

            Attributes:
                None

            Methods:
                None
            """

            pass

        class ErrorsTypos:
            """
            Notes:
                Методы обработки признаков с ошибками и опечатками.

            Attributes:
                pass

            Methods:
                pass
            """

            pass

        class Invalid:
            """
            Notes:
                Методы обработки признаков с недопустимыми значениями.

            Attributes:
                pass

            Methods:
                pass
            """

            @staticmethod
            def clear_dealer_name(series: pd.Series) -> str:
                """
                Notes:
                    Метод переименовывает некорректные наименования поставщиков.

                Raises:
                    TypeError: f"Тип переданных данных не соответствует ожидаемому: {expected_type}")

                Returns:
                    str: Наименование поставщика, очищенное от "недопустимых" подстрок.
                """

                # ? DATA VALIDATION
                (
                    DataValidation
                    .value_type(
                        value=series,
                        expected_type=pd.Series
                    )
                )

                # ! MAIN ALGORITHM
                series = (
                    series
                    .replace(
                        to_replace=Dealer.get_contragent_name_to_replace()
                    )
                )

                # Возврат значения;
                return series

            @staticmethod
            def clear_dealer_substring(string_to_replace: str) -> str:
                """
                Notes:
                    Метод принимает на вход строку с наименованием поставщика и очищает её от "недопустимых" подстрок.

                Args:
                    string_to_replace (str): Наименование поставщика.

                Raises:
                    TypeError: f"Тип переданных данных не соответствует ожидаемому: {expected_type}")

                Returns:
                    str: Наименование поставщика, очищенное от "недопустимых" подстрок.
                """

                # ? DATA VALIDATION
                (
                    DataValidation
                    .value_type(
                        value=string_to_replace,
                        expected_type=str
                    )
                )

                # ! MAIN ALGORITHM
                for false_string, true_string in (
                    Dealer
                    .get_contragent_substring_to_replace()
                    .items()
                ):
                    string_to_replace: str = (
                        string_to_replace
                        .replace(false_string, true_string)
                        .strip()
                    )

                # Возврат значения;
                return string_to_replace

            @staticmethod
            def clear_brand(string_to_replace: str) -> str:
                """
                Notes:
                    Метод принимает на вход строку с наименованием бренда и очищает её от
                    "недопустимых" подстрок.

                Args:
                    string_to_replace (str): Наименование бренда.

                Raises:
                    TypeError: f"Тип переданных данных не соответствует ожидаемому: {expected_type}")

                Returns:
                    str: Наименование бренда, очищенное от "недопустимых" подстрок.
                """

                # ? DATA VALIDATION
                (
                    DataValidation
                    .value_type(
                        value=string_to_replace,
                        expected_type=str
                    )
                )

                # ! MAIN ALGORITHM
                for (
                    false_string,
                    true_string,
                ) in Brand.get_brand_name_to_replace().items():
                    string_to_replace: str = (
                        string_to_replace
                        .replace(false_string, true_string)
                        .strip()
                    )

                # Возврат значения;
                return string_to_replace

            @staticmethod
            def clear_shop(shop_series: pd.Series) -> pd.Series:
                """
                Notes:
                    Метод принимает на вход pd.Series с наименованием магазинов и приводит к
                    требуемому виду путём переименования и "объединения" магазинов/складов.

                Args:
                    shop_series (pd.Series): Массив с наименованиями складов/магазинов.

                Raises:
                    TypeError: f"Тип переданных данных не соответствует ожидаемому: {expected_type}")

                Returns:
                    (pd.Series): Массив с переименованными складами/магазинами.
                """

                # DATA VALIDATION
                DataValidation.value_type(value=shop_series, expected_type=pd.Series)

                # MAIN ALGORITHM
                shop_series = shop_series.replace(
                    to_replace=Shop.get_shop_substring_to_replace()
                )

                return shop_series

            @staticmethod
            def clear_product_catalog_number(
                series: pd.Series, clearing_method: str
            ) -> pd.Series:
                """
                Notes:
                    Метод принимает на вход pandas серию и тип очистки: "origin" | "sotrans".
                    Очищает значения от символов пунктуации и алиаса бренда (в методе sotrans).

                    В зависимости от выбранного типа, отличается логика очистки:
                        - origin - очистка от символов пунктуации.
                        - sotrans - очистка от символов пунктуации с предварительным удалением алиаса бренда "_brand".

                Args:
                    series (pd.Series): Pandas серия, в которой требуется очистить значения от
                    символов пунктуации.
                    clearing_method (str): Метод очистки: "origin" | "sotrans".

                Raises:
                    TypeError: f"Тип переданных данных не соответствует ожидаемому: {expected_type}")

                Returns:
                    (pd.DataFrame): Датафрейм со столбцом "product_article_number" (очищенным номером по каталогу).
                """

                # DATA VALIDATION
                for value, expected_type in zip(
                    (series, clearing_method), (pd.Series, str)
                ):
                    DataValidation.value_type(value=value, expected_type=expected_type)

                # FUNCTIONS
                def origin_method(product_article_number: str) -> str:
                    """
                    Notes:
                        Метод принимает на вход номер по каталогу/артикул и производит приведение
                        к верхнему регистру и очистку от знаков пунктуации.

                    Args:
                        product_catalog_number (str): Номер по каталогу/артикул.

                    Raises:
                        TypeError: f"Тип переданныых данных не соответствует ожидаемому: {expected_type}")

                    Returns:
                        str: Номер, очищенный от знаков пунктуации.
                    """

                    # DATA VALIDATION
                    DataValidation.value_type(
                        value=product_article_number, expected_type=str
                    )

                    # MAIN ALGORITHM
                    # Очистить от символов пунктуации;
                    for symbol in f"{string.punctuation} ":
                        product_article_number: str = product_article_number.replace(
                            symbol, ""
                        ).strip()

                    # Возврат результата;
                    return product_article_number

                def sotrans_method(product_catalog_number: str) -> str:
                    """
                    Notes:
                        Метод принимает на вход номер по каталогу. Производит приведение
                        к верхнему регистру, очистку от знаков пунктуации и алиаса бренда.

                    Raises:
                        TypeError: f"Тип переданныых данных не соответствует ожидаемому: {expected_type}")

                    Args:
                        product_catalog_number (str): Номер по каталогу.

                    Returns:
                        str: Номер, очищенный от знаков пунктуации.
                    """

                    # DATA VALIDATION
                    DataValidation.value_type(
                        value=product_catalog_number, expected_type=str
                    )

                    # MAIN ALGORITHM
                    # Очистить от алиаса бренда и символов пунктуации;
                    for symbol in f"{string.punctuation} ":
                        product_catalog_number: str = (
                            product_catalog_number.split("_", -1)[0]
                            .replace(symbol, "")
                            .strip()
                        )

                    # Возврат результата;
                    return product_catalog_number

                # MAIN ALGORITHM
                match clearing_method:
                    case "origin":
                        series = pd.Series(
                            data=[
                                origin_method(product_article_number=value)
                                for value in series
                            ]
                        )

                    case "sotrans":
                        series = pd.Series(
                            data=[
                                sotrans_method(product_catalog_number=value)
                                for value in series
                            ]
                        )

                # Возврат результата;
                return series

        class Missed:
            """
            Notes:
                Методы обработки признаков с отсутствующими значениями.

            Attributes:
                pass

            Methods:
                pass
            """

            pass

        class Polysemy:
            """
            Notes:
                Методы обработки признаков с многозначными значениями.

            Examples:
                pass

            Attributes:
                None

            Methods:
                pass
            """

            pass
