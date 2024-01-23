# ! IMPORTS
# * Standard packages
import string

# * Extended packages
import pandas as pd

# * Sotrans packages
from projects.main.sotrans_package import data_validation
from projects.main.sotrans_package import constant


class Value:
    """
    Notes:
        Класс обработки ошибок в записях.
    """

    class Duplicate:
        """
        Notes:
            Методы очистки записей, задублированных по каким либо причинам.

        Attributes:
            …

        Methods:
            …
        """

        pass

    class Unique:
        """
        Notes:
            Методы очистки записей, которые должны быть уникальными: паспорт, страховой полис и т.д.

        Attributes:
            …

        Methods:
            …
        """

        pass

    class Contradiction:
        """
        Notes:
            Методы очистки записей, которые ссылаются на один объект и противоречат друг-другу.

        Attributes:
            …

        Methods:
            …
        """

        pass


class Feature:
    """
    Notes:
        Класс обработки ошибок в признаках.
    """

    class Missed:
        """
        Notes:
            Методы обработки признаков с отсутствующими значениями.

        Attributes:
            None

        Methods:
            del_nan_rows()
                Функция принимает на вход датафрейм и удаляет nan строки из столбцов (product_catalog_name,
                {alias}_count).

            fill_nan_down()
                Функция принимает на вход датафрейм и заполняет пропуски "вниз" в столбцах ("shop_name",
                "document_batch", "document_movement", "dealer_name").

            fill_nan_in_dealer_brand()
                Функция принимает на вход датафрейм и заполняет "заглушкой" пропуски в столбцах "дилер" и "бренд".

            replace_nan_to_zero()
                Функция принимает на вход датафрейм и тип документа.
                В столбцах "{alias}_sum_rub" и "{alias}_sum_eur" заменяет nan на 0.
        """

        @staticmethod
        def del_nan_rows(
            dataframe: pd.DataFrame,
            doc_type: str
        ) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и удаляет nan строки из столбцов (product_catalog_name,
                {alias}_count).

            Args:
                dataframe (pd.DataFrame): Датафрейм, в котором требуется удалить nan строки.
                doc_type (str): Тип документа, чтобы получить алиас для столбца.

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
                ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')

            Returns:
                (pd.DataFrame): Датафрейм с удалёнными nan строками.
            """

            # ! DATA VALIDATION
            # Проверить типы данных в аргументах (dataframe, doc_type).
            for value, var_type in zip(
                (dataframe, doc_type),
                (pd.DataFrame, str)
            ):
                data_validation.check.value_type(
                    value = value,
                    expected_type = var_type
                )

            # Проверить корректность значения в аргументе "doc_type".
            data_validation.check.value_in_range(
                value = doc_type,
                in_range = constant.one_c.get_doc_alias().keys()
            )

            # ! MAIN ALGORITHM
            # Получить алиас столбца.
            column_alias: dict[str: str] = constant.one_c.get_doc_alias()[doc_type]

            # Удалить nan строки.
            for column_name in ("product_catalog_number", f"{column_alias}_count"):
                dataframe: pd.DataFrame = dataframe[~dataframe[column_name].isna()]

            # Вернуть результат.
            return dataframe

        @staticmethod
        def fill_nan_down(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и заполняет пропуски "вниз" в столбцах ("shop_name",
                "document_batch", "document_movement").

            Args:
                dataframe (pd.DataFrame): Датафрейм, в котором требуется заполнить пропуски "вниз".

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Датафрейм с заполненными "вниз" пропусками.
            """

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            # Заполнить пропуски "вниз".
            for column_name in (
                    "shop_name",
                    "document_batch",
                    "document_movement",
            ):
                dataframe[column_name] = (
                    dataframe[column_name]
                    .ffill()
                )

            # Вернуть результат.
            return dataframe

        @staticmethod
        def fill_nan_in_dealer_brand(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и заполняет "заглушкой" пропуски в столбцах "дилер" и "бренд".

            Args:
                dataframe (pd.DataFrame): Датафрейм для заполнения пропусков в столбцах "дилер" и "бренд".

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Датафрейм с отработанными пропусками.
            """

            # ! DATA VALIDATION
            # Проверка тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            # Заполнить пропуски "заглушкой".
            for column_name, nan_name in zip(
                ("dealer_name", "brand_name"),
                ("контрагент не указан", "бренд не указан")
            ):
                dataframe[column_name] = [
                    nan_name
                    if value is None and product_id is None
                    else value

                    for value, product_id in zip(
                        dataframe[column_name],
                        dataframe["product_id_1c"]
                    )
                ]

            # Вернуть результат.
            return dataframe

        @staticmethod
        def replace_nan_to_zero(
            dataframe: pd.DataFrame,
            doc_type: str
        ) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и тип документа.
                В столбцах "{alias}_sum_rub" и "{alias}_sum_eur" заменяет nan на 0.

            Args:
                dataframe (pd.DataFrame): Датафрейм, в котором требуется заменить nan строки на 0.
                doc_type (str): Тип документа.

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
                ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')

            Returns:
                (pd.DataFrame): Датафрейм с заменёнными nan строками на 0.
            """

            # ! DATA VALIDATION
            # Проверить типы данных в атрибутах (dataframe, doc_type).
            for value, var_type in zip(
                (dataframe, doc_type),
                (pd.DataFrame, str)
            ):
                data_validation.check.value_type(
                    value = value,
                    expected_type = var_type
                )

            # Проверить корректность значения.
            data_validation.check.value_in_range(
                value = doc_type,
                in_range = constant.one_c.get_doc_names().keys()
            )

            # ! MAIN ALGORITHM
            # Получить алиас столбца.
            column_alias: str = constant.one_c.get_doc_alias()[doc_type]

            # Заменить nan значения.
            for column_name in (f"{column_alias}_count", f"{column_alias}_sum_rub", f"{column_alias}_sum_eur"):
                dataframe[column_name] = dataframe[column_name].fillna(0)

            # Вернуть результат.
            return dataframe

    class Invalid:
        """
        Notes:
            Методы обработки признаков с недопустимыми значениями.

        Attributes:
            None

        Methods:
            del_not_true_shop_names()
                Функция принимает на вход датафрейм и удаляет все записи со складами/магазинами, не входящими
                в "корректный" список.

            del_total_row()
                Функция принимает на вход датафрейм и удаляет из него строку "Итог".

            del_unclaimed_columns()
                Функция принимает на вход датафрейм и удаляет из него невостребованные столбцы.
        """

        @staticmethod
        def del_not_true_shop_names(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и удаляет все записи со складами/магазинами, не входящими
                в "корректный" список.

            Args:
                dataframe (pd.DataFrame): Датафрейм, который требуется очистить.

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Очищенный датафрейм.
            """

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            # Удалить склады/магазины, которые не входят в "корректный" список.
            dataframe: pd.DataFrame = dataframe[dataframe["shop_name"].isin(constant.shop.TRUE_SHOP_NAMES)]

            # Вернуть результат.
            return dataframe

        @staticmethod
        def del_total_row(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и удаляет из него строку "Итог".

            Args:
                dataframe (pd.DataFrame): Датафрейм, из которого требуется удалить строку "Итог".

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Датафрейм с удалённой строкой "Итог".
            """

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            # Удалить строку "Итог".
            dataframe: pd.DataFrame = dataframe[dataframe["shop_name"] != "Итог"]

            # Вернуть результат.
            return dataframe

        @staticmethod
        def del_unclaimed_columns(dataframe: pd.DataFrame, doc_type: str, file_type: str) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и удаляет из него невостребованные столбцы.

            Args:
                dataframe (pd.DataFrame): Датафрейм, из которого требуется удалить невостребованные столбцы.
                doc_type (str): Тип документа.
                file_type (str): Формат исходного файла (xlsx или json).

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
                ValueError(f'Столбец "{column_name}" в датафрейме "{dataframe_name}" не существует.')

            Returns:
                (pd.DataFrame): Датафрейм с удалёнными столбцами.
            """

            # ! VARIABLES
            column_alias: str = constant.one_c.get_doc_alias()[doc_type]

            xlsx_columns: list = [
                "Организация (покупатель) партии",
                "Цена",
                "Сумма НДС (в рег. валюте)",
                "Сумма без НДС (в рег. валюте)"
            ]

            json_columns: list = [
                f"{column_alias}_price",
                f"{column_alias}_sum_nds"
            ]

            # ! DATA VALIDATION
            # Проверить типы данных.
            for value, value_type in zip(
                (dataframe, doc_type, file_type),
                (pd.DataFrame, str, str)
            ):
                data_validation.check.value_type(
                    value = value,
                    expected_type = value_type
                )

            # Проверить наличие столбцов в таблице.
            if file_type == "xlsx":
                for column_name in xlsx_columns:
                    data_validation.check.column_exists(
                        dataframe = dataframe,
                        column = column_name
                    )

            # ! MAIN ALGORITHM
            # Удалить невостребованные столбцы.
            match file_type:
                case "xlsx":
                    dataframe: pd.DataFrame = dataframe.drop(
                        labels = xlsx_columns,
                        axis = 1
                    )
                case "json":
                    for column_name in json_columns:
                        if column_name in dataframe.columns:
                            dataframe: pd.DataFrame = dataframe.drop(
                                labels = column_name,
                                axis = 1
                            )

            # Вернуть результат.
            return dataframe

    class ErrorsTypos:
        """
        Notes:
            Методы обработки признаков с ошибками и опечатками.

        Attributes:
            None

        Methods:
            change_string_register()
                Функция принимает на вход датафрейм, кортеж столбцов и регистр, который применяет ко всем записям
                в указанных столбцах.

            clear_product_catalog_number()
                Функция принимает на вход датафрейм и тип очистки: "origin" | "sotrans".
                Очищает записи в столбце "product_catalog_number" от лишних символов (пунктуационных) и сохраняет
                данные в новый столбец "product_article_number".

            del_false_substring_in_contragent()
                Функция принимает на вход датафрейм и очищает наименования "контрагента" от некорректных подстрок.

            rename_columns()
                Функция принимает на вход датафрейм и тип документа, и переименовывает рус. наименования на англ.
        """

        @staticmethod
        def change_string_register(
            dataframe: pd.DataFrame,
            columns: tuple,
            register: str
        ) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм, кортеж столбцов и регистр, который применяет ко всем записям
                в указанных столбцах.

            Args:
                dataframe (pd.DataFrame): Датафрейм в котором требуется изменить регистр.
                columns (tuple): Кортеж наименований столбцов, в которых требуется изменить регистр.
                register (str): Регистр (lower, upper)

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
                ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')
                ValueError(f'Столбец "{column_name}" в датафрейме "{dataframe_name}" не существует.')

            Returns:
                (pd.DataFrame): Датафрейм с изменённым регистром в переданных столбцах.
            """

            # ! DATA VALIDATION
            # Проверить типы данных в аргументах (dataframe, columns, register).
            for value, value_type in zip(
                    (dataframe, columns, register),
                    (pd.DataFrame, tuple, str)
            ):
                data_validation.check.value_type(
                    value = value,
                    expected_type = value_type
                )

            # Проверить корректность значения.
            data_validation.check.value_in_range(
                value = register,
                in_range = ("lower", "upper")
            )

            # Проверить существование данных.
            for column_name in columns:
                data_validation.check.column_exists(
                    dataframe = dataframe,
                    column = column_name
                )

            # ! MAIN ALGORITHM
            # Изменить регистр строк.
            for column_name in columns:
                dataframe[column_name] = [
                    str(row).lower().strip()
                    if register == "lower"
                    else str(row).upper().strip()

                    for row in dataframe[column_name]
                ]

            # Вернуть результат.
            return dataframe

        @staticmethod
        def del_false_substring_in_contragent(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и очищает наименования "контрагента" от некорректных подстрок.

            Args:
                dataframe (pd.DataFrame): Датафрейм, в котором требуется очистить записи "контрагента" от
                некорректных подстрок.

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Датафрейм с очищенными записями "контрагента" от некорректных подстрок.
            """

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            def clear_contragent_name(contragent_name: str) -> str:
                """
                Notes:
                    Функция принимает на вход строку и очищает её от некорректных подстрок.

                Args:
                    contragent_name (str):

                Raises:
                    TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

                Returns:
                    (str): Строка, очищенная от некорректных подстрок.
                """

                # ! DATA VALIDATION
                # Проверить тип данных в аргументе "contragent_name".
                data_validation.check.value_type(
                    value = contragent_name,
                    expected_type = str
                )

                # MAIN ALGORITHM
                # Удалить символ из наименования контрагента.
                for false_substring in constant.contragent.FALSE_CONTRAGENT_SUBSTRINGS:
                    contragent_name = contragent_name.replace(false_substring, "")

                # Вернуть результат.
                return contragent_name

            # Применить функцию ко всем записям.
            dataframe.loc[:, "dealer_name"] = [
                clear_contragent_name(contragent_name = dealer)

                for dealer in dataframe["dealer_name"]
            ]

            # Вернуть результат.
            return dataframe

        @staticmethod
        def rename_columns(
            dataframe: pd.DataFrame,
            doc_type: str
        ) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и тип документа, и переименовывает рус. наименования на англ.

            Args:
                dataframe (pd.DataFrame): Датафрейм.
                doc_type (str): Тип документа.

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
                ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')

            Returns:
                (pd.DataFrame): Датафрейм с переименованными столбцами.
            """

            # ! DATA VALIDATION
            # Проверить, корректность типов данных.
            for value, var_type in zip(
                (dataframe, doc_type),
                (pd.DataFrame, str)
            ):
                data_validation.check.value_type(
                    value = value,
                    expected_type = var_type
                )

            # Проверить корректность значения.
            data_validation.check.value_in_range(
                value = doc_type,
                in_range = constant.one_c.get_doc_alias().keys()
            )

            # ! MAIN ALGORITHM
            # Получить алиас столбца.
            column_alias: dict[str: str] = constant.one_c.get_doc_alias()[doc_type]

            # Словарь {"рус.": "англ."} наименований столбцов.
            column_names: dict[str: str] = {
                "Склад компании": "shop_name",
                "Документ движения": "document_movement",
                "Документ партии": "document_batch",
                "Контрагент партии": "dealer_name",
                "Код номенклатуры": "product_id_1c",
                "Наименование номенклатуры": "product_name",
                "Производитель номенклатуры": "brand_name",
                "№ по каталогу номенклатуры": "product_catalog_number",
                "Количество (в ед. хранения)": f"{column_alias}_count",
                "Сумма (в рег. валюте)": f"{column_alias}_sum_rub",
                "Сумма (в упр. валюте)": f"{column_alias}_sum_eur"
            }

            # Переименование столбцов.
            dataframe: pd.DataFrame = dataframe.rename(columns = column_names)

            # Вернуть результат
            return dataframe

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

    class Polysemy:
        """
        Notes:
            Методы обработки признаков с многозначными значениями.

        Examples:
            "meklas group (connect)": "mekpa group (connect)",
            "mekpa otomotiv ith. ihr. san. tic. ltd.": "mekpa group (connect)",

        Attributes:
            None

        Methods:
            replace_shop_names()
                Функция принимает на вход датафрейм и переименовывает магазины в столбце "shop_names".
                По разным причинам наименования складов / магазинов могли измениться: закрытие, ошибочно заведённое.

            replace_contragent_names()
                Функция принимает на вход датафрейм, в котором требуется привести подобные названия "контрагентов" к
                одному.
        """

        @staticmethod
        def replace_shop_names(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм и переименовывает магазины в столбце "shop_names".
                По разным причинам наименования складов / магазинов могли измениться: закрытие, ошибочно заведённое.

            Args:
                dataframe (pd.DataFrame): Датафрейм в котором требуется переименовать магазины.

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Датафрейм с переименованными магазинами.
            """

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            # Переименовать магазины.
            dataframe["shop_name"] = dataframe["shop_name"].replace(to_replace = constant.shop.CORRECT_SHOP_NAMES)

            # Вернуть результат.
            return dataframe

        @staticmethod
        def replace_contragent_names(dataframe: pd.DataFrame) -> pd.DataFrame:
            """
            Notes:
                Функция принимает на вход датафрейм, в котором требуется привести подобные названия "контрагентов" к
                одному.

            Args:
                dataframe (pd.DataFrame): Датафрейм, в котором требуется переименовать "контрагентов".

            Raises:
                TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

            Returns:
                (pd.DataFrame): Датафрейм с переименованными контрагентами.
            """

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "dataframe".
            data_validation.check.value_type(
                value = dataframe,
                expected_type = pd.DataFrame
            )

            # ! MAIN ALGORITHM
            # Привести подобные наименования к одному.
            dataframe.loc[:, "dealer_name"] = (
                dataframe["dealer_name"]
                .replace(
                    to_replace = constant.contragent.CORRECT_CONTRAGENT_NAMES
                )
            )

            # Вернуть результат.
            return dataframe
