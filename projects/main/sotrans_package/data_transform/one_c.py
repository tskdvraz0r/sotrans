# ! IMPORTS
# * Standard packages
import datetime as dt
import string

# * Extended packages
import pandas as pd

# * Sotrans packages
from projects.main.sotrans_package import constant
from projects.main.sotrans_package import data_validation


class BatchMovement:
    """
    Notes:
        -

    Attributes:
        -

    Methods:
        feature_doc_movement_data()
            Функция принимает на вход датафрейм и добавляет в столбец "Документ движения" данные,
            если тип документа равен: start_balance, end_balance или transit_balance.
            Шаблон: f"(Начальный остаток | Конечный остаток | Товары в пути) №00000000 от 01.{file_month}.{file_year}"

        feature_document_data()
            Функция принимает на вход датафрейм и рассчитывает показатели по документу движения/партии:
            тип, номер, дата, время, дата начала месяца.

        clear_product_catalog_number()
            Функция принимает на вход датафрейм и тип очистки: "origin" | "sotrans".
            Очищает записи в столбце "product_catalog_number" от лишних символов (пунктуационных) и сохраняет
            данные в новый столбец "product_article_number".

        restructure_columns()
            Функция принимает на вход датафрейм и переставляет столбцы для более комфортного восприятия.

        change_dtypes()
            Функция принимает на вход датафрейм и корректирует типы данных в столбцах, для минимизации затрат памяти.
    """

    @staticmethod
    def feature_doc_movement_data(
        dataframe: pd.DataFrame,
        doc_type: str,
        file_month: int,
        file_year: int
    ) -> pd.DataFrame:
        """
        Notes:
            Функция принимает на вход датафрейм и добавляет в столбец "Документ движения" данные,
            если тип документа равен: start_balance, end_balance или transit_balance.
            Шаблон: f"(Начальный остаток | Конечный остаток | Товары в пути) №00000000 от 01.{file_month}.{file_year}"

        Args:
            dataframe (pd.DataFrame): Датафрейм, в который требуется вставить данные.
            doc_type (str): Тип документа (start_balance, end_balance, transit_balance).
            file_month (int): Месяц документа.
            file_year (int): Год документа.

        Raises:
            TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
            ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')

        Returns:
            (pd.DataFrame): Датафрейм с новыми данными по "документу движения".
        """

        # ! DATA VALIDATION
        # Проверка типов данных.
        for value, var_type in zip(
                (dataframe, doc_type, file_month, file_year),
                (pd.DataFrame, str, int, int)
        ):
            data_validation.check.value_type(
                value = value,
                expected_type = var_type
            )

        # Проверка значений.
        data_validation.check.value_in_range(
            value = doc_type,
            in_range = (
                "start_balance",
                "income_balance",
                "expend_balance",
                "end_balance",
                "transit_balance"
            )
        )

        # ! MAIN ALGORITHM
        # Добавить данные по документу движения.
        if doc_type in {"start_balance", "end_balance", "transit_balance"}:
            data_pattern: str = f"№00000000 от 01.{file_month}.{file_year} 00:00:00"

            match doc_type:
                case "start_balance":
                    dataframe["document_movement"] = f"начальный остаток {data_pattern}"

                case "end_balance":
                    dataframe["document_movement"] = f"конечный остаток {data_pattern}"

                case "transit_balance":
                    dataframe["document_movement"] = f"товары в пути {data_pattern}"

        return dataframe

    @staticmethod
    def feature_document_data(dataframe: pd.DataFrame, src_file: str) -> pd.DataFrame:
        """
        Notes:
            Функция принимает на вход датафрейм и рассчитывает показатели по документу движения/партии:
            тип, номер, дата, время, дата начала месяца.

        Args:
            dataframe (pd.DataFrame): Датафрейм, в котором требуется вывести новые данные.
            src_file (str): Формат исходного файла. От него зависит, какие шаги будут применены.

        Raises:
            TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

        Returns:
            (pd.DataFrame): Датафрейм с новыми данными по документу движения/партии.
        """

        # ! DATA VALIDATION
        # Проверить тип данных в аргументе "dataframe".
        data_validation.check.value_type(
            value = dataframe,
            expected_type = pd.DataFrame
        )

        # ! MAIN ALGORITHM
        dataframe["document_movement"] = (
            dataframe["document_movement"]
            .replace(to_replace = {'': None})
            .ffill()
        )

        dataframe["document_batch"] = (
            dataframe["document_batch"]
            .replace(to_replace = {'': None})
            .ffill()
        )

        match src_file:
            case "xlsx":
                for column_name in ("document_movement", "document_batch"):
                    # Вывести тип документа.
                    dataframe[f"{column_name}_type"] = [
                        " ".join(elements.split()[:-4])
                        for elements in dataframe[column_name]
                    ]

                    # Вывести номер документа.
                    dataframe[f"{column_name}_number"] = [
                        elements.split()[-4]
                        for elements in dataframe[column_name]
                    ]

                    # Вывести дату документа.
                    dataframe[f"{column_name}_date"] = [
                        elements.split()[-2]
                        for elements in dataframe[column_name]
                    ]

                    # Вывести время документа.
                    dataframe[f"{column_name}_time"] = [
                        elements.split()[-1]
                        for elements in dataframe[column_name]
                    ]

                    # Вывести дату документа с начала месяца.
                    dataframe[f"{column_name}_start_of_month"] = [
                        f"01{elements.split()[-2][2:]}"
                        for elements in dataframe[column_name]
                    ]

            case "json":
                for column_name in (
                    "document_movement",
                    "document_batch"
                ):
                    # Удалить "t" из datetime.
                    dataframe[column_name] = [
                        (
                            value
                            .replace("t", " ")
                            .replace("-", ".")
                        )
                        for value in dataframe[column_name]
                    ]

                    # Вывести список элементов документа.
                    dataframe[f"{column_name}_split"] = [
                        elements.split()
                        for elements in dataframe[column_name]
                    ]

                    # Вывести тип документа.
                    dataframe[f"{column_name}_type"] = [
                        " ".join(elements[:-4])
                        for elements in dataframe[f"{column_name}_split"]
                    ]

                    # Вывести номер документа.
                    dataframe[f"{column_name}_number"] = [
                        elements[-4]
                        for elements in dataframe[f"{column_name}_split"]
                    ]

                    if column_name == "document_movement":
                        # Вывести дату документа.
                        dataframe[f"{column_name}_date"] = [
                            dt.datetime.strptime(elements[-2], "%Y.%m.%d")
                            for elements in dataframe[f"{column_name}_split"]
                        ]

                    elif column_name == "document_batch":
                        # Вывести дату документа.
                        dataframe[f"{column_name}_date"] = [
                            dt.datetime.strptime(elements[-2], "%d.%m.%Y")
                            for elements in dataframe[f"{column_name}_split"]
                        ]

                    # Вывести время документа.
                    dataframe[f"{column_name}_time"] = [
                        elements[-1]
                        for elements in dataframe[f"{column_name}_split"]
                    ]

                    # Вывести дату документа с начала месяца.
                    dataframe[f"{column_name}_start_of_month"] = [
                        date.replace(day = 1)
                        for date in dataframe[f"{column_name}_date"]
                    ]

                    # Удалить временный столбец
                    dataframe = dataframe.drop(columns = f"{column_name}_split")

        # Вернуть результат.
        return dataframe

    @staticmethod
    def clear_product_catalog_number(
        dataframe: pd.DataFrame,
        clearing_type: str
    ) -> pd.DataFrame:
        """
        Notes:
            Функция принимает на вход датафрейм и тип очистки: "origin" | "sotrans".
            Очищает записи в столбце "product_catalog_number" от лишних символов (пунктуационных) и сохраняет
            данные в новый столбец "product_article_number".

            В зависимости от выбранного типа, отличается логика очистки:
                - origin - очистка от символов пунктуации.
                - sotrans - очистка от символов пунктуации с предварительным удалением алиаса бренда "_brand".

        Args:
            dataframe (pd.DataFrame): Датафрейм, в котором требуется очистить "product_catalog_number".
            clearing_type (str): Тип очистки: "origin" | "sotrans".

        Raises:
             TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
             ValueError(f'Столбец "{column_name}" в датафрейме "{dataframe_name}" не существует.')

        Returns:
            (pd.DataFrame): Датафрейм со столбцом "product_article_number" (очищенным номером по каталогу).
        """

        # ! FUNCTIONS
        def clear_origin_number(product_catalog_number: str) -> str:

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "product_catalog_number".
            data_validation.check.value_type(
                value = product_catalog_number,
                expected_type = str
            )

            # ! MAIN ALGORITHM
            # Сохранить значение в переменную.
            product_article_number: str = product_catalog_number

            # Очистить от символов пунктуации.
            for symbol in string.punctuation + " ":
                product_article_number: str = (
                    product_article_number
                    .replace(symbol, "")
                    .strip()
                )

            # Вернуть результат.
            return product_article_number

        def clear_sotrans_number(product_catalog_number: str) -> str:

            # ! DATA VALIDATION
            # Проверить тип данных в аргументе "product_catalog_number".
            data_validation.check.value_type(
                value = product_catalog_number,
                expected_type = str
            )

            # ! MAIN ALGORITHM
            # Сохранить значение в переменную.
            product_article_number: str = product_catalog_number

            # Очистить от символов пунктуации.
            for symbol in string.punctuation + " ":
                product_article_number: str = (
                    str(product_article_number)
                    .split("_", -1)[0]
                    .replace(symbol, "")
                    .strip()
                )

            # Вернуть результат.
            return product_article_number

        # ! DATA VALIDATION
        # Проверить тип данных в аргументе "dataframe".
        data_validation.check.value_type(
            value = dataframe,
            expected_type = pd.DataFrame
        )

        # Проверить тип данных в аргументе "clearing_type".
        data_validation.check.value_type(
            value = clearing_type,
            expected_type = str
        )

        # Проверить наличие столбца "product_catalog_number" в переданном датафрейме.
        data_validation.check.column_exists(
            dataframe = dataframe,
            column = "product_catalog_number"
        )

        # ! MAIN ALGORITHM
        # Вызов функции в зависимости от типа очистки.
        match clearing_type:
            case "origin":
                dataframe["product_article_number"] = [
                    clear_origin_number(product_catalog_number = value)

                    for value in dataframe["product_catalog_number"]
                ]

                # Вернуть результат.
                return dataframe

            case "sotrans":
                dataframe["product_article_number"] = [
                    clear_sotrans_number(product_catalog_number = value)

                    for value in dataframe["product_catalog_number"]
                ]

                # Вернуть результат.
                return dataframe

    @staticmethod
    def restructure_columns(dataframe: pd.DataFrame, doc_type: str) -> pd.DataFrame:
        """
        Notes:
            Функция принимает на вход датафрейм и переставляет столбцы для более комфортного восприятия.

        Args:
            dataframe (pd.DataFrame): Датафрейм, в котором  требуется переставить столбцы.
            doc_type (str): Тип документа, для получения алиаса столбца.

        Raises:
            TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
            ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')

        Returns:
            (pd.DataFrame): Датафрейм с переставленными столбцами.
        """

        # ! DATA VALIDATION
        # Проверить типы данных в аргументах (dataframe, doc_type)
        for value, value_type in zip(
            (dataframe, doc_type),
            (pd.DataFrame, str)
        ):
            data_validation.check.value_type(
                value = value,
                expected_type = value_type
            )

        # Проверить корректность значения в переменной "doc_type".
        data_validation.check.value_in_range(
            value = doc_type,
            in_range = constant.one_c.get_doc_names().keys()
        )

        # ! MAIN ALGORITHM
        # Алиас столбца.
        column_alias: dict[str: str] = constant.one_c.get_doc_alias()[doc_type]

        # Переставить столбцы местами.
        dataframe: pd.DataFrame = (
            dataframe[
                [
                    # Документ движения
                    "document_movement_start_of_month",
                    "document_movement_date",
                    "document_movement_time",
                    "document_movement_type",
                    "document_movement_number",

                    # Документ партии
                    "document_batch_start_of_month",
                    "document_batch_date",
                    "document_batch_time",
                    "document_batch_type",
                    "document_batch_number",

                    # Описательные данные
                    "shop_name",
                    "dealer_name",
                    "brand_name",
                    "product_name",
                    "product_id_1c",
                    "product_catalog_number",
                    "product_article_number",

                    # Количественные данные
                    f"{column_alias}_count",
                    f"{column_alias}_sum_rub",
                    f"{column_alias}_sum_eur"
                ]
            ]
            .sort_values(
                by = [
                    "document_movement_start_of_month",
                    "document_movement_date",
                    "document_movement_time",
                    "document_movement_type",
                    "document_movement_number",
                    "shop_name",
                    "dealer_name",
                    "brand_name",
                    "product_article_number"
                ]
            )
            .reset_index(drop = True)
        )

        return dataframe

    @staticmethod
    def change_dtypes(dataframe: pd.DataFrame, doc_type: str) -> pd.DataFrame:
        """
            Функция принимает на вход датафрейм и корректирует типы данных в столбцах, для минимизации затрат памяти.

        Args:
            dataframe (pd.DataFrame): Датафрейм, в котором требуется изменить типы данных.
            doc_type (str): Тип документа, для получения алиаса столбца.

        Returns:
            (pd.DataFrame): Датафрейм с изменёнными типами данных.
        """

        # ! DATA VALIDATION
        # Проверить типы данных в аргументах (dataframe, doc_type)
        for value, value_type in zip(
                (dataframe, doc_type),
                (pd.DataFrame, str)
        ):
            data_validation.check.value_type(
                value = value,
                expected_type = value_type
            )

        # Проверить корректность значения в переменной "doc_type".
        data_validation.check.value_in_range(
            value = doc_type,
            in_range = constant.one_c.get_doc_names().keys()
        )

        # ! MAIN ALGORITHM
        column_alias: str = constant.one_c.get_doc_alias()[doc_type]

        for column_name in ("document_movement", "document_batch"):
            dataframe[f"{column_name}_type"] = dataframe[f"{column_name}_type"].astype("category")
            dataframe[f"{column_name}_number"] = dataframe[f"{column_name}_number"].astype("category")

        for column_name, column_type in zip(
            (
                "shop_name",
                "dealer_name",
                "brand_name",
                "product_name",
                "product_id_1c",
                "product_catalog_number",
                "product_article_number",
                f"{column_alias}_count",
                f"{column_alias}_sum_rub",
                f"{column_alias}_sum_eur"
            ),
            (
                "category",
                "category",
                "category",
                "str",
                "str",
                "str",
                "str",
                "int32",
                "float32",
                "float32"
            )
        ):
            dataframe[column_name] = dataframe[column_name].astype(column_type)

        return dataframe
