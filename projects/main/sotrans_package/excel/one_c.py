# ! IMPORTS
# * Standard packages
import datetime as dt

# * Extended packages
import pandas as pd

# * Sotrans packages
from projects.main.sotrans_package.data_clearing.one_c import Feature
from projects.main.sotrans_package import data_transform
from projects.main.sotrans_package import data_validation
from projects.main.sotrans_package import data_verification


# ! CLASSES
class BatchMovement:

    # ! METHODS
    @staticmethod
    def clear_data_file(
        dataframe: pd.DataFrame,
        doc_type: str,
        file_type: str,
        file_month: int,
        file_year: int
    ) -> pd.DataFrame:
        """
        Notes:


        Args:
            dataframe (pd.DataFrame):
            doc_type (str):
            file_type (str):
            file_month (int):
            file_year (int):

        Returns:

        """

        # ! DATA VALIDATION
        # Проверить корректность типов данных.
        for value, var_type in zip(
            (dataframe, doc_type, file_type, file_month, file_year),
            (pd.DataFrame, str, str, int, int)
        ):
            data_validation.check.value_type(
                value = value,
                expected_type = var_type
            )

        # Проверить корректность переданных данных.
        if file_type == "xlsx":
            for value, include_in in zip(
                (file_month, file_year),
                (list(range(1, 13)), list(range(2014, 2024)))
            ):
                data_validation.check.value_in_range(
                    value = value,
                    in_range = include_in
                )

        elif file_type == "json":
            for value, include_in in zip(
                    (file_month, file_year),
                    (list(range(1, 13)), list(range(2024, file_year + 1)))
            ):
                data_validation.check.value_in_range(
                    value = value,
                    in_range = include_in
                )

        # ! MAIN ALGORITHM
        # * DATA CLEARING
        # Удалить невостребованные столбцы.
        dataframe: pd.DataFrame = (
            Feature
            .Invalid
            .del_unclaimed_columns(
                dataframe = dataframe,
                doc_type = doc_type,
                file_type = file_type
            )
        )
        print(f"[{dt.datetime.now()}] Удалены невостребованные столбцы.")

        # Переименовать столбцы.
        dataframe: pd.DataFrame = (
            Feature
            .ErrorsTypos
            .rename_columns(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )
        print("Переименованы столбцы.")

        # Удалить строку "Итог".
        dataframe: pd.DataFrame = (
            Feature
            .Invalid
            .del_total_row(dataframe = dataframe)
        )
        print("Удалена строка 'Итог'.")

        if file_type == "xlsx":
            # Заполнить пропуски в столбцах "dealer_name" и "brand_name".
            dataframe: pd.DataFrame = (
                Feature
                .Missed
                .fill_nan_in_dealer_brand(dataframe = dataframe)
            )
            print("Заполнены пропуски 'dealer_name' и 'brand_name'.")

            # Заполнить пропуски "вниз" в столбцах ("shop_name", "document_batch", "document_movement", "dealer_name").
            dataframe: pd.DataFrame = (
                Feature
                .Missed
                .fill_nan_down(dataframe = dataframe)
            )
            print("Заполнены пропуски 'вниз' в качественных столбцах.")

        # Удалить nan строки в столбцах "product_catalog_number" и "count".
        dataframe: pd.DataFrame = (
            Feature
            .Missed
            .del_nan_rows(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )
        print("Удалены nan строки.")

        # Заменить nan значения в столбцах ("count", "sum_rub", "sum_eur")
        dataframe: pd.DataFrame = (
            Feature
            .Missed
            .replace_nan_to_zero(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )
        print("Заменены nan значения в количественных столбцах.")

        # Изменить регистр
        dataframe: pd.DataFrame = (
            Feature
            .ErrorsTypos
            .change_string_register(
                dataframe = dataframe,
                columns = (
                    "shop_name",
                    "document_batch",
                    "document_movement",
                    "dealer_name",
                    "brand_name",
                    "product_name"
                ),
                register = "lower"
            )
        )

        dataframe: pd.DataFrame = (
            Feature
            .ErrorsTypos
            .change_string_register(
                dataframe = dataframe,
                columns = (
                    "product_id_1c",
                    "product_catalog_number"
                ),
                register = "upper"
            )
        )
        print("Изменён регистр строк.")

        # Переименовать склады/магазины
        dataframe: pd.DataFrame = (
            Feature
            .Polysemy
            .replace_shop_names(dataframe = dataframe)
        )
        print("Переименованы магазины.")

        # Проверить наименования магазинов на наличие в константах.
        data_verification.one_c.check_shop_names(dataframe = dataframe)

        # Удалить магазины, которые не в списке
        dataframe: pd.DataFrame = (
            Feature
            .Invalid
            .del_not_true_shop_names(dataframe = dataframe)
        )
        print("Удалены магазины не из списка")

        # Удалить некорректные подстроки из наименований контрагента
        dataframe: pd.DataFrame = (
            Feature
            .ErrorsTypos
            .del_false_substring_in_contragent(dataframe = dataframe)
        )
        print("Удалены некорректные подстроки из 'dealer_name'.")

        # Переименовать контрагентов
        dataframe: pd.DataFrame = (
            Feature
            .Polysemy
            .replace_contragent_names(dataframe = dataframe)
        )
        print("Переименованы дубли в 'dealer_name'.")

        # * FEATURE ENGINEERING
        # Добавить данные в столбец "Документ движения"
        if file_type == "xlsx":
            dataframe: pd.DataFrame = (
                data_transform
                .one_c
                .BatchMovement
                .feature_doc_movement_data(
                    dataframe = dataframe,
                    doc_type = doc_type,
                    file_month = file_month,
                    file_year = file_year
                )
            )
            print("Добавлены данные в столбец 'Документ движения'.")

        # Добавить данные по типу, номеру, дате и времени документа движения/партии.
        dataframe: pd.DataFrame = (
            data_transform
            .one_c
            .BatchMovement
            .feature_document_data(
                dataframe = dataframe,
                file_type = file_type
            )
        )
        print("Добавлены данные по типу, номеру, дате документа движения/партии.")

        # Добавить столбец с очищенным "номером по каталогу".
        dataframe: pd.DataFrame = (
            data_transform
            .one_c
            .BatchMovement
            .clear_product_catalog_number(
                dataframe = dataframe,
                clearing_type = "sotrans"
            )
        )
        print("Добавлен столбец с очищенным номером по каталогу.")

        # Переставить местами столбцы.
        dataframe: pd.DataFrame = (
            data_transform
            .one_c
            .BatchMovement
            .restructure_columns(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )
        print("Столбцы переставлены местами.")

        # Изменить типы данных в датафрейме.
        dataframe: pd.DataFrame = (
            data_transform
            .one_c
            .BatchMovement
            .change_dtypes(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )
        print("Изменены типы данных.")

        return dataframe
