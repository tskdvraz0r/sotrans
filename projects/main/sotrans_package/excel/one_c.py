# ! IMPORTS
# * Standard packages

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
        file_month: int,
        file_year: int
    ) -> pd.DataFrame:

        # ! DATA VALIDATION
        # Проверить корректность типов данных.
        for value, var_type in zip(
            (dataframe, doc_type, file_month, file_year),
            (pd.DataFrame, str, int, int)
        ):
            data_validation.check.value_type(
                value = value,
                expected_type = var_type
            )

        # Проверить корректность переданных данных.
        for value, include_in in zip(
            (file_month, file_year),
            (list(range(1, 13)), list(range(2014, 2024)))
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
            .del_unclaimed_columns(dataframe = dataframe)
        )

        # Переименовать столбцы.
        dataframe: pd.DataFrame = (
            Feature
            .ErrorsTypos
            .rename_columns(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )

        # Удалить строку "Итог".
        dataframe: pd.DataFrame = (
            Feature
            .Invalid
            .del_total_row(dataframe = dataframe)
        )

        # Заполнить пропуски в столбцах "dealer_name" и "brand_name".
        dataframe: pd.DataFrame = (
            Feature
            .Missed
            .fill_nan_in_dealer_brand(dataframe = dataframe)
        )

        # Заполнить пропуски "вниз" в столбцах ("shop_name", "document_batch", "document_movement", "dealer_name").
        dataframe: pd.DataFrame = (
            Feature
            .Missed
            .fill_nan_down(dataframe = dataframe)
        )

        # Удалить nan строки в столбцах "product_catalog_number" и "count".
        dataframe: pd.DataFrame = (
            Feature
            .Missed
            .del_nan_rows(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )

        # Заменить nan значения в столбцах ("count", "sum_rub", "sum_eur")
        dataframe: pd.DataFrame = (
            Feature
            .Missed
            .replace_nan_to_zero(
                dataframe = dataframe,
                doc_type = doc_type
            )
        )

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

        # Переименовать склады/магазины
        dataframe: pd.DataFrame = (
            Feature
            .Polysemy
            .replace_shop_names(dataframe = dataframe)
        )

        # Проверить наименования магазинов на наличие в константах.
        data_verification.one_c.check_shop_names(dataframe = dataframe)

        # Удалить магазины, которые не в списке
        dataframe: pd.DataFrame = (
            Feature
            .Invalid
            .del_not_true_shop_names(dataframe = dataframe)
        )

        # Удалить некорректные подстроки из наименований контрагента
        dataframe: pd.DataFrame = (
            Feature
            .ErrorsTypos
            .del_false_substring_in_contragent(dataframe = dataframe)
        )

        # Переименовать контрагентов
        dataframe: pd.DataFrame = (
            Feature
            .Polysemy
            .replace_contragent_names(dataframe = dataframe)
        )

        # * FEATURE ENGINEERING
        # Добавить данные в столбец "Документ движения"
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

        # Добавить данные по типу, номеру, дате и времени документа движения/партии.
        dataframe: pd.DataFrame = (
            data_transform
            .one_c
            .BatchMovement
            .feature_document_data(dataframe = dataframe)
        )

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

        return dataframe
