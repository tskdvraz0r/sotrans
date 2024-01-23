
# Standard packages
import os

# Extended packages
import pandas as pd

# Sotrans packages
from projects.main.sotrans_package import excel
from projects.main.sotrans_package import data_migration
from projects.main.sotrans_package import data_validation


def batch_movement_xlsx(
    doc_types: tuple[str, ...],
    doc_paths: tuple[str, ...],
    doc_years: tuple[int, ...],
    is_vba: bool = False
) -> None:

    # ! DATA VALIDATION
    # Проверить корректность типов данных.
    for value, value_type in zip(
        (doc_types, doc_paths, doc_years, is_vba),
        (tuple, tuple, tuple, bool)
    ):
        data_validation.check.value_type(
            value = value,
            expected_type = value_type
        )

    # ! MAIN ALGORITHM
    for doc_path, doc_type in zip(doc_paths, doc_types):
        for doc_year in doc_years:
            for filename in os.listdir(path = rf"{doc_path}\{doc_year}"):
                print(
                    f"{doc_type} - {doc_year} - {filename}",
                    "_________________________",
                    sep = "\n"
                )

                if is_vba:
                    # Выполнить VBA-скрипт.
                    excel.vba.exec_vba(
                        filepath = rf"{doc_path}\{doc_year}\{filename}",
                        vba_name = "start_balance"
                    )

                # Загрузить датафрейм.
                df: pd.DataFrame = pd.read_excel(
                    io = rf"{doc_path}\{doc_year}\{filename}",
                    engine = "openpyxl"
                )

                # Получить номер месяца из наименования файла.
                doc_month: int = int(filename.replace(".xlsx", "").split("_")[-2])

                # Выполнить очистку 1С-отчёта.
                df: pd.DataFrame = excel.one_c.BatchMovement.clear_data_file(
                    dataframe = df,
                    doc_type = doc_type,
                    file_type = "xlsx",
                    file_year = doc_year,
                    file_month = doc_month
                )

                # Сохранить результат в PostgreSQL.
                data_migration.to_postgres.save_data(
                    dataframe = df,
                    doc_type = doc_type,
                    doc_month = doc_month,
                    doc_year = doc_year,
                    db_schema = doc_type
                )

                print("\n")


def batch_movement_json(
    doc_types: tuple[str, ...],
    doc_paths: tuple[str, ...],
    doc_years: tuple[int, ...]
) -> None:

    # ! DATA VALIDATION
    # Проверить корректность типов данных.
    for value, value_type in zip(
        (doc_types, doc_paths, doc_years),
        (tuple, tuple, tuple, bool)
    ):
        data_validation.check.value_type(
            value = value,
            expected_type = value_type
        )

    # ! MAIN ALGORITHM
    for doc_path, doc_type in zip(doc_paths, doc_types):
        for doc_year in doc_years:
            for doc_month in os.listdir(path = rf"{doc_path}\{doc_year}"):

                list_with_dataframes: list[pd.DataFrame] = []

                for filename in os.listdir(path = rf"{doc_path}\{doc_year}\{doc_month}"):
                    print(
                        f"{doc_type} - {doc_year} - {filename}",
                        "_________________________",
                        sep = "\n"
                    )

                    # Загрузить датафрейм.
                    df: pd.DataFrame = pd.read_json(
                        path_or_buf = rf"{doc_path}\{doc_year}\{doc_month}\{filename}",
                        orient = "records",
                        encoding = "utf-8-sig"
                    )

                    # Выполнить очистку 1С-отчёта.
                    df: pd.DataFrame = excel.one_c.BatchMovement.clear_data_file(
                        dataframe = df,
                        doc_type = doc_type,
                        file_type = "json",
                        file_year = doc_year,
                        file_month = int(doc_month)
                    )

                    list_with_dataframes.append(df)

                    print("\n")

                # Сохранить результат в PostgreSQL.
                data_migration.to_postgres.save_data(
                    dataframe = pd.concat(list_with_dataframes),
                    doc_type = doc_type,
                    doc_month = int(doc_month),
                    doc_year = doc_year,
                    db_schema = doc_type
                )
