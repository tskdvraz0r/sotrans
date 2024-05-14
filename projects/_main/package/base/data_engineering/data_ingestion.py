import sys
import datetime as dt
import logging

import pandas as pd
import sqlalchemy as sa

sys.path.insert(
    0,
    r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\projects\_main\sotrans_package",
)
from config.database.mysql import MySQL
from data_analysis.data_validation import DataValidation


class DataIngestion:
    """
    Notes:
        Data Ingestion (потребление данных): перемещение данных из множества разных
        источников — баз данных SQL и NoSQL, IoT-устройств, веб-сайтов, потоковых
        сервисов и так далее — в целевую систему с целью преобразования для
        дальнейшего анализа. Данные поступают в различных видах и могут быть как
        структурированными, так и неструктурированными.

    Attributes:
        pass

    Methods:
        pass

    SubClasses:
        PartMart - методы получения данных из базы данных сайта part-mart.ru;
        OneC - методы получения данных из отчётов 1С;
    """

    class PartMart:
        """
        Notes:
            pass

        Attributes:
            pass

        Methods:
            pass
        """

        pass

    class OneC:
        """
        Notes:
            pass

        Attributes:
            pass

        Methods:
            pass
        """

        class BatchMovement:
            """
            Notes:
                pass

            Attributes:
                pass

            Methods:
                pass
            """
            
            @staticmethod
            def extract_document_batch(
                    dataframe: pd.DataFrame,
                    file_day: int,
                    file_month: int,
                    file_year: int,
                    processed: bool = False
            ) -> None:
                """
                Notes:
                    Метод принимает на вход датафрейм одного из типов отчётов 1С:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток

                    Вычленяет данные с документами партий и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки,
                    перед добавлением в базу.

                Attributes:
                    sql_batch_connection (sa.Connection): Подключение к базе данных.
                    sql_batch_schema (str): Наименование схемы.
                    sql_batch_table_name (str): Наименование таблицы.

                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """
                
                # ! ATTRIBUTES
                no_data: str = "_нет данных"
                sql_batch_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="document")).connect()
                )
                sql_batch_schema: str = "document"
                sql_batch_table_name: str = "batch"
                
                # ! EXTRACT SOURCE DATA
                # Загрузка данных по документам партий;
                sql_doc_batch: pd.DataFrame = (
                    pd.read_sql_table(
                        table_name=sql_batch_table_name,
                        con=sql_batch_connection,
                        schema=sql_batch_schema
                    )
                )
                
                # ! DATA VALIDATION
                for value, expected_type in zip(
                    (dataframe, file_day, file_month, file_year),
                    (pd.DataFrame, int, int, int)
                ):
                    (
                        DataValidation
                        .value_type(
                            value=value,
                            expected_type=expected_type
                        )
                    )
                
                for value, in_range in zip(
                    (file_month, file_year),
                    (range(1,13), range(2014, 2025))
                ):
                    (
                        DataValidation
                        .value_in_range(
                            value=value,
                            in_range=in_range
                        )
                    )
                
                # ! MAIN ALGORITHM
                # Формироваание датафрейма с документами движения партий товаров;
                df_doc_batch: pd.DataFrame = (
                    pd.DataFrame(
                        data=dataframe["document_batch"].drop_duplicates(
                            ignore_index=True
                        ),
                        columns=["document_batch"]
                    )
                )
                logging.info(msg="Сформирован датафрейм с документами движения партий товаров;")

                # Формирование столбец с наименованием документов;
                df_doc_batch["document"] = [
                    " ".join(value.rsplit(" от ", 1)[0].split()[:-1])
                    if value != no_data
                    else no_data
                    for value in df_doc_batch["document_batch"]
                ]
                logging.info(msg="Сформирован столбец с наименованием документов;")

                # Формирование столбца с номерами документов;
                df_doc_batch["number"] = [
                    value.rsplit(" от ", 1)[0].split()[-1]
                    if value != no_data
                    else "№00000000"
                    for value in df_doc_batch["document_batch"]
                ]
                logging.info(msg="Сформирован столбец с номерами документов;")

                # Формирование столбец с датами документов;
                df_doc_batch["date"] = [
                    value.rsplit(" от ", 1)[1].split()[0]
                    if value != no_data
                    else f"{str(file_day).rjust(2, "0")}.{str(file_month).rjust(2, "0")}.{file_year}"
                    for value in df_doc_batch["document_batch"]
                ]
                logging.info(msg="Сформирован столбец с датами документов;")

                # Формирование столбец с временем документов;
                df_doc_batch["time"] = [
                    value.rsplit(" от ", 1)[1].split()[1]
                    if value != no_data
                    else "00:00:00"
                    for value in df_doc_batch["document_batch"]
                ]
                logging.info(msg="Сформирован столбец с временем документов;")
                
                # Удаление невостребованного столбца;
                df_doc_batch: pd.DataFrame = (
                    df_doc_batch
                    .drop(
                        labels="document_batch",
                        axis=1
                    )
                )
                
                # Преобразование дат;
                df_doc_batch["date"] = [
                    dt.datetime.strptime(value, "%d.%m.%Y").date()
                    for value in df_doc_batch["date"]
                ]
                
                # Преобрзование времени;
                df_doc_batch["time"] = [
                    dt.datetime.strptime(value, "%H:%M:%S").time()
                    for value in df_doc_batch["time"]
                ]
                
                # Формирование объединённого столбца;
                df_doc_batch["document_batch"] = [
                    f"{document} {number} от {date} {time}"
                    for document, number, date, time in zip(
                        df_doc_batch["document"],
                        df_doc_batch["number"],
                        df_doc_batch["date"],
                        df_doc_batch["time"]
                    )
                ]
                
                # Фильтрация существующих данных в MySQL;
                df_doc_batch: pd.DataFrame = (
                    df_doc_batch[~df_doc_batch["document_batch"].isin(values=sql_doc_batch["document_batch"])]
                )
                
                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_doc_batch.to_sql(
                        name=sql_batch_table_name,
                        con=sql_batch_connection,
                        schema=sql_batch_schema,
                        if_exists="append",
                        index=False,
                        chunksize=100,
                    )

                else:
                    print(
                        f"Количество новых документов партий: {len(df_doc_batch)}",
                        df_doc_batch,
                        "",
                        sep="\n",
                    )
                


            @staticmethod
            def extract_document_movement(
                    dataframe: pd.DataFrame,
                    file_day: int,
                    file_month: int,
                    file_year: int,
                    doc_type: str,
                    processed: bool = False
            ) -> None:
                """
                Notes:
                    Метод принимает на вход датафрейм одного из типов отчётов 1С:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток

                    Вычленяет данные с документами партий и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки,
                    перед добавлением в базу.

                Attributes:
                    sql_movement_connection (sa.Connection): Подключение к базе данных.
                    sql_movement_schema (str): Наименование схемы.
                    sql_movement_table_name (str): Наименование таблицы.

                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    doc_type (str): Тип документа.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """
                
                # ! ATTRIBUTES
                no_data: str = "_нет данных"
                sql_movement_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="document")).connect()
                )
                sql_movement_schema: str = "document"
                sql_movement_table_name: str = "movement"
                
                # ! EXTRACT SOURCE DATA
                # Загрузка данных по документам партий;
                # Загрузка данных по документам движения;
                sql_doc_movement: pd.DataFrame = (
                    pd.read_sql_table(
                        table_name=sql_movement_table_name,
                        con=sql_movement_connection,
                        schema=sql_movement_schema
                    )
                )
                
                # ! DATA VALIDATION
                for value, expected_type in zip(
                    (dataframe, file_day, file_month, file_year, doc_type),
                    (pd.DataFrame, int, int, int, str)
                ):
                    (
                        DataValidation
                        .value_type(
                            value=value,
                            expected_type=expected_type
                        )
                    )
                
                for value, in_range in zip(
                    (file_month, file_year),
                    (range(1,13), range(2014, 2025))
                ):
                    (
                        DataValidation
                        .value_in_range(
                            value=value,
                            in_range=in_range
                        )
                    )
                
                # ! MAIN ALGORITHM
                # Формироваание датафрейма с документами движения партий товаров;
                df_doc_movement: pd.DataFrame = (
                    pd.DataFrame(
                        data=dataframe["document_movement"].drop_duplicates(
                            ignore_index=True
                        ),
                        columns=["document_movement"]
                    )
                )
                logging.info(msg="Сформирован датафрейм с документами движения партий товаров;")
                
                if doc_type in ("stb", "trb"):
                    # Формирование столбец с наименованием документов;
                    df_doc_movement["document"] = [
                        " ".join(value.rsplit(" от ", 1)[0].split()[:-1])
                        if value != no_data
                        else no_data
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с наименованием документов;")
                    
                    # Формирование столбца с номерами документов;
                    df_doc_movement["number"] = [
                        value.rsplit(" от ", 1)[0].split()[-1]
                        if value != no_data
                        else "№00000000"
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с номерами документов;")
                    
                    # Формирование столбец с датами документов;
                    df_doc_movement["date"] = [
                        value.rsplit(" от ", 1)[1].split("t")[0]
                        if value != no_data
                        else f"{str(file_day).rjust(2, "0")}.{str(file_month).rjust(2, "0")}.{file_year}"
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с датами документов;")
                    
                    # Формирование столбец с временем документов;
                    df_doc_movement["time"] = [
                        value.rsplit(" от ", 1)[1].split("t")[1]
                        if value != no_data
                        else "00:00:00"
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с временем документов;")
                    
                    # Удаление невостребованного столбца;
                    df_doc_movement: pd.DataFrame = (
                        df_doc_movement
                        .drop(
                            labels="document_movement",
                            axis=1
                        )
                    )
                    
                    # Преобразование дат;
                    df_doc_movement["date"] = [
                        dt.datetime.strptime(value, "%Y-%m-%d").date()
                        for value in df_doc_movement["date"]
                    ]
                    
                    # Преобрзование времени;
                    df_doc_movement["time"] = [
                        dt.datetime.strptime(value, "%H:%M:%S").time()
                        for value in df_doc_movement["time"]
                    ]
                    
                    # Формирование объединённого столбца;
                    df_doc_movement["document_movement"] = [
                        f"{document} {number} от {date} {time}"
                        for document, number, date, time in zip(
                            df_doc_movement["document"],
                            df_doc_movement["number"],
                            df_doc_movement["date"],
                            df_doc_movement["time"]
                        )
                    ]
                    
                    # Фильтрация существующих данных в MySQL;
                    df_doc_movement: pd.DataFrame = (
                        df_doc_movement[~df_doc_movement["document_movement"].isin(values=sql_doc_movement["document_movement"])]
                    )
                    
                else:
                    # Формирование столбец с наименованием документов;
                    df_doc_movement["document"] = [
                        " ".join(value.rsplit(" от ", 1)[0].split()[:-1])
                        if value != no_data
                        else no_data
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с наименованием документов;")
                    
                    # Формирование столбца с номерами документов;
                    df_doc_movement["number"] = [
                        value.rsplit(" от ", 1)[0].split()[-1]
                        if value != no_data
                        else "№00000000"
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с номерами документов;")
                    
                    # Формирование столбец с датами документов;
                    df_doc_movement["date"] = [
                        value.rsplit(" от ", 1)[1].split()[0]
                        if value != no_data
                        else f"{str(file_day).rjust(2, "0")}.{str(file_month).rjust(2, "0")}.{file_year}"
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с датами документов;")
                    
                    # Формирование столбец с временем документов;
                    df_doc_movement["time"] = [
                        value.rsplit(" от ", 1)[1].split()[1]
                        if value != no_data
                        else "00:00:00"
                        for value in df_doc_movement["document_movement"]
                    ]
                    logging.info(msg="Сформирован столбец с временем документов;")
                    
                    # Удаление невостребованного столбца;
                    df_doc_movement: pd.DataFrame = (
                        df_doc_movement
                        .drop(
                            labels="document_movement",
                            axis=1
                        )
                    )
                    
                    # Преобразование дат;
                    df_doc_movement["date"] = [
                        dt.datetime.strptime(value, "%d.%m.%Y").date()
                        for value in df_doc_movement["date"]
                    ]
                    
                    # Преобрзование времени;
                    df_doc_movement["time"] = [
                        dt.datetime.strptime(value, "%H:%M:%S").time()
                        for value in df_doc_movement["time"]
                    ]
                    
                    # Формирование объединённого столбца;
                    df_doc_movement["document_movement"] = [
                        f"{document} {number} от {date} {time}"
                        for document, number, date, time in zip(
                            df_doc_movement["document"],
                            df_doc_movement["number"],
                            df_doc_movement["date"],
                            df_doc_movement["time"]
                        )
                    ]
                    
                    # Фильтрация существующих данных в MySQL;
                    df_doc_movement: pd.DataFrame = (
                        df_doc_movement[~df_doc_movement["document_movement"].isin(values=sql_doc_movement["document_movement"].unique())]
                    )
                    
                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_doc_movement.to_sql(
                        name=sql_movement_table_name,
                        con=sql_movement_connection,
                        schema=sql_movement_schema,
                        if_exists="append",
                        index=False,
                        chunksize=100,
                    )

                else:
                    print(
                        f"Количество новых документов движения: {len(df_doc_movement)}",
                        df_doc_movement,
                        "",
                        sep="\n",
                    )


            @staticmethod
            def extract_shop(
                    dataframe: pd.DataFrame,
                    processed: bool = False
            ) -> None:
                """
                Notes:
                    Метод принимает на вход датафрейм одного из типов отчётов 1С:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток

                    Вычленяет данные с наименованиями магазинов и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки,
                    перед добавлением в базу.

                Attributes:
                    sql_shop_connection (sa.Connection): Подключение к базе данных.
                    sql_shop_schema (str): Наименование схемы.
                    sql_shop_table_name (str): Наименование таблицы.

                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """

                # ! ATTRIBUTES
                sql_shop_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="warehouse_and_shop")).connect()
                )
                sql_shop_schema: str = "warehouse_and_shop"
                sql_shop_table_name: str = "warehouse_and_shop"

                # ! EXTRACT SOURCE DATA
                # Получить данные по магазинам из MySQL;
                sql_shop: pd.DataFrame = pd.read_sql_table(
                    table_name=sql_shop_table_name,
                    con=sql_shop_connection,
                    schema=sql_shop_schema,
                )

                # ! DATA VALIDATION
                (
                    DataValidation
                    .value_type(
                        value=dataframe,
                        expected_type=pd.DataFrame
                    )
                )

                # ! MAIN ALGORITHM
                # Сформировать датафрейм с уникальными наименованиями;
                df_shop: pd.DataFrame = (
                    pd.DataFrame(data=dataframe["shop_name"])
                    .sort_values(by="shop_name", ascending=True)
                    .drop_duplicates(
                        subset="shop_name", keep="first", ignore_index=True
                    )
                    .rename(columns={"shop_name": "name"})
                )

                # Оставить только те магазины, которых нет в MySQL;
                df_shop: pd.DataFrame = df_shop[
                    ~df_shop["name"].isin(values=sql_shop["name"])
                ]

                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_shop.to_sql(
                        name=sql_shop_table_name,
                        con=sql_shop_connection,
                        schema=sql_shop_schema,
                        if_exists="append",
                        index=False,
                        chunksize=100,
                    )

                else:
                    print(
                        f"Количество новых магазинов: {len(df_shop)}",
                        df_shop,
                        "",
                        sep="\n",
                    )


            @staticmethod
            def extract_dealer(
                    dataframe: pd.DataFrame,
                    processed: bool = False
            ) -> None:
                """
                Notes:
                    Метод принимает на вход датафрейм одного из типов отчётов 1С:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток

                    Вычленяет данные с наименованиями поставщиков и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки,
                    перед добавлением в базу.

                Attributes:
                    sql_dealer_connection (sa.Connection): Подключение к базе данных.
                    sql_dealer_schema (str): Наименование схемы.
                    sql_dealer_table_name (str): Наименование таблицы.

                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """

                # ? ATTRIBUTES
                sql_dealer_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="dealer")).connect()
                )
                sql_dealer_schema: str = "dealer"
                sql_dealer_table_name: str = "dealer"

                # ! EXTRACT SOURCE DATA
                # Получить данные по дилерам из MySQL;
                sql_dealers: pd.DataFrame = pd.read_sql_table(
                    table_name=sql_dealer_table_name,
                    con=sql_dealer_connection,
                    schema=sql_dealer_schema,
                )

                # ! DATA VALIDATION
                (
                    DataValidation
                    .value_type(
                        value=dataframe,
                        expected_type=pd.DataFrame
                    )
                )

                # MAIN ALGORITHM
                # Сформировать датафрейм с уникальными наименованиями;
                df_dealer: pd.DataFrame = (
                    pd.DataFrame(data=dataframe["dealer_name"])
                    .sort_values(by="dealer_name", ascending=True)
                    .drop_duplicates(
                        subset="dealer_name", keep="first", ignore_index=True
                    )
                    .rename(columns={"dealer_name": "name"})
                )

                # Оставить только те бренды, которых нет в MySQL;
                df_dealer: pd.DataFrame = df_dealer[
                    ~df_dealer["name"].isin(values=sql_dealers["name"])
                ]

                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_dealer.to_sql(
                        name=sql_dealer_table_name,
                        con=sql_dealer_connection,
                        schema=sql_dealer_schema,
                        if_exists="append",
                        index=False,
                        chunksize=100,
                    )

                else:
                    print(
                        f"Количество новых поставщиков: {len(df_dealer)}",
                        df_dealer,
                        "",
                        sep="\n",
                    )


            @staticmethod
            def extract_brand(
                    dataframe: pd.DataFrame,
                    processed: bool = False
            ) -> None:
                """
                Notes:
                    Метод принимает на вход датафрейм одного из типов отчётов 1С:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток

                    Вычленяет данные с наименованиями брендов и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки,
                    перед добавлением в базу.

                Attributes:
                    sql_brand_connection (sa.Connection): Подключение к базе данных.
                    sql_brand_schema (str): Наименование схемы.
                    sql_brand_table_name (str): Наименование таблицы.

                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """

                # ? ATTRIBUTES
                sql_brand_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="brand")).connect()
                )
                sql_brand_schema: str = "brand"
                sql_brand_table_name: str = "brand"

                # * EXTRACT SOURCE DATA
                # Получить данные по брендам из MySQL;
                sql_brand: pd.DataFrame = pd.read_sql_table(
                    table_name=sql_brand_table_name,
                    con=sql_brand_connection,
                    schema=sql_brand_schema,
                )

                # ! DATA VALIDATION
                DataValidation.value_type(value=dataframe, expected_type=pd.DataFrame)

                # ! MAIN ALGORITHM
                # Сформировать датафрейм с уникальными наименованиями;
                df_brand: pd.DataFrame = (
                    pd.DataFrame(data=dataframe["brand_name"])
                    .sort_values(by="brand_name", ascending=True)
                    .drop_duplicates(
                        subset="brand_name", keep="first", ignore_index=True
                    )
                    .rename(columns={"brand_name": "name"})
                )

                # Оставить только те бренды, которых нет в MySQL;
                df_brand: pd.DataFrame = df_brand[
                    ~df_brand["name"].isin(values=sql_brand["name"])
                ]

                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_brand.to_sql(
                        name=sql_brand_table_name,
                        con=sql_brand_connection,
                        schema=sql_brand_schema,
                        if_exists="append",
                        index=False,
                        chunksize=100,
                    )

                else:
                    print(
                        f"Количество новых брендов: {len(df_brand)}",
                        df_brand,
                        "",
                        sep="\n",
                    )


            @staticmethod
            def extract_product(
                dataframe: pd.DataFrame, processed: bool = False
            ) -> None:
                """
                Notes:
                    Метод принимает на вход датафрейм одного из типов отчётов 1С:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток

                    Вычленяет данные с наименованиями товаров и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки,
                    перед добавлением в базу.
                    Есди есть товары, которые в базе называются иначе (произошло переименование):
                    выводит список для ознакомления и предобработки, перед добавлением в базу.

                Attributes:
                    sql_product_connection (sa.Connection): Подключение к базе данных.
                    sql_brand_connection (sa.Connection): Подключение к базе данных.
                    sql_temp_connection (sa.Connection): Подключение к базе данных.

                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """

                # ? ATTRIBUTES
                sql_product_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="product")).connect()
                )

                sql_brand_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="brand")).connect()
                )

                sql_temp_connection: sa.Connection = (
                    sa.create_engine(url=MySQL.get_database_url(name="_temp")).connect()
                )
                
                # ! DATA VALIDATION
                (
                    DataValidation
                    .value_type(
                        value=dataframe,
                        expected_type=pd.DataFrame
                    )
                )

                # ! EXTRACT SOURCE DATA
                # Получить данные по товарам из MySQL;
                sql_product: pd.DataFrame = (
                    pd.read_sql_table(
                        table_name="product",
                        con=sql_product_connection,
                        schema="product"
                    )
                )

                # Получить данные по брендам из MySQL;
                sql_brand: pd.DataFrame = (
                    pd.read_sql_table(
                        table_name="brand",
                        con=sql_brand_connection,
                        schema="brand"
                    )
                )

                # ! MAIN ALGORITHM
                # Сформировать таблицу с наименованиями товаров;
                df_product: pd.DataFrame = (
                    pd.DataFrame(
                        data=dataframe[[
                            "brand_name",
                            "product_article_number",
                            "product_name"
                        ]]
                    )
                    .sort_values(
                        by=[
                            "brand_name",
                            "product_article_number",
                            "product_name"
                        ],
                        ascending=[True, True, True],
                    )
                    .drop_duplicates(
                        subset=["brand_name", "product_article_number"],
                        keep="first",
                        ignore_index=True,
                    )
                    .rename(
                        columns={
                            "product_article_number": "article_number",
                            "product_name": "name",
                        }
                    )
                )

                # Заменить наименование бренда на индекс;
                df_product["brand_name"] = df_product["brand_name"].replace(
                    to_replace=sql_brand.set_index(keys="name").to_dict()["id"]
                )

                # Переименование столбцов;
                df_product = df_product.rename(columns={"brand_name": "brand_id"})

                # Сформировать список SKU, который нет в MySQL;
                new_sku: pd.DataFrame = df_product[
                    (~df_product["brand_id"].isin(values=sql_product["brand_id"]))
                    & (
                        ~df_product["article_number"].isin(
                            values=sql_product["article_number"]
                        )
                    )
                ]

                # Сфромировать список SKU, которые есть в MySQL, но с другим наименованием;
                sku_to_replace: pd.DataFrame = (
                    df_product.merge(
                        right=sql_product,
                        how="inner",
                        on=["brand_id", "article_number"],
                        validate="many_to_one",
                        suffixes=("_new", "_old"),
                    )
                    .query(expr="name_old != None & name_new != name_old")
                    .rename(columns={"name_new": "name"})
                    .drop(labels="name_old", axis=1)[
                        ["id", "brand_id", "article_number", "name"]
                    ]
                )

                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:

                    # Добавить новые данные в MySQL после обработки;
                    new_sku.to_sql(
                        name="product",
                        con=sql_product_connection,
                        schema="product",
                        if_exists="append",
                        index=False,
                        chunksize=100,
                    )

                    # Обновить временную таблицу, для замены данных.
                    sku_to_replace.to_sql(
                        name="temp_product",
                        con=sql_temp_connection,
                        schema="_temp",
                        if_exists="replace",
                        index=False,
                        chunksize=100,
                    )

                    # SQL-запрос на обновление данных (новые наименования);
                    sql_update_name: sa.TextClause = sa.text(
                        text="""
                        UPDATE product
                        INNER JOIN _temp.temp_product
                        ON product.id = temp_product.id
                        SET product.name = temp_product.name
                        """
                    )

                    # Выполнение SQL-запроса и коммит;
                    sql_product_connection.execute(statement=sql_update_name)
                    sql_product_connection.commit()

                else:
                    print(
                        f"Количество новых SKU: {len(new_sku)}",
                        new_sku,
                        "",
                        sep="\n",
                    )
                    print()
                    print(
                        f"Количество SKU с новыми наименованиями: {len(sku_to_replace)}",
                        sku_to_replace,
                        "",
                        sep="\n",
                    )

