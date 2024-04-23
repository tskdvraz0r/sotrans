import sys

import pandas as pd
import sqlalchemy as sa

sys.path.insert(0, r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\projects\_main\sotrans_package")
from _settings._database import _mysql
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
                    SQL_SHOP_CONNECTION (sa.Connection): Подключение к базе данных.
                    SQL_SHOP_SCHEMA (str): Наименование схемы.
                    SQL_SHOP_TABLE_NAME (str): Наименование таблицы.
                
                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """
                
                # ATTRIBUTES
                SQL_SHOP_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{_mysql.LOGIN}:{_mysql.PASSWORD}@localhost/warehouse_and_shop"
                ).connect()
                SQL_SHOP_SCHEMA: str = "warehouse_and_shop"
                SQL_SHOP_TABLE_NAME: str = "warehouse_and_shop"
                
                # EXTRACT SOURCE DATA
                # Получить данные по магазинам из MySQL;
                sql_shop: pd.DataFrame = pd.read_sql_table(
                    table_name=SQL_SHOP_TABLE_NAME,
                    con=SQL_SHOP_CONNECTION,
                    schema=SQL_SHOP_SCHEMA
                )
                
                # DATA VALIDATION
                DataValidation.value_type(
                    value=dataframe,
                    expected_type=pd.DataFrame
                )
                
                # MAIN ALGORITHM
                # Сформировать датафрейм с уникальными наименованиями;
                df_shop: pd.DataFrame = (
                    pd.DataFrame(data=dataframe["shop_name"])
                    .sort_values(
                        by="shop_name",
                        ascending=True
                    )
                    .drop_duplicates(
                        subset="shop_name",
                        keep="first",
                        ignore_index=True
                    )
                    .rename(
                        columns={
                            "shop_name": "name"
                        }
                    )
                )
                
                # Оставить только те магазины, которых нет в MySQL;
                df_shop: pd.DataFrame = df_shop[~df_shop["name"].isin(values=sql_shop["name"])]
                
                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_shop.to_sql(
                        name=SQL_SHOP_TABLE_NAME,
                        con=SQL_SHOP_CONNECTION,
                        schema=SQL_SHOP_SCHEMA,
                        if_exists="append",
                        index=False,
                        chunksize=100
                    )
                
                else:
                    print(
                        f"Количество новых магазинов: {len(df_shop)}",
                        df_shop["name"],sep="\n"
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
                    SQL_DEALER_CONNECTION (sa.Connection): Подключение к базе данных.
                    SQL_DEALER_SCHEMA (str): Наименование схемы.
                    SQL_DEALER_TABLE_NAME (str): Наименование таблицы.
                
                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """
                
                # ATTRIBUTES
                SQL_DEALER_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{_mysql.LOGIN}:{_mysql.PASSWORD}@localhost/dealer"
                ).connect()
                SQL_DEALER_SCHEMA: str = "dealer"
                SQL_DEALER_TABLE_NAME: str = "dealer"
                
                # EXTRACT SOURCE DATA
                # Получить данные по дилерам из MySQL;
                sql_dealers: pd.DataFrame = pd.read_sql_table(
                    table_name=SQL_DEALER_TABLE_NAME,
                    con=SQL_DEALER_CONNECTION,
                    schema=SQL_DEALER_SCHEMA
                )
                
                # DATA VALIDATION
                DataValidation.value_type(
                    value=dataframe,
                    expected_type=pd.DataFrame
                )
                
                # MAIN ALGORITHM
                # Сформировать датафрейм с уникальными наименованиями;
                df_dealer: pd.DataFrame = (
                    pd.DataFrame(data=dataframe["dealer_name"])
                    .sort_values(
                        by="dealer_name",
                        ascending=True
                    )
                    .drop_duplicates(
                        subset="dealer_name",
                        keep="first",
                        ignore_index=True
                    )
                    .rename(
                        columns={
                            "dealer_name": "name"
                        }
                    )
                )
                
                # Оставить только те бренды, которых нет в MySQL;
                df_dealer: pd.DataFrame = df_dealer[~df_dealer["name"].isin(values=sql_dealers["name"])]
                
                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_dealer.to_sql(
                        name=SQL_DEALER_TABLE_NAME,
                        con=SQL_DEALER_CONNECTION,
                        schema=SQL_DEALER_SCHEMA,
                        if_exists="append",
                        index=False,
                        chunksize=100
                    )
                
                else:
                    print(
                        f"Количество новых поставщиков: {len(df_dealer)}",
                        df_dealer["name"],sep="\n"
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
                    SQL_BRAND_CONNECTION (sa.Connection): Подключение к базе данных.
                    SQL_BRAND_SCHEMA (str): Наименование схемы.
                    SQL_BRAND_TABLE_NAME (str): Наименование таблицы.
                
                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """
                
                # ATTRIBUTES
                SQL_BRAND_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{_mysql.LOGIN}:{_mysql.PASSWORD}@localhost/brand"
                ).connect()
                SQL_BRAND_SCHEMA: str = "brand"
                SQL_BRAND_TABLE_NAME: str = "brand"
                
                # EXTRACT SOURCE DATA
                # Получить данные по брендам из MySQL;
                sql_brand: pd.DataFrame = pd.read_sql_table(
                    table_name=SQL_BRAND_TABLE_NAME,
                    con=SQL_BRAND_CONNECTION,
                    schema=SQL_BRAND_SCHEMA
                )
                
                # DATA VALIDATION
                DataValidation.value_type(
                    value=dataframe,
                    expected_type=pd.DataFrame
                )
                
                # MAIN ALGORITHM
                # Сформировать датафрейм с уникальными наименованиями;
                df_brand: pd.DataFrame = (
                    pd.DataFrame(data=dataframe["brand_name"])
                    .sort_values(
                        by="brand_name",
                        ascending=True
                    )
                    .drop_duplicates(
                        subset="brand_name",
                        keep="first",
                        ignore_index=True
                    )
                    .rename(
                        columns={
                            "brand_name": "name"
                        }
                    )
                )
                
                # Оставить только те бренды, которых нет в MySQL;
                df_brand: pd.DataFrame = df_brand[~df_brand["name"].isin(values=sql_brand["name"])]
                
                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    df_brand.to_sql(
                        name=SQL_BRAND_TABLE_NAME,
                        con=SQL_BRAND_CONNECTION,
                        schema=SQL_BRAND_SCHEMA,
                        if_exists="append",
                        index=False,
                        chunksize=100
                    )
                
                else:
                    print(
                        f"Количество новых брендов: {len(df_brand)}",
                        df_brand["name"],sep="\n"
                    )


            @staticmethod
            def extract_product(
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
                    
                    Вычленяет данные с наименованиями товаров и сверяет с текущими данными в MySQL.
                    Если есть новые данные: выводит список для ознакомления и предобработки, 
                    перед добавлением в базу.
                    Есди есть товары, которые в базе называются иначе (произошло переименование): 
                    выводит список для ознакомления и предобработки, перед добавлением в базу.
                
                Attributes:
                    SQL_PRODUCT_CONNECTION (sa.Connection): Подключение к базе данных.
                    SQL_BRAND_CONNECTION (sa.Connection): Подключение к базе данных.
                    SQL_TEMP_CONNECTION (sa.Connection): Подключение к базе данных.
                    
                Args:
                    dataframe (pd.DataFrame): Датафрейм с данными отчёта 1С.
                    processed (bool, optional): Статус предобработки новых данных. По умолчанию False.
                """
                
                # ATTRIBUTES
                SQL_PRODUCT_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{_mysql.LOGIN}:{_mysql.PASSWORD}@localhost/product"
                ).connect()
                
                SQL_BRAND_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{_mysql.LOGIN}:{_mysql.PASSWORD}@localhost/brand"
                ).connect()
                
                SQL_TEMP_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{_mysql.LOGIN}:{_mysql.PASSWORD}@localhost/_temp"
                ).connect()
                
                # DATA VALIDATION
                DataValidation.value_type(
                    value=dataframe,
                    expected_type=pd.DataFrame
                )
                
                # EXTRACT SOURCE DATA
                # Получить данные по товарам из MySQL;
                sql_product: pd.DataFrame = pd.read_sql_table(
                    table_name="product",
                    con=SQL_PRODUCT_CONNECTION,
                    schema="product"
                )
                
                # Получить данные по брендам из MySQL;
                sql_brand: pd.DataFrame = pd.read_sql_table(
                    table_name="brand",
                    con=SQL_BRAND_CONNECTION,
                    schema="brand"
                )
                
                # MAIN ALGORITHM
                # Сформировать таблицу с наименованиями товаров;
                df_product: pd.DataFrame = (
                    pd.DataFrame(data=dataframe[["brand_name", "product_article_number", "product_name"]])
                    .sort_values(
                        by=["brand_name", "product_article_number", "product_name"],
                        ascending=[True, True, True]
                    )
                    .drop_duplicates(
                        subset=["brand_name", "product_article_number"],
                        keep="first",
                        ignore_index=True
                    )
                    .rename(
                        columns={
                            "product_article_number": "article",
                            "product_name": "name"
                        }
                    )
                )
                
                # Заменить наименование бренда на индекс;
                df_product["brand_name"] = (
                    df_product["brand_name"]
                    .replace(
                        to_replace=sql_brand
                        .set_index(keys="name")
                        .to_dict()["id"]
                    )
                )

                # Переименование столбцов;
                df_product = df_product.rename(columns={"brand_name": "brand_id"})
                
                # Сформировать список SKU, который нет в MySQL;
                new_sku: pd.DataFrame = df_product[
                    (~df_product["brand_id"].isin(values=sql_product["brand_id"]))
                    & (~df_product["article"].isin(values=sql_product["article"]))
                ]
                
                # Сфромировать список SKU, которые есть в MySQL, но с другим наименованием;
                sku_to_replace: pd.DataFrame = (
                    df_product
                    .merge(
                        right=sql_product,
                        how="inner",
                        on=["brand_id", "article"],
                        validate="many_to_one",
                        suffixes=("_new", "_old")
                    )
                    .query(expr="name_old != None & name_new != name_old")
                    .rename(columns={"name_new": "name"})
                    .drop(
                        labels="name_old",
                        axis=1
                    )
                    [[
                        "id",
                        "brand_id",
                        "article",
                        "name"
                    ]]
                )
                
                # Если есть новые записи, их требуется предварительно "обработать" перед заливкой в базу;
                if processed:
                    
                    # Добавить новые данные в MySQL после обработки;
                    new_sku.to_sql(
                        name="product",
                        con=SQL_PRODUCT_CONNECTION,
                        schema="product",
                        if_exists="append",
                        index=False,
                        chunksize=100
                    )
                    
                    # Обновить временную таблицу, для замены данных.
                    sku_to_replace.to_sql(
                        name="temp_product",
                        con=SQL_TEMP_CONNECTION,
                        schema="_temp",
                        if_exists="append",
                        index=False,
                        chunksize=100
                    )
                    
                    # SQL-запрос на обновление данных (новые наименования);
                    sql_update_name: sa.TextClause = sa.text(
                        text="""
                        UPDATE product
                        INNER JOIN temp_product
                        ON product.id = temp_product.id
                        SET product.name = temp_product.name
                        """
                    )
                    
                    # Выполнение SQL-запроса и коммит; 
                    SQL_PRODUCT_CONNECTION.execute(sql_update_name)
                    SQL_PRODUCT_CONNECTION.commit()
                
                else:
                    print(
                        f"Количество новых SKU: {len(new_sku)}",
                        new_sku["name"],sep="\n"
                    )
                    print()
                    print(
                        f"Количество SKU с новыми наименованиями: {len(sku_to_replace)}",
                        sku_to_replace["name"],sep="\n"
                    )

