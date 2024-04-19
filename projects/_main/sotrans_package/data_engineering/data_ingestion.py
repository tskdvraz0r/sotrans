import sys

import pandas as pd
import sqlalchemy as sa

sys.path.insert(0, r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\projects\_main\sotrans_package")
from _settings import mysql
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
            def extract_brand(
                    dataframe: pd.DataFrame
            ) -> None:
                """
                Notes:
                    Функция принимает на вход датафрейм одного из типов отчётов 1С и вытаскивает 
                    данные по брендам.

                Attributes:
                    SQL_BRAND_CONNECTION: Подключение к базе данных.
                    SQL_SCHEMA: Название схемы.
                    SQL_TABLE_NAME: Название таблицы.
                
                Args:
                    dataframe (pd.DataFrame): Датафрейм типа:
                        - Начальный остаток
                        - Приход
                        - Расход
                        - Конечный остаток
                        - Товары в пути
                """
                
                # ATTRIBUTES
                SQL_BRAND_CONNECTION: sa.Connection = sa.create_engine(
                    url=f"mysql+pymysql://{mysql.LOGIN}:{mysql.PASSWORD}@localhost/brand"
                ).connect()
                SQL_SCHEMA: str = "brand"
                SQL_TABLE_NAME: str = "brand_name"
                
                # DATA VALIDATION
                DataValidation.value_type(
                    value=dataframe,
                    expected_type=pd.DataFrame
                )
                
                # Сформировать датафрейм с уникальными наименованиями;
                df_brands: pd.DataFrame = (
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
                )
                
                # Загрузить данные из MySQL;
                sql_brand: pd.DataFrame = pd.read_sql_table(
                    table_name=SQL_TABLE_NAME,
                    con=SQL_BRAND_CONNECTION,
                    schema=SQL_SCHEMA
                )
                
                # Оставить только те бренды, которых нет в MySQL;
                df_brands: pd.DataFrame = (
                    df_brands[~df_brands["brand_name"].isin(values=sql_brand["brand_name"])]
                )
                
                # Загрузить новые данные в MySQL;
                df_brands.to_sql(
                    name=SQL_TABLE_NAME,
                    con=SQL_BRAND_CONNECTION,
                    schema=SQL_SCHEMA,
                    if_exists="append",
                    index=False,
                    chunksize=100
                )
