# Standard packages

# Extended packages
import pandas as pd
import sqlalchemy as sa

# Sotrans packages
from projects.main.sotrans_package import database


# ! FUNCTIONS
def save_data(
    dataframe: pd.DataFrame,
    doc_type: str,
    doc_month: int,
    doc_year: int,
    db_schema: str,
) -> None:
    """Сохранение дата фрейма в PostgreSQL базу.

    Args:
        dataframe (pd.DataFrame): Наименование дата фрейма
        doc_type (str): Наименование типа документа
        doc_month (int): Месяц документа
        doc_year (int): Год документа
        db_schema (str): Наименование "схемы" SQL
    """

    dataframe.to_sql(
        name = f"{doc_year}_{doc_month}_{doc_type}",
        con = sa.create_engine(url = database.postgresql.get_database_url(db_name = "one_c")),
        schema = db_schema,
        if_exists = "replace",
        index = False
    )
