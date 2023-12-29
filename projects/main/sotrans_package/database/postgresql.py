# >>> IMPORTS
# Standard packages

# Extended packages

# Sotrans packages
from .._settings import postgresql
from projects.main.sotrans_package import data_validation


# >>> FUNCTIONS
def get_database_url(db_name: str) -> str:
    """
    Notes:
        Функция принимает на вход название базы данных в postgres и возвращает ссылку для подключения.
    
    Args:
        db_name (str): Наименование базы данных. ("tensor", "monitoring", "coefficient", "classifier")
    
    Raises:
        TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
        ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')
        
    Returns:
        (str): Функция возвращает строковую ссылку на базу данных "tensor" в Postgres.
    """

    # >>> DATA VALIDATION
    # Проверить, что в аргумент "db_name" передан тип данных str.
    data_validation.check.value_type(
        value = db_name,
        expected_type = str
    )

    # Проверить, что значение "db_name" находится в списке.
    data_validation.check.value_in_range(
        value = db_name,
        in_range = postgresql.DATABASES
    )

    # >>> MAIN ALGORITHM
    return f"postgresql://{postgresql.LOGIN}:{postgresql.PASSWORD}@localhost:5432/{db_name}"
