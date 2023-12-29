# ! IMPORTS
# * Standard packages
import os
import typing
import logging as log

import pandas as pd


# * Extended packages

# * Sotrans packages


# ! FUNCTIONS
def value_type(
    value: typing.Any,
    expected_type: typing.Any
) -> None:
    """
    Notes:
        Функция принимает на вход значение и ожидаемый тип данных.
        Производит проверку с логированием на соответствие переданного типа данных к ожидаемому.

    Args:
        value (typing.Any): Значение.
        expected_type (typing.Any): Ожидаемый тип данных значения.

    Raises:
        TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
    """

    # Получить название переменной в виде строки.
    var_name: str = f"{value=}".split('=')[0]

    if not isinstance(value, expected_type):
        log_critical_message: str = f'Аргумент "{var_name}" ожидает тип данных "{expected_type}".'
        log.critical(msg = log_critical_message)
        raise TypeError(log_critical_message)
    log.info(msg = f'Тип данных в аргументе "{var_name}" корректный.')


def value_in_range(
    value: typing.Any,
    in_range: typing.Any
) -> None:
    """
    Notes:
        Функция принимает на вход значение

    Args:
        value (typing.Any): Переменная. Искомое значение.
        in_range (tuple[typing.Any]): Кортеж с исходными значениями.

    Raises:
        ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')
    """

    # Получить название переменной в виде строки.
    var_name: str = f"{value=}".split("=")[0]
    include_in_name: str = f"{in_range=}".split("=")[0]

    if value not in in_range:
        log_critical_message: str = f'Значение переменной "{var_name}" вне списка {include_in_name}.'
        log.critical(msg=log_critical_message)
        raise ValueError(log_critical_message)
    log.info(msg=f'Значение переменной "{var_name}" корректно.')


def file_exists(filepath: str) -> None:
    """
    Notes:
        Функция принимает на вход ссылку на файл и проверяет, существует ли он.

    Args:
        filepath (str): Путь к файлу.

    Raises:
        FileExistsError(f'Файл по ссылке из переменной "{var_name}" не существует.')
    """

    # Получить название переменной в виде строки.
    var_name: str = f"{filepath=}".split('=')[0]

    if not os.path.exists(filepath):
        log_critical_message: str = f'Файл по ссылке из переменной "{var_name}" не существует.'
        log.critical(msg = log_critical_message)
        raise FileExistsError(log_critical_message)
    log.info(msg = f'Файл по ссылке из переменной "{var_name}" существует.')


def column_exists(
    dataframe: pd.DataFrame,
    column: str
) -> None:
    """
    Notes:
        Функция принимает на вход ссылку на файл и проверяет, существует ли он.

    Args:
        dataframe (pd.DataFrame): Путь к файлу.
        column (str):

    Raises:
        FileExistsError(f'Файл по ссылке из переменной "{var_name}" не существует.')
    """

    # Получить название переменной в виде строки.
    dataframe_name: str = f"{dataframe=}".split("=")[0]
    column_name: str = f"{column=}".split("=")[0]

    if column not in dataframe.columns:
        log_critical_message: str = f'Столбец "{column_name}" в датафрейме "{dataframe_name}" не существует.'
        log.critical(msg = log_critical_message)
        raise ValueError(log_critical_message)
    log.info(msg = f'Столбец "{column_name}" в датафрейме "{dataframe_name}" существует.')
