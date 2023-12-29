# >>> IMPORTS
# Standard packages

# Extended packages
import xlwings as xw

# Sotrans packages
from ..data_validation import check


# >>> FUNCTIONS
def exec_vba(
    filepath: str,
    vba_name: str
) -> None:
    """
    Notes:
        Функция принимает на сход ссылку к XLSX-файлу и наименование VBA-скрипта, который требуется выполнить в файле.

    Args:
        filepath (str): Путь к XLSX-файлу.
        vba_name (str): Наименование VBA-скрипта, которые требуется применить к XLSX-файлу.
                        Доступные VBA-скрипты: start_balance, ...

    Raises:
        TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')
        FileExistsError(f'Файл по ссылке из переменной "{var_name}" не существует.')
        ValueError(f'Значение переменной "{name_arg}" вне списка {include_in}.')
    """

    # >>> DATA VALIDATION
    # Проверить, что в аргумент "filepath" передан тип данных "str".
    check.value_type(
        value = vba_name,
        expected_type = str
    )

    try:
        # Проверить, существует ли XLSX-файл по указанной ссылке.
        check.file_exists(filepath = filepath)

    except FileExistsError:
        # Если XLSX-файла не существует, проверить наличие JSON-файла.
        check.file_exists(filepath = filepath.replace(".xlsx", ".json"))

    # Проверить, что в аргумент "vba_name" передан тип данных "str".
        check.value_type(
            value = vba_name,
            expected_type = str
        )

    # Проверить, что значение в переменной "vba_name" находится в списке ("start_balance", ).
    check.value_in_range(
        value = vba_name,
        in_range = ("start_balance", ))

    # >>> MAIN ALGORITHM
    # Отключить визуализацию выполнения VBA-скрипта
    xw.App().visible = False

    # Открыть XLSX-файл
    excel_workbook = xw.Book(filepath)

    # Создать приложение xlwings
    excel_workbook_app = excel_workbook.app

    # Загрузить VBA-скрипт
    excel_vba_script = excel_workbook_app.macro(name = f"'PERSONAL.XLSB'!{vba_name}")

    # Выполнить VBA-скрипт
    excel_vba_script()

    # Сохранить изменения и закрыть файл
    excel_workbook.save()
    excel_workbook_app.quit()
