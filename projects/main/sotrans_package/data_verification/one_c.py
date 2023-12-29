# ! IMPORTS
# * Standard packages

# * Extended packages
import pandas as pd

# * Sotrans packages
from projects.main.sotrans_package._settings import path
from projects.main.sotrans_package import constant
from projects.main.sotrans_package import data_validation


# ! FUNCTIONS
def check_shop_names(dataframe: pd.DataFrame) -> None:
    """
    Notes:
        Функция принимает на вход датафрейм и сверяет множество наименований магазинов с известными данными,
        прописанными в константах. Если есть неизвестные, добавляет их в WARNING-файл, для последующей обработки.


    Args:
        dataframe (pd.DataFrame): Датафрейм, в котором требуется проверить наличие новых наименований.

    Raises:
        TypeError(f'Аргумент "{var_name}" ожидает тип данных "{var_type}".')

    Returns:
        Функция ничего не возвращает. Если есть "неизвестные" данные -- записывает их в WARNING-файл.

    """

    # ! ATTRIBUTES
    # Ссылка на отчётный CSV-файл с магазинами, которых нет в константах.
    missing_shop_file: str = rf"{path.sotrans.Root.Data._get_export_folder()}\main\missing_shop.csv"

    # ! DATA VALIDATION
    # Проверить тип данных в аргументе "dataframe".
    data_validation.check.value_type(
        value = dataframe,
        expected_type = pd.DataFrame
    )

    # Проверить, существует ли файл.
    try:
        data_validation.check.file_exists(
            filepath = missing_shop_file
        )
    except FileExistsError:
        (
            pd.DataFrame(
                data = [],
                columns = ["shop_name"]
            )
            .to_csv(
                path_or_buf = missing_shop_file,
                index = False
            )
        )

    # ! THE MAIN ALGORITHM
    # WARNING-файл.
    warning_file: pd.DataFrame = pd.read_csv(
        filepath_or_buffer = missing_shop_file
    )

    # Множество наименований магазинов из датафрейма.
    shop_names_in_dataframe = set(dataframe["shop_name"])

    # Множество наименований магазинов из WARNING-файла.
    shop_names_in_warning_file: set = set(warning_file["shop_name"])

    # Множество наименований магазинов из констант.
    shop_names_in_constant = constant.shop.FALSE_SHOP_NAMES.union(constant.shop.TRUE_SHOP_NAMES)

    # Множество наименований магазинов, которых нет в константах.
    shop_names_not_in_constant = set(
        shop_name
        for shop_name in shop_names_in_dataframe
        if shop_name not in shop_names_in_constant
    )

    # Множество наименований магазинов, которых нет в WARNING-файле.
    shop_names_not_in_warning_file = set(
        shop_name
        for shop_name in shop_names_not_in_constant
        if shop_name not in shop_names_in_warning_file
    )

    # Объединить новое множество неизвестных наименований с данными из WARNING-файла.
    new_set_shop_names: set = shop_names_in_warning_file.union(shop_names_not_in_warning_file)

    # Если есть новые наименования магазинов -- сохранить результат в CSV-файл.
    if len(new_set_shop_names) > 0:
        (
            pd.DataFrame(
                data = new_set_shop_names,
                columns = ["shop_name"]
            )
            .to_csv(
                path_or_buf = missing_shop_file,
                index = False
            )
        )

    print(f"Количество новых наименований магазинов: {len(new_set_shop_names)}!")
