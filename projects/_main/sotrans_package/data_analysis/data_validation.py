import os
import typing


class DataValidation:
    """
    Notes:
        Data Validation (валидация данных): проверка, что данными можно пользоваться.

    Attributes:
        pass

    Methods:
        pass
    """

    @staticmethod
    def value_type(
            value: typing.Any,
            expected_type: typing.Any
    ) -> None:
        """
        Notes:
            Функция принимает на вход значение и ожидаемый тип данных. Производит проверку на 
            соответствие переданного типа данных к ожидаемому.

        Args:
            value (typing.Any): Проверяемое значение.
            expected_type (typing.Any): Ожидаемый тип данных.

        Raises:
            TypeError: f"Аргумент ожидает тип данных {expected_type}"
        """
        
        if not isinstance(value, expected_type):
            raise TypeError(f"Значение не соответствует типу данных {expected_type}")
    
    @staticmethod
    def value_in_range(
            value: typing.Any,
            in_range: typing.Iterable
    ) -> None:
        """
        Notes:
            Функция принимает на вход значение и итерируемую последовательность. Проверяет на 
            вхождение переданного значения в итерируемую последовательность.

        Args:
            value (typing.Any): Искомое значение.
            in_range (typing.Iterable): Итерируемая последовательность.

        Raises:
            ValueError: "Значение не входит в переданный кортеж/список/..."
        """
        
        if value not in in_range:
            raise ValueError("Значение не входит в переданный кортеж/список/...")
    
    @staticmethod
    def file_exists(
            filepath: str
    ) -> None:
        """
        Notes:
            Функция принимает на вход ссылку на файл и проверяет, существует ли он.

        Args:
            filepath (str): Путь к файлу.

        Raises:
            FileExistsError: "Файла по указанному пути не существует"
        """
        
        if not os.path.exists(path=filepath):
            raise FileExistsError("Файла по указанному пути не существует")
