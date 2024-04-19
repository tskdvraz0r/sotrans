import sys
import string
import pandas as pd

sys.path.insert(0, r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\projects\_main\sotrans_package")
from data_analysis.data_validation import DataValidation


class DataTransformation:
    """
    Notes:
        Data Transformation (преобразование данных): подстраивание разобщённых
        данных под потребности конечных пользователей. Этот этап включает в себя
        устранение ошибок и дублирование данных, их нормализацию и преобразование в
        нужный формат.

    Attributes:
        pass

    Methods:
        pass
    """

    class Value:
        """
        Notes:
            Класс обработки ошибок в записях.
        """
        
        class Contradiction:
            """
            Notes:
                Методы очистки записей, которые ссылаются на один объект и противоречат друг-другу.

            Attributes:
                pass

            Methods:
                pass
            """

            pass
        
        
        class Duplicate:
            """
            Notes:
                Методы очистки записей, задублированных по каким либо причинам.

            Attributes:
                pass

            Methods:
                pass
            """

            pass
        
        
        class Unique:
            """
            Notes:
                Методы очистки записей, которые должны быть уникальными: паспорт, страховой полис...

            Attributes:
                pass

            Methods:
                pass
            """

            pass
    
    class Feature:
        """
        Notes:
            Класс обработки ошибок в признаках.
        """
        
        
        class Abnormal:
            """
            Notes:
                Методы обработки признаков с аномальными значениями.

            Attributes:
                None

            Methods:
                None
            """
            
            pass
        
        
        class ErrorsTypos:
            """
            Notes:
                Методы обработки признаков с ошибками и опечатками.

            Attributes:
                pass

            Methods:
                pass
            """
            
            pass
        
        
        class Invalid:
            """
            Notes:
                Методы обработки признаков с недопустимыми значениями.

            Attributes:
                pass

            Methods:
                pass
            """
            
            @staticmethod
            def clear_product_catalog_number(
                    series: pd.Series,
                    clearing_method: str
            ) -> pd.Series:
                """
                Notes:
                    Функция принимает на вход pandas серию и тип очистки: "origin" | "sotrans".
                    Очищает значения от символов пунктуации и алиаса бренда (в методе sotrans).

                    В зависимости от выбранного типа, отличается логика очистки:
                        - origin - очистка от символов пунктуации.
                        - sotrans - очистка от символов пунктуации с предварительным удалением алиаса бренда "_brand".

                Args:
                    series (pd.Series): Pandas серия, в которой требуется очистить значения от 
                    символов пунктуации.
                    clearing_method (str): Метод очистки: "origin" | "sotrans".

                Raises:
                    TypeError: f"Тип переданныых данных не соответствует ожидаемому: {expected_type}")

                Returns:
                    (pd.DataFrame): Датафрейм со столбцом "product_article_number" (очищенным номером по каталогу).
                """
                
                # DATA VALIDATION
                for value, expected_type in zip(
                        (series, clearing_method),
                        (pd.Series, str)
                ):
                    DataValidation.value_type(
                        value=value,
                        expected_type=expected_type
                    )
                
                # FUNCTIONS
                def origin_method(
                        product_article_number: str
                ) -> str:
                    """
                    Notes:
                        Функция принимает на вход номер по каталогу/артикул и производит приведение 
                        к верхнему регистру и очистку от знаков пунктуации.

                    Args:
                        product_catalog_number (str): Номер по каталогу/артикул.
                    
                    Raises:
                        TypeError: f"Тип переданныых данных не соответствует ожидаемому: {expected_type}")

                    Returns:
                        str: Номер, очищенный от знаков пунктуации.
                    """
                    
                    # DATA VALIDATION
                    DataValidation.value_type(
                        value=product_article_number,
                        expected_type=str
                    )

                    # MAIN ALGORITHM
                    # Очистить от символов пунктуации;
                    for symbol in f"{string.punctuation} ":
                        product_article_number: str = (
                            product_article_number
                            .replace(symbol, "")
                            .strip()
                        )

                    # Возврат результата;
                    return product_article_number

                def sotrans_method(
                        product_catalog_number: str
                ) -> str:
                    """
                    Notes:
                        Функция принимает на вход номер по каталогу. Производит приведение 
                        к верхнему регистру, очистку от знаков пунктуации и алиаса бренда.
                    
                    Raises:
                        TypeError: f"Тип переданныых данных не соответствует ожидаемому: {expected_type}")

                    Args:
                        product_catalog_number (str): Номер по каталогу.

                    Returns:
                        str: Номер, очищенный от знаков пунктуации.
                    """
                    
                    # DATA VALIDATION
                    DataValidation.value_type(
                        value=product_catalog_number,
                        expected_type=str
                    )

                    # MAIN ALGORITHM
                    # Очистить от алиаса бренда и символов пунктуации;
                    for symbol in f"{string.punctuation} ":
                        product_catalog_number: str = (
                            product_catalog_number
                            .split("_", -1)[0]
                            .replace(symbol, "")
                            .strip()
                        )

                    # Возврат результата;
                    return product_catalog_number
                
                # MAIN ALGORITHM
                match clearing_method:
                    case "origin":
                        series = pd.Series(
                            data=[
                                origin_method(product_article_number=value)
                                for value in series
                            ]
                        )
                    
                    case "sotrans":
                        series = pd.Series(
                            data=[
                                sotrans_method(product_catalog_number=value)
                                for value in series
                            ]
                        )

                # Возврат результата;
                return series
        
        class Missed:
            """
            Notes:
                Методы обработки признаков с отсутствующими значениями.

            Attributes:
                pass

            Methods:
                pass
            """
            
            pass
        
        
        class Polysemy:
            """
            Notes:
                Методы обработки признаков с многозначными значениями.

            Examples:
                pass

            Attributes:
                None

            Methods:
                pass
            """
            
            pass
