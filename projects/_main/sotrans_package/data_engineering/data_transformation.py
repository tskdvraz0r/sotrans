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
                None

            Methods:
                change_string_register()
                    Функция принимает на вход датафрейм, кортеж столбцов и регистр, который применяет ко всем записям
                    в указанных столбцах.

                clear_product_catalog_number()
                    Функция принимает на вход датафрейм и тип очистки: "origin" | "sotrans".
                    Очищает записи в столбце "product_catalog_number" от лишних символов (пунктуационных) и сохраняет
                    данные в новый столбец "product_article_number".

                del_false_substring_in_contragent()
                    Функция принимает на вход датафрейм и очищает наименования "контрагента" от некорректных подстрок.

                rename_columns()
                    Функция принимает на вход датафрейм и тип документа, и переименовывает рус. наименования на англ.
            """
            
            pass
        
        
        class Invalid:
            """
            Notes:
                Методы обработки признаков с недопустимыми значениями.

            Attributes:
                None

            Methods:
                del_not_true_shop_names()
                    Функция принимает на вход датафрейм и удаляет все записи со складами/магазинами, не входящими
                    в "корректный" список.

                del_total_row()
                    Функция принимает на вход датафрейм и удаляет из него строку "Итог".

                del_unclaimed_columns()
                    Функция принимает на вход датафрейм и удаляет из него невостребованные столбцы.
            """
            
            pass
        
        
        class Missed:
            """
            Notes:
                Методы обработки признаков с отсутствующими значениями.

            Attributes:
                None

            Methods:
                del_nan_rows()
                    Функция принимает на вход датафрейм и удаляет nan строки из столбцов (product_catalog_name,
                    {alias}_count).

                fill_nan_down()
                    Функция принимает на вход датафрейм и заполняет пропуски "вниз" в столбцах ("shop_name",
                    "document_batch", "document_movement", "dealer_name").

                fill_nan_in_dealer_brand()
                    Функция принимает на вход датафрейм и заполняет "заглушкой" пропуски в столбцах "дилер" и "бренд".

                replace_nan_to_zero()
                    Функция принимает на вход датафрейм и тип документа.
                    В столбцах "{alias}_sum_rub" и "{alias}_sum_eur" заменяет nan на 0.
            """
            
            pass
        
        
        class Polysemy:
            """
            Notes:
                Методы обработки признаков с многозначными значениями.

            Examples:
                "meklas group (connect)": "mekpa group (connect)",
                "mekpa otomotiv ith. ihr. san. tic. ltd.": "mekpa group (connect)",

            Attributes:
                None

            Methods:
                replace_shop_names()
                    Функция принимает на вход датафрейм и переименовывает магазины в столбце "shop_names".
                    По разным причинам наименования складов / магазинов могли измениться: закрытие, ошибочно заведённое.

                replace_contragent_names()
                    Функция принимает на вход датафрейм, в котором требуется привести подобные названия "контрагентов" к
                    одному.
            """
            
            pass
