import numpy as np
import pandas as pd
from scipy import stats


class DataAnalysis:
    """
    Notes:
        Data Analysis (анализ данных): анализ данных...

    Attributes:
        pass

    Methods:
        pass

    Subclasses:
        - Descriptive Analysis (описательный анализ): включает в себя обобщение и
        описание основных характеристик набора данных. Фокусируется на организации
        и представлении данных значимым образов, часто с использованием таких
        показателей, как среднее значение, медиана, мода и стандартное отклонение.
        Содержит обзор данных и помогает выявить закономерности или тенденции;

        - Inferential Analysis (логический анализ): направлен на то, чтобы делать
        выводы или предсказания относительно большей совокупности на основе выборочных
        данных. Включает в себя применение статистических методов, таких как, проверка
        гипотез, доверительные интервалы и регрессионный анализ;

        - Exploratory Data Analysis (исследовательский анализ): фокусируется на
        изучении данных без предвзятых гипотез. Включает визуализацию, сводную
        статистику и методы профилирования данных для выявления закономерностей,
        взаимосвязей и интересных функций. Помогает генерировать гипотезы для
        дальнейшего анализа;

        - Diagnostic Analysis (диагностический анализ): направлен на понимание
        причинно-следственных связей в данных. Исследует факторы или переменные,
        которые влияют на конкретные результаты или поведение. Обычно используются
        такие методы, как регрессионный анализ, ANOVA или корреляционный анализ;

        - Predictive Analysis (прогнозный анализ): предполагает использование
        исторических данных для составления прогнозов относительно будущих
        результатов. Используются методы статистического моделирования, алгоритмы
        машинного обучения и анализ временных рядов для выявления закономерностей и
        построения прогнозных моделей. Часто используется для прогнозирования
        продаж, поведения клиентов или оценки риска;

        - Prescriptive Analysis (директивный анализ): выходит за рамки
        прогностического анализа, рекомендую действия или решения на основе
        прогнозов. Сочетает в себе исторические данные, алгоритмы оптимизации и
        бизнес-правила для получения полезной информации и оптимизации результатов.
        Это помогает в принятии решений и распределении ресурсов;
    """

    class DescriptiveAnalysis:
        """
        Notes:
            Descriptive Analysis (описательный анализ): включает в себя обобщение и
        описание основных характеристик набора данных. Фокусируется на организации
        и представлении данных значимым образов, часто с использованием таких
        показателей, как среднее значение, медиана, мода и стандартное отклонение.
        Содержит обзор данных и помогает выявить закономерности или тенденции;

        Attributes:
            pass

        Methods:
            pass
        """

        pass


    class InferentialAnalysis:
        """
        Notes:
            Inferential Analysis (логический анализ): направлен на то, чтобы делать
        выводы или предсказания относительно большей совокупности на основе выборочных
        данных. Включает в себя применение статистических методов, таких как, проверка
        гипотез, доверительные интервалы и регрессионный анализ;

        Attributes:
            pass

        Methods:
            pass
        """

        pass


    class ExploratoryDataAnalysis:
        """
        Notes:
            Exploratory Data Analysis (исследовательский анализ): фокусируется на
        изучении данных без предвзятых гипотез. Включает визуализацию, сводную
        статистику и методы профилирования данных для выявления закономерностей,
        взаимосвязей и интересных функций. Помогает генерировать гипотезы для
        дальнейшего анализа;

        Attributes:
            pass

        Methods:
            pass
        """

        class MeasureCentralTrend:
            """
            Notes:
                - Measures of the Central Trend (Меры центральной тенденции): ...

            Attributes:
                pass

            Methods:
                - mean(): pass
                - weighted_mean(): pass
                - trimmed_mean(): pass
                - median(): pass
                - weighted_median(): pass
                - robust(): pass
                - outlier(): pass
            """

            @classmethod
            def mean(cls):
                pass

            @classmethod
            def weighted_mean(cls):
                pass

            @classmethod
            def trimmed_mean(cls):
                pass

            @classmethod
            def median(cls):
                pass

            @classmethod
            def weighted_median(cls):
                pass

            @classmethod
            def robust(cls):
                pass

            @classmethod
            def outlier(cls):
                pass

        class MeasureVariability:
            """
            Notes:
                - Measures of Variability (Меры изменчивости): ...

            Attributes:
                pass

            Methods:
                - deviation(): pass
                - variance(): pass
                - standard_deviation() pass
                - mean_absolute_deviation(): pass
                - median_absolute_deviation_from_median(): pass
                - range(): pass
                - order_statistics(): pass
                - percentile(): pass
                - interquartile_range(): pass
            """

            @classmethod
            def deviation(cls):
                pass

            @classmethod
            def variance(cls):
                pass

            @classmethod
            def standard_deviation(cls):
                pass

            @classmethod
            def mean_absolute_deviation(cls):
                pass

            @classmethod
            def median_absolute_deviation_from_median(cls):
                pass

            @classmethod
            def range(cls):
                pass

            @classmethod
            def order_statistics(cls):
                pass

            @classmethod
            def percentile(cls):
                pass

            @classmethod
            def interquartile_range(cls):
                pass


    class DiagnosticAnalysis:
        """
        Notes:
            Diagnostic Analysis (диагностический анализ): направлен на понимание
        причинно-следственных связей в данных. Исследует факторы или переменные,
        которые влияют на конкретные результаты или поведение. Обычно используются
        такие методы, как регрессионный анализ, ANOVA или корреляционный анализ;

        Attributes:
            pass

        Methods:
            pass
        """

        @staticmethod
        def trend_calculation(
            dataframe: pd.DataFrame,
            current_year: int,
            current_month: int,
            current_day: int
        ) -> None:

            # ATTRIBYTES
            folder_result: str = (
                r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\apps\sales\coefficients\trend\data"
            )

            # EXTRACT
            src_dealer_interaction: pd.DataFrame = pd.read_excel(
                io=r"\\192.168.100.122\Trade\data_science\data\lookup\brand\dealer_and_brand.xlsx",
                engine="openpyxl",
                skiprows=2,
            ).drop(labels=["Unnamed: 0", "del"], axis=1)

            # TRANSFORM
            ## DATA PRE_PROCESSING
            # ФИЛЬТРАЦИЯ ДАННЫХ: оставить в src_data_mart только бренды со статусом "работаем";
            dataframe: pd.DataFrame = dataframe[
                dataframe["brand_name"].isin(
                    values=src_dealer_interaction["brand_name"][
                        src_dealer_interaction["interaction_status"] == "работаем"
                    ].unique()
                )
            ]

            ## FEATURE ENGINEERING
            # РАСЧЁТ: массив продаж с учётом упущенного спроса;
            dataframe.loc[:, "sales_with_lost_demand"] = [
                np.sum([ldb_value, exb_value], axis=0)
                for ldb_value, exb_value in zip(
                    dataframe["ldb_value_count_array"],
                    dataframe["exb_sales_and_cash_reg_count_array"],
                )
            ]

            ## MAIN ALGORITHM
            # ФОРМИРОВАНИЕ: итоговая таблица;
            dataframe: pd.DataFrame = dataframe[[
                "brand_name",
                "product_article_number",
                "sales_with_lost_demand"
            ]].copy()

            # РАСЧЁТ: линейная регрессия (slope, intercept, r_value, p_value, srd_err);
            dataframe.loc[:, "linreg"] = [
                stats.linregress(x=range(len(value)), y=value)
                for value in dataframe["sales_with_lost_demand"]
            ]

            ## DATA POST-PROCESSING
            # ФОРМИРОВАНИЕ: новые столбцы;
            # * slope - это мера наклона прямой, описывающей функцию линейной регресии;
            # * intercept - свободный коэффициент; то, чему равна зависимая переменная, если предиктор равен нулю;
            # * r_value - сила линейной зависимости между переменными-предикторами и переменной отклика. r_value, кратный 1, указывает на идеальную линейную зависимость, тогда как r_value кратный 0, указывает на отсутствие какой-либо линейной зависимости;
            # * p_value - значимость моели регрессии; сравнивается с уровнем значимости: 0.01, 0.05, 0.1;
            # * std_err - среднее расстояние, на которое наблюдаемые значения отклоняются от линии регрессии.
            dataframe.loc[:, "slope"] = [value[0] for value in dataframe["linreg"]]
            dataframe.loc[:, "intercept"] = [value[1] for value in dataframe["linreg"]]
            dataframe.loc[:, "r_value"] = [value[2] for value in dataframe["linreg"]]
            dataframe.loc[:, "p_value"] = [value[3] for value in dataframe["linreg"]]
            dataframe.loc[:, "std_err"] = [value[4] for value in dataframe["linreg"]]

            # ВИД: итоговая таблица;
            dataframe: pd.DataFrame = dataframe[
                [
                    "brand_name",
                    "product_article_number",
                    "slope",
                    "intercept",
                    "r_value",
                    "p_value",
                    "std_err",
                ]
            ].drop_duplicates(
                subset=["brand_name", "product_article_number"],
                keep="first",
                ignore_index=True
            )

            # LOAD
            # СОХРАНЕНИЕ: сохранить данные в JSON-файл;
            dataframe.to_json(
                path_or_buf=rf"{folder_result}\{current_year}_{current_month}_{current_day}_trend.json",
                orient="table",
                index=False,
            )


    class PredictiveAnalysis:
        """
        Notes:
            Predictive Analysis (прогнозный анализ): предполагает использование
        исторических данных для составления прогнозов относительно будущих
        результатов. Используются методы статистического моделирования, алгоритмы
        машинного обучения и анализ временных рядов для выявления закономерностей и
        построения прогнозных моделей. Часто используется для прогнозирования
        продаж, поведения клиентов или оценки риска;

        Attributes:
            pass

        Methods:
            pass
        """

        pass


    class PrescriptiveAnalysis:
        """
        Notes:
            Prescriptive Analysis (директивный анализ): выходит за рамки
        прогностического анализа, рекомендую действия или решения на основе
        прогнозов. Сочетает в себе исторические данные, алгоритмы оптимизации и
        бизнес-правила для получения полезной информации и оптимизации результатов.
        Это помогает в принятии решений и распределении ресурсов;

        Attributes:
            pass

        Methods:
            pass
        """

        pass
