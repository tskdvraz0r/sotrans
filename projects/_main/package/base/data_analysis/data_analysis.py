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

        pass


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
