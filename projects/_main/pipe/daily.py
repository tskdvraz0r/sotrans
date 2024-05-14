import sys
import datetime as dt

sys.path.insert(0, r"C:\Users\d.zakharchenko\Desktop\new_structure\sotrans\projects\_main\package")
from base.data_engineering.data_pipeline import DataPipeline

# Расчёт тренда;
DataPipeline.morning_pipeline(
    current_year=2023,
    current_month=12,
    current_day=31,
    analyzed_period=48
)
