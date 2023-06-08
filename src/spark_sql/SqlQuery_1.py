from pyspark.sql import *
from pyspark.sql.functions import *
import time


def query(dataframe : DataFrame) -> tuple([DataFrame, float]) :
    result = dataframe.select(
        "TradingDate", "TradingTimeHour", "ID", "Last"
    ).where(
        dataframe.SecType == "E"
    ).where(
        dataframe.ID.endswith(".FR")
    ).groupBy(
        "TradingDate", "TradingTimeHour", "ID"
    ).agg(
        min("Last"), avg("Last"), max("Last"), count(expr("*"))
    )
    
    print("Collecting result of First Query with SQL")
    start = time.time()
    result.collect()
    end = time.time()


    return (result, end - start)