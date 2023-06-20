from pyspark.sql import *
from pyspark.sql.functions import *
import time


def query(dataframe : DataFrame) -> tuple([DataFrame, float]) :
    resultDataFrame = dataframe.select(
        "TradingDate", "TradingTimeHour", "ID", "Last"
    ).where(
        dataframe.SecType == "E"
    ).where(
        dataframe.ID.endswith(".FR")
    ).groupBy(
        "TradingDate", "TradingTimeHour", "ID"
    ).agg(
        min("Last"), avg("Last"), max("Last"), count(expr("*"))
    ).withColumnsRenamed(
        {"min(Last)" : "Min", "avg(Last)" : "Avg", "max(Last)" : "Max", "count(1)" : "Count"}
    )

    
    print("Collecting result of First Query with SQL")
    start = time.time()
    resultDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)


    return (resultDataFrame, end - start)