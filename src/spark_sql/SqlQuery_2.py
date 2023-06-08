from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


def query(dataFrame : DataFrame) -> tuple([DataFrame, float]) :

    # ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    timeDataFrame = dataFrame.groupBy(
        "TradingDate", "TradingTimeHour", "ID"
    ).agg(
        min("TradingTime"), max("TradingTime")
    ).withColumnsRenamed(
        {"min(TradingTime)" : "MinTime", "max(TradingTime)" : "MaxTime"}
    ).select(
        "TradingDate", "TradingTimeHour", "ID", "MinTime", "MaxTime"
    ).withColumn(
        "Count", 
        when(
            col("MinTime") == col("MaxTime"), 1
        ).otherwise(2)
    )

    variationDataFrame = dataFrame.alias("Table_1").join(
        timeDataFrame.alias("Times"),
        on = [
            col("Table_1.TradingDate") == col("Times.TradingDate"),
            col("Table_1.TradingTime") == col("Times.MinTime"),
            col("Table_1.ID") == col("Times.ID")
        ]
    ).join(
        dataFrame.alias("Table_2"),
        on = [
            col("Table_2.TradingDate") == col("Times.TradingDate"),
            col("Table_2.TradingTime") == col("Times.MaxTime"),
            col("Table_2.ID") == col("Times.ID")
        ]
    ).select(
        "Table_1.TradingDate", "Times.TradingTimeHour", "Table_1.ID", "Table_1.Last", "Table_2.Last", "Times.Count"
    ).withColumn(
        "Variation", col("Table_1.Last") - col("Table_2.Last")
    ).select(
        "TradingDate", "TradingTimeHour", "ID", "Variation", "Count"
    ).groupBy(
        "TradingDate", "ID"
    ).agg(
        avg("Variation"), stddev("Variation"), sum("Count")
    ).withColumnsRenamed(
        {"avg(Variation)" : "Avg" , "stddev_samp(Variation)" : "StdDev"}
    )

    print("Collecting result of Second Query with SQL")
    start = time.time()
    variationDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    ## TODO Classifica Azioni

    return (variationDataFrame, end - start)