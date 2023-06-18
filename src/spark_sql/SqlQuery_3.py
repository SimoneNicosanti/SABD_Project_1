from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


def query(dataFrame : DataFrame) -> tuple([DataFrame, float]) :

    timeDataFrame = dataFrame.groupBy(
        "TradingDate", "ID"
    ).agg(
        min("TradingTime"), max("TradingTime")
    ).withColumnsRenamed(
        {"min(TradingTime)" : "MinTime", "max(TradingTime)" : "MaxTime"}
    ).select(
        "TradingDate", "ID", "MinTime", "MaxTime"
    ).filter(
        col("MinTime") != col("MaxTime")
    ).withColumn(
        "Count", 
        lit(2)
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
        "Table_1.TradingDate", 
        "Table_1.ID", 
        "Table_1.Last", 
        "Table_2.Last", 
        "Times.Count"
    ).withColumn(
        "Variation", 
        col("Table_2.Last") - col("Table_1.Last")
    ).select(
        "TradingDate", 
        "ID", 
        "Variation", 
        "Count"
    ).withColumn(
        "Country",
        substring_index("ID", ".", -1)
    ).groupBy(
        "TradingDate", 
        "Country"
    ).agg(
        percentile_approx("Variation", 0.25, 1000000), 
        percentile_approx("Variation", 0.5, 1000000), 
        percentile_approx("Variation", 0.75, 1000000),
        sum("Count")
    ).withColumnsRenamed(
        {
        "percentile_approx(Variation, 0.25, 1000000)" : "25_perc", 
        "percentile_approx(Variation, 0.5, 1000000)" : "50_perc",
        "percentile_approx(Variation, 0.75, 1000000)" : "75_perc",
        "sum(Count)" : "Count"
        }
    )

    print("Collecting Result of Third Query with SQL")
    start = time.time()
    variationDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (variationDataFrame, end - start)