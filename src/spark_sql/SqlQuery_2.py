from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from pyspark.sql.window import Window


def query(dataFrame : DataFrame) -> tuple([DataFrame, float]) :

    # DataFrame of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    ## Looking for min time and max time for any hour
    timeDataFrame = dataFrame.groupBy(
        "TradingDate", "TradingTimeHour", "ID"
    ).agg(
        min("TradingTime"), max("TradingTime")
    ).withColumnsRenamed(
        {"min(TradingTime)" : "MinTime", "max(TradingTime)" : "MaxTime"}
    )
    
    ## Looking for first price and last price for every hour
    pricesDataFrame = dataFrame.alias("Table_1").withColumnRenamed("Last", "MinLast").join(
        timeDataFrame.alias("Times"),
        on = [
            col("Table_1.TradingDate") == col("Times.TradingDate"),
            col("Table_1.TradingTime") == col("Times.MinTime"),
            col("Table_1.ID") == col("Times.ID")
        ]
    ).join(
        dataFrame.withColumnRenamed("Last", "MaxLast").alias("Table_2"),
        on = [
            col("Table_2.TradingDate") == col("Times.TradingDate"),
            col("Table_2.TradingTime") == col("Times.MaxTime"),
            col("Table_2.ID") == col("Times.ID")
        ]
    ).select(
        "Table_1.TradingDate", "Times.TradingTimeHour", "Table_1.ID", "MinTime", "MinLast", "MaxTime", "MaxLast"
    )

    ## Last price of an hour is the previous price of the prev hour beacause there is no row with TradingTime equals to hh:00:00.0000
    initialPricesDataFrame = pricesDataFrame.select(
        "TradingDate", "TradingTimeHour", "ID", "MaxLast"
    ).withColumn(
        "TradingTimeHour", col("TradingTimeHour").substr(1, 2).cast("int") + 1
    ).withColumnRenamed(
        "MaxLast", "Last"
    )

    ## Looking for previous price for every hour: it is the price of the biggest previous hour
    prevHourDataFrame = initialPricesDataFrame.alias("First").join(
        initialPricesDataFrame.alias("Second"),
        on = [
            col("First.TradingDate") == col("Second.TradingDate"),
            col("First.ID") == col("Second.ID")
        ]
    ).where(
        "First.TradingTimeHour < Second.TradingTimeHour"
    ).groupBy(
        "First.TradingDate", "Second.TradingTimeHour", "First.ID"
    ).agg(
        max("First.TradingTimeHour")
    ).withColumnRenamed(
        "max(First.TradingTimeHour)" , "PrevHour"
    ).withColumnRenamed(
        "TradingTimeHour", "Hour"
    )

    ## DataFrame of couples
    pricesCouplesDataFrame = initialPricesDataFrame.withColumnRenamed("Last", "PrevPrice").alias("First").join(
        prevHourDataFrame.alias("Times"),
        on = [
            col("First.TradingDate") == col("Times.TradingDate"),
            col("First.ID") == col("Times.ID"),
            col("First.TradingTimeHour") == col("Times.PrevHour")
        ]
    ).join(
        initialPricesDataFrame.withColumnRenamed("Last", "Price").alias("Second"),
        on = [
            col("Second.TradingDate") == col("Times.TradingDate"),
            col("Second.ID") == col("Times.ID"),
            col("Second.TradingTimeHour") == col("Times.Hour")
        ]
    ).select(
        "Times.TradingDate", "Times.ID", "Times.PrevHour", "Times.Hour", "First.PrevPrice", "Second.Price"
    )


    ## Variations DataFrame
    variationsDataFrame = pricesCouplesDataFrame.withColumn(
        "Variation", col("Price") - col("PrevPrice")
    ).select(
        "TradingDate", "ID", "Variation"
    ).groupBy(
        "TradingDate", "ID"
    ).agg(
        avg("Variation"), stddev_pop("Variation"), count(expr("*")) + 1
    ).withColumnRenamed(
        "avg(Variation)" , "Avg"
    ).withColumnRenamed(
        "stddev_pop(Variation)" , "StdDev"
    ).withColumnRenamed(
        "(count(1) + 1)" , "Count"
    )

    ## Looking for best stocks
    bestWindows = Window.partitionBy(
        "TradingDate"
    ).orderBy(
        col("TradingDate"),
        col("Avg").desc()
    )

    bestRows = variationsDataFrame.withColumn(
        "Row" ,row_number().over(bestWindows)
    ).filter(
        col("Row") <= 5
    ).drop("Row")


    ## Looking for worst stocks
    worstWindows = Window.partitionBy(
        "TradingDate"
    ).orderBy(
        col("TradingDate"),
        col("Avg")
    )

    worstRows = variationsDataFrame.withColumn(
        "Row" ,row_number().over(worstWindows)
    ).filter(
        col("Row") <= 5
    ).drop("Row")


    ## Union of best and worst stocks
    resultDataFrame = bestRows.union(worstRows)

    print("Collecting result of Second Query with SQL")
    start = time.time()
    resultDataFrame = resultDataFrame.persist()
    resultDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultDataFrame, end - start)