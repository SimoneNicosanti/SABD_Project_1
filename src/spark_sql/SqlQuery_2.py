from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from pyspark.sql.window import Window


def query(dataFrame : DataFrame) -> tuple([DataFrame, float]) :

    # ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    timeDataFrame = dataFrame.groupBy(
        "TradingDate", "TradingTimeHour", "ID"
    ).agg(
        min("TradingTime"), max("TradingTime")
    ).withColumnsRenamed(
        {"min(TradingTime)" : "MinTime", "max(TradingTime)" : "MaxTime"}
    )
    
    # .select(
    #     "TradingDate", "TradingTimeHour", "ID", "MinTime", "MaxTime"
    # )

    #timeDataFrame.show() 
    

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


    #pricesDataFrame.show()

    # pricesDataFrame_1 = pricesDataFrame.where(
    #     "MinTime" == concat("TradingTimeHour", lit(":00:00.0000"))
    # ).select(
    #     "TradingDate", "TradingTimeHour", "ID", "MinLast"
    # ).withColumnRenamed(
    #     "MinLast" , "Last"
    # )

    #pricesDataFrame_1.show()

    initialPricesDataFrame = pricesDataFrame.select(
        "TradingDate", "TradingTimeHour", "ID", "MaxLast"
    ).withColumn(
        "TradingTimeHour", col("TradingTimeHour").substr(1, 2).cast("int") + 1
    ).withColumnRenamed(
        "MaxLast", "Last"
    )

    #pricesDataFrame_2.show()
    # initialPricesDataFrame = pricesDataFrame_1.union(pricesDataFrame_2)

    #initialPricesDataFrame.show()

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

    #prevHourDataFrame.show(n = 1000)

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

    #pricesCouplesDataFrame.show()

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

    variationsDataFrame.persist()

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


    resultDataFrame = bestRows.union(worstRows)
    #resultDataFrame.show(n = 100)


    

    #variationsDataFrame.show(n = 100)

   
    
    # .withColumn(
    #     "Variation", col("Table_1.Last") - col("Table_2.Last")
    # ).select(
    #     "TradingDate", "TradingTimeHour", "ID", "Variation", "Count"
    # ).groupBy(
    #     "TradingDate", "ID"
    # ).agg(
    #     avg("Variation"), stddev("Variation"), sum("Count")
    # ).withColumnsRenamed(
    #     {"avg(Variation)" : "Avg" , "stddev_samp(Variation)" : "StdDev"}
    # )

    print("Collecting result of Second Query with SQL")
    start = time.time()
    resultDataFrame = resultDataFrame.persist()
    resultDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultDataFrame, end - start)