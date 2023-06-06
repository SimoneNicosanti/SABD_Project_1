from pyspark.sql import *
from pyspark.sql.functions import *


def query(dataFrame : DataFrame) :

    # "ID", "SecType", "Last", "TradingDate", "TradingTime", "TradingTimeHour"

    timeDataFrame = dataFrame.select(
        "ID", "TradingDate", "TradingTimeHour", "TradingTime"
        ).groupBy(
            "ID", "TradingDate", "TradingTimeHour"
        ).agg(
            min("TradingTime"), max("TradingTime")
        ).sort(
            "TradingDate", "TradingTimeHour", "ID"
        ).withColumnsRenamed(
            {"min(TradingTime)" : "MinTime", "max(TradingTime)" : "MaxTime"}
        )

    #timeDataFrame.show(n = 100)

    firstTradeDataFrame = dataFrame.alias("Table").join(
        timeDataFrame.alias("Times"),
        on = [
            col("Table.ID") == col("Times.ID"),
            col("Table.TradingDate") == col("Times.TradingDate"),
            col("Table.TradingTime") == col("Times.MinTime")
        ]
        ).select(
            col("Table.ID"), col("Table.TradingDate"), col("Table.Last"), col("Times.TradingTimeHour"), col("Table.TradingTime")
        )
    
    firstTradeDataFrame.show(n = 50)
    
    firstTradeDataFrame = firstTradeDataFrame.groupBy(
        "ID", "TradingDate", "TradingTimeHour"
    ).agg(
        min("Last")
    ).withColumnRenamed(
        "min(Last)", "Last"
    ).sort(
        "ID", "TradingDate", "TradingTimeHour"
    )

    lastTradeDataFrame = dataFrame.alias("Table").join(
        timeDataFrame.alias("Times"),
        on = [
            col("Table.ID") == col("Times.ID"),
            col("Table.TradingDate") == col("Times.TradingDate"),
            col("Table.TradingTime") == col("Times.MaxTime")
        ]
        ).select(
            col("Table.ID"), col("Table.TradingDate"), col("Table.Last"), col("Times.TradingTimeHour"), col("Table.TradingTime")
        ).sort(
            col("Table.TradingDate"), col("Table.ID"), col("Table.TradingTimeHour")
        )
    
    lastTradeDataFrame = lastTradeDataFrame.groupBy(
        "ID", "TradingDate", "TradingTimeHour"
    ).agg(
        max("Last")
    ).withColumnRenamed(
        "max(Last)", "Last"
    ).sort(
        "ID", "TradingDate", "TradingTimeHour"
    )

    firstTradeDataFrame.show(n = 20)
    lastTradeDataFrame.show(n = 20)

    variationDataFrame = firstTradeDataFrame.alias("FirstTrade").join(
        lastTradeDataFrame.alias("LastTrade"),
        on = [
            col("FirstTrade.ID") == col("LastTrade.ID"),
            col("FirstTrade.TradingDate") == col("LastTrade.TradingDate"),
            col("FirstTrade.TradingTimeHour") == col("LastTrade.TradingTimeHour")
        ]
    ).withColumn(
        colName = "Variation",
        col = col("FirstTrade.Last") - col("LastTrade.Last")
    ).select(
        col("FirstTrade.ID"), col("FirstTrade.TradingDate"), col("FirstTrade.TradingTimeHour"), col("Variation")
    )

    variationDataFrame.show(n = 20)

    result = variationDataFrame.groupBy(
        "ID", "TradingDate"
    ).agg(
        avg("Variation"), stddev_pop("Variation"), count(expr("*"))
    )

    result.show(n = 100)

    return result