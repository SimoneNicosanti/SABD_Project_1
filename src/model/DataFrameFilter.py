from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def setUpDataFrame(dataframe : DataFrame) -> DataFrame :

    modifiedDataframe = dataframe

    # Null Date removal
    modifiedDataframe = modifiedDataframe.where(modifiedDataframe.TradingDate.isNotNull())
    # Formatting Time Field
    modifiedDataframe = modifiedDataframe.withColumn("TradingTime", date_format("TradingTime", "HH:mm:ss.SSS"))
    # Formatting Date Field
    modifiedDataframe = modifiedDataframe.withColumn(colName = "TradingDate", col = to_date("TradingDate", "dd-MM-yyyy"))
    # Removing rows with not valid time
    modifiedDataframe = modifiedDataframe.where(modifiedDataframe.TradingTime != "00:00:00.000")
    # Removing duplicate rows
    modifiedDataframe = modifiedDataframe.distinct()
    # Sorting by data and time
    modifiedDataframe = modifiedDataframe.sort("TradingDate", "TradingTime")

    

    return modifiedDataframe