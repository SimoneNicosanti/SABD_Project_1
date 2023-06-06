from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def prepareForProcessing(dataframe : DataFrame) -> DataFrame :

    modifiedDataframe = dataframe

    ## TODO Posso rimuovere il testo dell'intestazione prima oppure lo devo fare con spark ??

    # Renaming Dataframe colf for easier access
    modifiedDataframe = modifiedDataframe.withColumnRenamed("Trading date", "TradingDate")
    modifiedDataframe = modifiedDataframe.withColumnRenamed("Trading time", "TradingTime")

    # Selecting only interesting cols
    modifiedDataframe = modifiedDataframe.select("ID", "SecType", "Last", "TradingDate", "TradingTime")
    # Null Date removal
    modifiedDataframe = modifiedDataframe.where(modifiedDataframe.TradingDate.isNotNull())
    # Formatting Time Field
    modifiedDataframe = modifiedDataframe.withColumn("TradingTime", date_format("TradingTime", "HH:mm:ss.SSSS"))
    # Formatting Date Field
    modifiedDataframe = modifiedDataframe.withColumn(colName = "TradingDate", col = to_date("TradingDate", "dd-MM-yyyy"))
    # Removing rows with not valid time
    modifiedDataframe = modifiedDataframe.where(modifiedDataframe.TradingTime != "00:00:00.0000")
    # Removing duplicate rows: if there are rows with same key we mantain the avg of lasts
    modifiedDataframe = modifiedDataframe.groupBy(
        ["TradingDate", "TradingTime", "ID", "SecType"]
    ).agg(
        avg("Last")
    ).withColumnRenamed("avg(Last)", "Last")

    
    # Adding col TradingTimeHour to simplify queries
    modifiedDataframe = modifiedDataframe.withColumn("TradingTimeHour", concat(substring("TradingTime", 1, 2), lit(":00")))

    return modifiedDataframe