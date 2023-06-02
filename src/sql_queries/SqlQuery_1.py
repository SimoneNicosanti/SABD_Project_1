from pyspark.sql import *
from pyspark.sql.functions import *


def query(dataframe : DataFrame) -> DataFrame :
    answer = dataframe.select(
        "TradingDate", "TradingTimeHour", "ID", "Last"
        ).where(
            dataframe.SecType == "E"
        ).where(
            dataframe.ID.endswith(".FR")
        ).groupBy(
            "TradingDate", "TradingTimeHour", "ID"
        ).agg(
            min("Last"), avg("Last"), max("Last"), count(expr("*"))
        ).sort(
            "ID", "TradingDate", "TradingTimeHour")

    return answer