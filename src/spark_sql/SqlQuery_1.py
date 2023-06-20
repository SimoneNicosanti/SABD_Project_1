from pyspark.sql import *
from pyspark.sql.functions import *
import time
from engineering import SparkSingleton


def query(dataframe : DataFrame) -> tuple([DataFrame, float]) :
    # resultDataFrame = dataframe.select(
    #     "TradingDate", "TradingTimeHour", "ID", "Last"
    # ).where(
    #     dataframe.SecType == "E"
    # ).where(
    #     dataframe.ID.endswith(".FR")
    # ).groupBy(
    #     "TradingDate", "TradingTimeHour", "ID"
    # ).agg(
    #     min("Last"), avg("Last"), max("Last"), count(expr("*"))
    # ).withColumnsRenamed(
    #     {"min(Last)" : "Min", "avg(Last)" : "Avg", "max(Last)" : "Max", "count(1)" : "Count"}
    # )

    sqlQuery : str = """
                        SELECT TradingDate, TradingTimeHour, ID, min(Last) as Min, avg(Last) as Avg, max(Last) as Max, count(*) as Count
                        FROM Trade
                        WHERE SecType = "E" AND ID LIKE "%.FR"
                        GROUP BY TradingDate, TradingTimeHour, ID
                     """
    
    resultDataFrame = SparkSingleton.getSparkSession().sql(sqlQuery)

    
    print("Collecting result of First Query with SQL")
    start = time.time()
    resultDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)


    return (resultDataFrame, end - start)