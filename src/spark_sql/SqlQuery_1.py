from pyspark.sql import *
from pyspark.sql.functions import *
import time
from engineering import SparkSingleton


def query(dataframe : DataFrame) -> tuple([DataFrame, float]) :

    # DataFrame of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

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