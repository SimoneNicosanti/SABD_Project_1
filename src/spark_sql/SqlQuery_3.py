from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from engineering import SparkSingleton


def query(dataFrame : DataFrame) -> tuple([DataFrame, float]) :

    # DataFrame of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    timesSqlQuery = """
                    SELECT TradingDate, ID, MinTime, MaxTime
                    FROM (
                        SELECT TradingDate, ID, min(TradingTime) as MinTime, max(TradingTime) as MaxTime
                        FROM Trade
                        GROUP BY TradingDate, ID
                        )
                    WHERE MinTime <> MaxTime
                    """
    timeDataFrame = SparkSingleton.getSparkSession().sql(timesSqlQuery)
    timeDataFrame.createOrReplaceTempView("ExtremeTime")


    couplesSqlQuery = """
                        SELECT T_1.TradingDate, T_1.ID, T_1.Last as FirstPrice, T_2.Last as LastPrice, 2 as Count
                        FROM Trade as T_1 JOIN
                            ExtremeTime as ET on T_1.TradingDate = ET.TradingDate AND T_1.ID = ET.ID AND T_1.TradingTime = ET.MinTime JOIN
                            Trade as T_2 ON T_2.TradingDate = ET.TradingDate AND T_2.ID = ET.ID AND T_2.TradingTime = ET.MaxTime
                    """
    
    couplesDataFrame = SparkSingleton.getSparkSession().sql(couplesSqlQuery)
    couplesDataFrame.createOrReplaceTempView("FirstAndLastPriceCouple")


    variationSqlQuery = """
                        SELECT TradingDate, SUBSTRING_INDEX(ID, ".", -1) as Country, LastPrice - FirstPrice as Variation, Count
                        FROM FirstAndLastPriceCouple
                        """
    
    variationDataFrame = SparkSingleton.getSparkSession().sql(variationSqlQuery)
    variationDataFrame.createOrReplaceTempView("DailyVariation")

    resultSqlQuery = """
                        SELECT TradingDate, Country, APPROX_PERCENTILE(Variation, 0.25) as 25_Perc, APPROX_PERCENTILE(Variation, 0.5) as 50_Perc, APPROX_PERCENTILE(Variation, 0.75) as 75_Perc, sum(Count) as Count
                        FROM DailyVariation
                        GROUP BY TradingDate, Country
                    """
    
    resultDataFrame = SparkSingleton.getSparkSession().sql(resultSqlQuery)


    print("Collecting Result of Third Query with SQL")
    start = time.time()
    resultDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultDataFrame, end - start)