from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from pyspark.sql.window import Window
from engineering import SparkSingleton


def query(dataFrame : DataFrame) -> tuple([DataFrame, float]) :

    # DataFrame of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    ## Looking for lats trade of each hour
    lastTradeSqlQuery = """
                        SELECT TradingDate, CAST(SUBSTRING(TradingTimeHour, 1, 2) AS INTEGER) as TradingHour, ID, TradingTime, Last
                        FROM Trade as T_1
                        WHERE NOT EXISTS(
                            SELECT *
                            FROM Trade as T_2
                            WHERE   T_1.TradingDate = T_2.TradingDate AND
                                    T_1.TradingTimeHour = T_2.TradingTimeHour AND
                                    T_1.ID = T_2.ID AND
                                    T_1.TradingTime < T_2.TradingTime
                            )
                        """
    lastTradeDataFrame = SparkSingleton.getSparkSession().sql(lastTradeSqlQuery)
    lastTradeDataFrame.createOrReplaceTempView("LastTrade")

    firstPriceSqlQuery = """
                        SELECT TradingDate, TradingHour + 1 as TradingHour, ID, Last
                        FROM LastTrade
                        """
    firstPriceDataFrame = SparkSingleton.getSparkSession().sql(firstPriceSqlQuery)
    firstPriceDataFrame.createOrReplaceTempView("FirstPrice")


    priceCouplesSqlQuery = """
                        SELECT FP_1.TradingDate, FP_1.ID, FP_1.TradingHour as PrevHour, FP_1.Last as PrevLast, FP_2.TradingHour as Hour, FP_2.Last as Last
                        FROM FirstPrice as FP_1 JOIN FirstPrice as FP_2
                            ON FP_1.TradingDate = FP_2.TradingDate AND FP_1.ID = FP_2.ID
                        WHERE FP_1.TradingHour < FP_2.TradingHour AND
                            NOT EXISTS (
                                SELECT *
                                FROM FirstPrice as FP_3
                                WHERE   FP_3.TradingDate = FP_1.TradingDate AND
                                        FP_3.ID = FP_1.ID AND
                                        FP_3.TradingHour < FP_2.TradingHour AND
                                        FP_3.TradingHour > FP_1.TradingHour
                            )
                        """
    
    priceCouplesDataFrame = SparkSingleton.getSparkSession().sql(priceCouplesSqlQuery)
    priceCouplesDataFrame.createOrReplaceTempView("PriceCouple")
    
    variationSqlQuery = """
                        SELECT TradingDate, ID, Last - PrevLast as Variation
                        FROM PriceCouple
                        """
    
    variationDataFrame = SparkSingleton.getSparkSession().sql(variationSqlQuery)
    variationDataFrame.createOrReplaceTempView("Variation")

    statisticsSqlQuery = """
                        SELECT TradingDate, ID, avg(Variation) as Mean, stddev_pop(Variation) StdDev, count(*) + 1 as Count
                        FROM Variation
                        GROUP BY TradingDate, ID
                        """
    statisticsDataFrame = SparkSingleton.getSparkSession().sql(statisticsSqlQuery)
    statisticsDataFrame.createOrReplaceTempView("Statistic")

    topFiveSqlQuery = """
                        SELECT TradingDate, ID, Mean, StdDev, Count
                        FROM (
                            SELECT TradingDate, ID, Mean, StdDev, Count, row_number() OVER (PARTITION BY TradingDate ORDER BY Mean DESC) as RowNum 
                            FROM Statistic
                        )
                        WHERE RowNum <= 5
                    """
    topFiveDataFrame = SparkSingleton.getSparkSession().sql(topFiveSqlQuery)
    topFiveDataFrame.createOrReplaceTempView("TopFive")

    worstFiveSqlQuery = """
                        SELECT TradingDate, ID, Mean, StdDev, Count
                        FROM (
                            SELECT TradingDate, ID, Mean, StdDev, Count, row_number() OVER (PARTITION BY TradingDate ORDER BY Mean ASC) as RowNum 
                            FROM Statistic
                        )
                        WHERE RowNum <= 5
                    """
    worstFiveDataFrame = SparkSingleton.getSparkSession().sql(worstFiveSqlQuery)
    worstFiveDataFrame.createOrReplaceTempView("WorstFive")

    resultSqlQuery = """
                        SELECT *
                        FROM TopFive 
                        
                        UNION 

                        SELECT *
                        FROM WorstFive
                    """
    
    resultDataFrame = SparkSingleton.getSparkSession().sql(resultSqlQuery)
    

    print("Collecting result of Second Query with SQL")
    start = time.time()
    resultDataFrame = resultDataFrame.persist()
    resultDataFrame.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultDataFrame, end - start)
