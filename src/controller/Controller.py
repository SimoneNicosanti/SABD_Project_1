from dao import HDFSDao
from model import DataFrameFilter
from model import ResultConverter
from dao import SparkResultWriter

from spark import Query_1, Query_2
from spark_sql import SqlQuery_2

from pyspark.sql import *
from pyspark import *



## from functools import partial
# To use custom functions


def dropColumnsFromDataframe(dataFrame : DataFrame) -> DataFrame:
    return dataFrame.select("ID", "SecType", "Last", "TradingDate", "TradingTime")

def controller() :
    
    dataFrame : DataFrame = HDFSDao.loadFromHdfs("Dataset.csv")
    dataFrame = dataFrame.withColumnRenamed("Trading date", "TradingDate")
    dataFrame = dataFrame.withColumnRenamed("Trading time", "TradingTime")
    
    dataFrame = dropColumnsFromDataframe(dataFrame)

    dataFrame = DataFrameFilter.setUpDataFrame(dataFrame)

    #dataFrame.show()

    dataFrame = dataFrame.persist()
    rdd = dataFrame.rdd.map(tuple)
    rdd = rdd.persist()

    sparkController(rdd)
    sparkSqlController(dataFrame)


def sparkController(rdd : RDD) :
    #print(rdd.collect()[0])
    (resultList, executionTime) = Query_1.query(rdd)
    SparkResultWriter.writeRdd(
        resultList = resultList, 
        header = ["Date", "Hour", "ID", "Min", "Mean", "Max", "Count"], 
        fileName = "Query_1", 
        parentDirectory = "/Results/spark",
        sortList = ["Date", "Hour", "ID"])


    #Query_2.query(rdd)
    return


def sparkSqlController(dataFrame : DataFrame) :
    #SqlQuery_2.query(dataFrame)
    return