from dao import HDFSDao
from dao import FileSystemDao
from model import DataFrameFilter

from queries import Query_1, Query_2
from sql_queries import SqlQuery_2

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
    Query_1.query(rdd)
    #Query_2.query(rdd)
    return


def sparkSqlController(dataFrame : DataFrame) :
    #SqlQuery_2.query(dataFrame)
    return