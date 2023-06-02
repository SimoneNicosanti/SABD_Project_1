from dao import HDFSDao
from dao import FileSystemDAO
from model import DataFrameFilter
from pyspark.sql import *
from pyspark import *

import queries.Query_1
from sql_queries import SqlQuery_1


def dropColumnsFromDataframe(dataFrame : DataFrame) -> DataFrame:
    return dataFrame.select("ID", "SecType", "Last", "TradingDate", "TradingTime")

def controller() :
    dataFrame : DataFrame = HDFSDao.loadFromHdfs("Dataset.csv")
    dataFrame = dataFrame.withColumnRenamed("Trading date", "TradingDate")
    dataFrame = dataFrame.withColumnRenamed("Trading time", "TradingTime")
    
    dataFrame = dropColumnsFromDataframe(dataFrame)

    dataFrame = DataFrameFilter.setUpDataFrame(dataFrame)

    dataFrame.show()

    dataFrame.persist()

    rdd = dataFrame.rdd.map(tuple)
    rdd.persist()

    sparkController(rdd)
    sparkSqlController(dataFrame)


def sparkController(rdd : RDD) :
    print(rdd.collect()[0])
    queries.Query_1.query(rdd)
    return


def sparkSqlController(dataFrame : DataFrame) :
    SqlQuery_1.query(dataFrame)
    return