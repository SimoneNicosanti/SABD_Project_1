from dao import HDFSDao
from controller import Preprocessor
from dao import SparkResultWriter

from spark import Query_1, Query_2, Query_3
from spark_sql import SqlQuery_2

from pyspark.sql import *
from pyspark import *



## from functools import partial
# To use custom functions


def controller() :
    
    dataFrame : DataFrame = HDFSDao.loadFromHdfs("Dataset.csv")
    

    dataFrame = Preprocessor.prepareForProcessing(dataFrame)

    dataFrame = dataFrame.persist()
    rdd = dataFrame.rdd.map(tuple)
    rdd = rdd.persist()

    print(dataFrame.schema.names)

    dataFrame.count()
    rdd.count()

    sparkController(rdd)
    #sparkSqlController(dataFrame)


def sparkController(rdd : RDD) :
    #print(rdd.collect()[0])
    # (resultList_1, executionTime_1) = Query_1.query(rdd)
    # SparkResultWriter.writeRdd(
    #     resultList = resultList_1, 
    #     header = ["Date", "Hour", "ID", "Min", "Mean", "Max", "Count"], 
    #     fileName = "Query_1", 
    #     parentDirectory = "/Results/spark",
    #     sortList = ["Date", "Hour", "ID"])


    # (resultList_2, executionTime_2) = Query_2.query(rdd)
    # SparkResultWriter.writeRdd(
    #     resultList_2,
    #     ["Date", "ID", "Mean", "StdDev", "Count"],
    #     "Query_2",
    #     "/Results/spark",
    #     ["Date", "ID"]
    # )

    (resultList_3, executionTime_3) = Query_3.query(rdd)
    SparkResultWriter.writeRdd(
        resultList_3,
        ["Date", "Country", "25_Perc", "50_Perc", "75_Perc"],
        "Query_3",
        "/Results/spark",
        ["Date", "Country"]
    )

    return


def sparkSqlController(dataFrame : DataFrame) :
    #SqlQuery_2.query(dataFrame)
    return