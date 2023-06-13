from dao import HDFSDao
from controller import Preprocessor
from dao import SparkResultWriter
from dao import SparkSqlResultWriter
from dao import EvaluationWriter

from spark import Query_1, Query_2, Query_3
from spark_sql import SqlQuery_1, SqlQuery_2, SqlQuery_3

from pyspark.sql import *
from pyspark import *


def controller(queryNumber : int = 0, framework : int = 0) :
    
    dataFrame : DataFrame = HDFSDao.loadFromHdfs("Dataset.csv")
    
    dataFrame = Preprocessor.prepareForProcessing(dataFrame)

    dataFrame = dataFrame.persist()
    rdd = dataFrame.rdd.map(tuple)
    rdd = rdd.persist()

    print(dataFrame.schema.names)

    dataFrame.count()
    rdd.count()

    if (queryNumber == 0) :
        for i in range(1, 4) :
            if (framework == 1) :
                executionTime = sparkController(rdd, i)
                EvaluationWriter.writeEvaluation(executionTime, "Query_" + str(i))
            elif (framework == 2) :
                executionTimeSql = sparkSqlController(dataFrame, i)
                EvaluationWriter.writeEvaluation(executionTimeSql, "SqlQuery_" + str(i))
            else :
                executionTime = sparkController(rdd, i)
                EvaluationWriter.writeEvaluation(executionTime, "Query_" + str(i))
                executionTimeSql = sparkSqlController(dataFrame, i)
                EvaluationWriter.writeEvaluation(executionTimeSql, "SqlQuery_" + str(i))

            
    else :
        if (framework == 1) :
            sparkController(rdd, queryNumber)
        elif (framework == 2) :
            sparkSqlController(dataFrame, queryNumber)
        else :
            sparkController(rdd, queryNumber)
            sparkSqlController(dataFrame, queryNumber)
    

def sparkController(rdd : RDD, queryNumber : int) : 

    if (queryNumber == 1) :
        (resultList_1, executionTime_1) = Query_1.query(rdd)
        SparkResultWriter.writeRdd(
            resultList = resultList_1, 
            header = ["Date", "Hour", "ID", "Min", "Mean", "Max", "Count"], 
            fileName = "Query_1", 
            parentDirectory = "/Results/spark",
            sortList = ["Date", "Hour", "ID"]
        )

        return executionTime_1

    elif (queryNumber == 2) :
        (resultList_2, executionTime_2) = Query_2.query(rdd)
        SparkResultWriter.writeRdd(
            resultList_2,
            ["Date", "ID", "Mean", "StdDev", "Count"],
            "Query_2",
            "/Results/spark",
            ["Date", "Mean"]
        )

        return executionTime_2

    elif (queryNumber == 3) :
        (resultList_3, executionTime_3) = Query_3.query(rdd)
        SparkResultWriter.writeRdd(
            resultList_3,
            ["Date", "Country", "25_Perc", "50_Perc", "75_Perc", "Count"],
            "Query_3",
            "/Results/spark",
            ["Date", "Country"]
        )
        return executionTime_3

    return 0


def sparkSqlController(dataFrame : DataFrame, queryNumber : int) :

    if (queryNumber == 1) :
        (result_1, executionTime_1) = SqlQuery_1.query(dataFrame)
        SparkSqlResultWriter.writeDataFrame(
            result_1,
            "Query_1",
            "/Results/spark_sql",
            ["TradingDate", "TradingTimeHour", "ID"]
        )
        return executionTime_1

    elif (queryNumber == 2) :
        (result_2, executionTime_2) = SqlQuery_2.query(dataFrame)
        SparkSqlResultWriter.writeDataFrame(
            result_2, 
            "Query_2", 
            "/Results/spark_sql", 
            ["TradingDate", "ID"]
        )
        return executionTime_2

    elif (queryNumber == 3) :
        (result_3, executionTime_3) = SqlQuery_3.query(dataFrame)
        SparkSqlResultWriter.writeDataFrame(
            result_3,
            "Query_3",
            "/Results/spark_sql",
            ["TradingDate", "Country"]
        )
        return executionTime_3

    return 0