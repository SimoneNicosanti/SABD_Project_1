from dao import HDFSDao
from controller import Preprocessor
from dao import SparkResultWriter
from dao import SparkSqlResultWriter
from dao import EvaluationWriter

from spark import Query_1, Query_2, Query_3
from spark_sql import SqlQuery_1, SqlQuery_2, SqlQuery_3

from pyspark.sql import *
from pyspark import *


def controller(queryNumber : int = 0, framework : int = 0, writeOutput : bool = True, writeEvaluation : bool = False) :
    
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
                executionTime = sparkController(rdd, i, writeOutput)
                if (writeEvaluation) :
                    EvaluationWriter.writeEvaluation(executionTime, "Query_" + str(i))
            elif (framework == 2) :
                executionTimeSql = sparkSqlController(dataFrame, i, writeOutput)
                if (writeEvaluation) :
                    EvaluationWriter.writeEvaluation(executionTimeSql, "SqlQuery_" + str(i))
            else :
                executionTime = sparkController(rdd, i, writeOutput)
                executionTimeSql = sparkSqlController(dataFrame, i, writeOutput)
                if (writeEvaluation) :
                    EvaluationWriter.writeEvaluation(executionTime, "Query_" + str(i))
                    EvaluationWriter.writeEvaluation(executionTimeSql, "SqlQuery_" + str(i), writeOutput)

            
    else :
        if (framework == 1) :
            sparkController(rdd, queryNumber, writeOutput)
        elif (framework == 2) :
            sparkSqlController(dataFrame, queryNumber, writeOutput)
        else :
            sparkController(rdd, queryNumber, writeOutput)
            sparkSqlController(dataFrame, queryNumber, writeOutput)
    

def sparkController(rdd : RDD, queryNumber : int, writeOutput : bool = True) : 

    if (queryNumber == 1) :
        (resultRDD, executionTime) = Query_1.query(rdd)
        
        header = ["Date", "Hour", "ID", "Min", "Mean", "Max", "Count"]
        fileName = "Query_1"
        parentDirectory = "/Results/spark"
        sortList = ["Date", "Hour", "ID"]
        ascendingList = None


    elif (queryNumber == 2) :
        (resultRDD, executionTime) = Query_2.query(rdd)

        header = ["Date", "ID", "Mean", "StdDev", "Count"]
        fileName = "Query_2"
        parentDirectory = "/Results/spark"
        sortList = ["Date", "Mean"]
        ascendingList = [True, False]

    elif (queryNumber == 3) :
        (resultRDD, executionTime) = Query_3.query(rdd)
        header = ["Date", "Country", "25_Perc", "50_Perc", "75_Perc", "Count"]
        fileName = "Query_3"
        parentDirectory = "/Results/spark"
        sortList = ["Date", "Country"]
        ascendingList = None

    if (writeOutput) :
        SparkResultWriter.writeRdd(
            resultRDD,
            header,
            fileName,
            parentDirectory,
            sortList,
            ascendingList
        )

    return executionTime


def sparkSqlController(dataFrame : DataFrame, queryNumber : int, writeOutput : bool) -> float :


    if (queryNumber == 1) :
        (resultDataFrame, executionTime) = SqlQuery_1.query(dataFrame)
        fileName = "Query_1"
        parentDirectory = "/Results/spark_sql"
        sortList = ["TradingDate", "TradingTimeHour", "ID"]
        ascendingList = None
        

    elif (queryNumber == 2) :
        (resultDataFrame, executionTime) = SqlQuery_2.query(dataFrame)

        fileName = "Query_2"
        parentDirectory = "/Results/spark_sql"
        sortList = ["TradingDate", "Avg"]
        ascendingList = [True, False]
        

    elif (queryNumber == 3) :
        (resultDataFrame, executionTime) = SqlQuery_3.query(dataFrame)

        fileName = "Query_2"
        parentDirectory = "/Results/spark_sql"
        sortList = ["TradingDate", "Country"]
        ascendingList = None

    if (writeOutput) :
    
        SparkSqlResultWriter.writeDataFrame(
            resultDataFrame,
            fileName,
            parentDirectory,
            sortList,
            ascendingList
        )

    return executionTime