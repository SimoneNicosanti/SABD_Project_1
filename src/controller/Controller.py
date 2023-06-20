from dao import HDFSDao
from controller import Preprocessor
from dao import SparkResultWriter
from dao import SparkSqlResultWriter
from dao import EvaluationWriter

from spark import Query_1, Query_2, Query_3, Query_2_variant
from spark_sql import SqlQuery_1, SqlQuery_2, SqlQuery_3

from pyspark.sql import *
from pyspark import *


def controller(queryNumber : int = 0, framework : int = 0, writeOutput : bool = True, writeEvaluation : bool = False) :
    
    dataFrame : DataFrame = HDFSDao.loadFromHdfs("Dataset.csv")
    
    dataFrame = Preprocessor.prepareForProcessing(dataFrame)

    dataFrame = dataFrame.persist()
    rdd = dataFrame.rdd.map(tuple)
    rdd = rdd.persist()

    ## To force persist
    dataFrame.count()
    rdd.count()

    dataFrame.createOrReplaceTempView("Trade")

    if (queryNumber == 0) :
        startIndex = 1
        endIndex = 4

    else :
        startIndex = queryNumber
        endIndex = queryNumber + 1

    
    for i in range(startIndex, endIndex) :
        if (framework == 1) :
            sparkController(rdd, i, writeOutput, writeEvaluation)
        elif (framework == 2) :
            sparkSqlController(dataFrame, i, writeOutput, writeEvaluation) 
        else :
            sparkController(rdd, i, writeOutput, writeEvaluation)
            sparkSqlController(dataFrame, i, writeOutput, writeEvaluation)



def sparkController(rdd : RDD, queryNumber : int, writeOutput : bool = True, writeEvaluation : bool = False) : 

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

    if (writeEvaluation) :
        EvaluationWriter.writeEvaluation(
            executionTime = executionTime,
            queryNum = queryNumber,
            dataStructure = "RDD"
        )

    if (writeEvaluation and queryNumber == 2) :
        (resultRDD, variantExecutionTime) = Query_2_variant.query(rdd)
        EvaluationWriter.writeEvaluation(
            executionTime = variantExecutionTime,
            queryNum = queryNumber,
            dataStructure = "RDD",
            queryVariant = 2
        )
        
    return


def sparkSqlController(dataFrame : DataFrame, queryNumber : int, writeOutput : bool, writeEvaluation : bool = False) -> float :


    if (queryNumber == 1) :
        (resultDataFrame, executionTime) = SqlQuery_1.query(dataFrame)
        fileName = "SqlQuery_1"
        parentDirectory = "/Results/spark_sql"
        sortList = ["TradingDate", "TradingTimeHour", "ID"]
        ascendingList = None
        

    elif (queryNumber == 2) :
        (resultDataFrame, executionTime) = SqlQuery_2.query(dataFrame)

        fileName = "SqlQuery_2"
        parentDirectory = "/Results/spark_sql"
        sortList = ["TradingDate", "Mean"]
        ascendingList = [True, False]
        

    elif (queryNumber == 3) :
        (resultDataFrame, executionTime) = SqlQuery_3.query(dataFrame)

        fileName = "SqlQuery_3"
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

    if (writeEvaluation) :
        EvaluationWriter.writeEvaluation(
            executionTime = executionTime,
            queryNum = queryNumber,
            dataStructure = "DataFrame"
        )

    return executionTime