from dao import FileSystemDao
from dao import HDFSDao
from dao import RedisDao

from pyspark.sql import *
from pyspark import *

from engineering import SparkSingleton

def writeRdd(resultList : list, header : list, fileName : str, parentDirectory : str, sortList : list) -> None :
    
    sparkSession = SparkSingleton.SparkSingleton.getInstance().getSparkSession()

    dataFrame = sparkSession.createDataFrame(convertRddResultList(resultList), schema = header)
    dataFrame = dataFrame.sort(sortList).coalesce(1)

    FileSystemDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)
    HDFSDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)
    # TODO ADD REDIS WRITE

    return 


def buildDataFrameFromRddResult(resultList : RDD, header : str) -> DataFrame:

    return 


def convertRddResultList(resultList : list) -> list :
    resultMatrix = []
    for row in resultList :
        key = row[0]
        value = row[1]
        rowList = []

        for keyElem in key :
            rowList.append(keyElem)

        for valueElem in value :
            rowList.append(valueElem)

        resultMatrix.append(rowList)

    return resultMatrix