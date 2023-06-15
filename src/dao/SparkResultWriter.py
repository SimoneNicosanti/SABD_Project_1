from dao import FileSystemDao
from dao import HDFSDao
from dao import RedisDao
from engineering import SparkSingleton

from pyspark.sql import *
from pyspark import *
from pyspark.sql.types import *

from engineering import SparkSingleton


def writeRdd(resultList : list, header : list, fileName : str, parentDirectory : str, sortList : list, ascendingList : list = None) -> None :
    sparkSession = SparkSingleton.getSparkSession()

    dataFrame = sparkSession.createDataFrame(convertRddResultList(resultList), schema = header)
    if (ascendingList == None) :
        dataFrame = dataFrame.sort(sortList).coalesce(1)
    else :
        dataFrame = dataFrame.sort(
            sortList, 
            ascending = ascendingList)

    FileSystemDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)
    HDFSDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)
    # RedisDao.putResult(resultList, fileName)

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