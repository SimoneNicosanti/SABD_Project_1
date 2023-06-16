from engineering import SparkSingleton
from dao import SparkSqlResultWriter
from dao import RedisDao

from pyspark.sql import *
from pyspark import *
from pyspark.sql.types import *

from engineering import SparkSingleton


def writeRdd(resultRDD : RDD, header : list, fileName : str, parentDirectory : str, sortList : list, ascendingList : list = None) -> None :
    sparkSession = SparkSingleton.getSparkSession()

    dataFrame = sparkSession.createDataFrame(resultRDD, schema = header)

    SparkSqlResultWriter.writeDataFrame(dataFrame, fileName, parentDirectory, sortList, ascendingList)
    RedisDao.putResult(dataFrame, fileName)

    return 
