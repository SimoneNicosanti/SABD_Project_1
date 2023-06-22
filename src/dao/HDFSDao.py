import jproperties as jprop
from engineering import SparkSingleton
from pyspark.sql import *
import os


def loadFromHdfs(fileName : str) -> DataFrame :
    baseHdfsUrl : str = getHdfsUrl()
    fileUrl = baseHdfsUrl + "/" + fileName

    sparkSession = SparkSingleton.getSparkSession()
    
    dataFrame = sparkSession.read.text(fileUrl)

    return dataFrame


def writeDataFrameAsCsv(dataFrame : DataFrame, fileName : str, parentPath : str) :

    hdfsUrl = getHdfsUrl()

    dataFrame.write.csv(
        path = hdfsUrl + parentPath + "/" + fileName,
        mode = "overwrite",
        header = True
    )

    return 


def writeDafaFrameAsParquet(dataFrame : DataFrame, name : str) :
    hdfsUrl = getHdfsUrl()
    dataFrame.write.parquet(
        path = hdfsUrl + "/" + name,
        mode = "overwrite"
    )

def readParquet(name : str) :
    hdfsUrl = getHdfsUrl()
    dataFrame = SparkSingleton.getSparkSession().read.parquet(hdfsUrl + "/" + name)

    return dataFrame


def getHdfsUrl() -> str :
    configs = jprop.Properties()

    with open("./properties/hdfs.properties", 'rb') as config_file:
        configs.load(config_file)

    masterHost = configs.get("hdfs.host").data
    masterPort = configs.get("hdfs.port").data

    return "hdfs://" + masterHost + ":" + masterPort