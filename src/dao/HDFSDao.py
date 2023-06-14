import jproperties as jprop
from engineering import SparkSingleton
from pyspark.sql import *
import os


def loadFromHdfs(fileName : str) -> DataFrame :
    baseHdfsUrl : str = loadHdfsUrl()
    fileUrl = baseHdfsUrl + "/" + fileName

    sparkSession = SparkSingleton.getSparkSession()
    
    dataFrame = sparkSession.read.text(fileUrl)

    return dataFrame


def writeDataFrameAsCsv(dataFrame : DataFrame, fileName : str, parentPath : str) :
    
    filePath = os.path.join(parentPath, fileName)

    hdfsUrl = loadHdfsUrl()

    dataFrame = dataFrame.alias("Query_1")
    dataFrame.write.csv(
        path = hdfsUrl + parentPath + "/" + fileName,
        mode = "overwrite",
        header = True
    )

    return 


def loadHdfsUrl() -> str :
    configs = jprop.Properties()

    with open("./properties/hdfs.properties", 'rb') as config_file:
        configs.load(config_file)

    masterHost = configs.get("hdfs.host").data
    masterPort = configs.get("hdfs.port").data

    return "hdfs://" + masterHost + ":" + masterPort