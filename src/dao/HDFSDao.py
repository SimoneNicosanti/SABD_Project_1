import jproperties as jprop
from engineering import SparkSingleton
from pyspark.sql import *


def loadFromHdfs(fileName : str) -> DataFrame :
    baseHdfsUrl : str = loadHdfsUrl()
    fileUrl = baseHdfsUrl + "/" + fileName

    sparkSession = SparkSingleton.SparkSingleton.getInstance().getSparkSession()
    
    dataFrame = sparkSession.read.csv(fileUrl, inferSchema = True, header = True)

    return dataFrame


def loadToHdfs(fileName) :
    return 


def loadHdfsUrl() -> str :
    configs = jprop.Properties()

    with open("./properties/hdfs.properties", 'rb') as config_file:
        configs.load(config_file)

    masterHost = configs.get("hdfs.host").data
    masterPort = configs.get("hdfs.port").data

    return "hdfs://" + masterHost + ":" + masterPort