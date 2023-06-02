import jproperties as jprop
from engineering import SparkSingleton
from pyspark.sql import *


def loadFromHdfs(fileName : str) -> DataFrame :
    baseHdfsUrl : str = loadHdfsUrl()
    fileUrl = baseHdfsUrl + "/" + fileName

    sparkSession = SparkSingleton.SparkSingleton.getInstance().getSparkSession()
    
    dataFrame = sparkSession.read.csv(fileUrl, inferSchema = True, header = True)

    return dataFrame


def loadToHdfs(fileName : str, dataframe : DataFrame) :
    baseHdfsUrl : str = loadHdfsUrl()
    fileUrl = baseHdfsUrl + "/Results/" + fileName

    dataframe.write.csv(path = fileUrl, header = True, mode = "overwrite")

    return 


def loadHdfsUrl() -> str :
    configs = jprop.Properties()

    with open("./properties/hdfs.properties", 'rb') as config_file:
        configs.load(config_file)

    masterHost = configs.get("hdfs.host").data
    masterPort = configs.get("hdfs.port").data

    return "hdfs://" + masterHost + ":" + masterPort