import jproperties as jprop
from pyspark import *
from pyspark.sql import *


__sparkContext = None
__sparkSession = None


def getSparkContext() -> SparkContext :
    global __sparkContext
    if (__sparkContext == None) :
        configs = jprop.Properties()

        with open("./properties/spark.properties", 'rb') as config_file:
            configs.load(config_file)

            masterName = configs.get("spark.master").data
            appName = configs.get("spark.appName").data
            masterPort = configs.get("spark.port").data

            sparkMasterUrl = "spark://" + masterName + ":" + masterPort
            
            __sparkContext = SparkContext(
                master = sparkMasterUrl , 
                appName = appName, 
            )
    return __sparkContext


def getSparkSession() -> SparkSession :
    global __sparkSession
    if (__sparkSession == None) :
        sparkContext = getSparkContext()
        __sparkSession = SparkSession(sparkContext = sparkContext)
    return __sparkSession


def resetConnection() :
    global __sparkContext
    global __sparkSession

    if (__sparkSession != None) :
        __sparkSession.stop()
        __sparkSession = None

    if (__sparkContext != None) :
        __sparkContext.stop()
        __sparkContext = None