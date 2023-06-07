import jproperties as jprop
from pyspark import *
from pyspark.sql import *



class SparkSingleton(object) :

    __instance = None

    __sparkContext = None
    __sparkSession = None
    
    @classmethod
    def getInstance(cls) :
        if (cls.__instance == None) :
            cls.__instance = super().__new__(cls)

            configs = jprop.Properties()

            with open("./properties/spark.properties", 'rb') as config_file:
                configs.load(config_file)

            masterName = configs.get("spark.master")
            appName = configs.get("spark.appName")
            masterPort = configs.get("spark.port")

            sparkConf = SparkConf()

            sparkMasterUrl = "spark://" + masterName.data + ":" + masterPort.data
            
            cls.__sparkContext = SparkContext(
                master = sparkMasterUrl , 
                appName = appName.data, 
            )

            cls.__sparkSession = SparkSession(sparkContext = cls.__sparkContext)

        return cls.__instance


    def getSparkSession(self) :
        return self.__sparkSession
    
    def getSparkContext(self) :
        return self.__sparkContext
