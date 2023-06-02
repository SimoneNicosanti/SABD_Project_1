from pyspark.sql import *


def writeDataFrame(dataFrame : DataFrame, fileName : str) :
    
    dataFrame.write.csv("/Results/" + fileName, header = True, mode = "overwrite")
    
    return



def getFilePath(fileName : str) :
    return '/Results/' + fileName