from dao import HDFSDao
from model import DataFrameFilter
from pyspark.sql import *

def controller() :
    
    dataFrame : DataFrame = HDFSDao.loadFromHdfs("Dataset.csv")
    dataFrame = dataFrame.withColumnRenamed("Trading date", "TradingDate")
    dataFrame = dataFrame.withColumnRenamed("Trading time", "TradingTime")

    dataFrame.select()
    
    dataFrame = dropColumnsFromDataframe(dataFrame)
    dataFrame = DataFrameFilter.setUpDataFrame(dataFrame)

    print(dataFrame.dtypes)

    
    dataFrame.show(n = 1000)


def dropColumnsFromDataframe(dataFrame : DataFrame) -> DataFrame:
    return dataFrame.select("ID", "SecType", "Last", "TradingDate", "TradingTime")