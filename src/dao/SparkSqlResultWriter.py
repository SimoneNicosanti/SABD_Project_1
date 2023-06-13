from dao import FileSystemDao
from dao import HDFSDao
from dao import RedisDao

from pyspark.sql import *
from pyspark import *

def writeDataFrame(dataFrame : DataFrame, fileName : str, parentDirectory : str, sortList : list) :

    dataFrame = dataFrame.sort(sortList)

    FileSystemDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)
    HDFSDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)