from dao import FileSystemDao
from dao import HDFSDao
from dao import RedisDao

from pyspark.sql import *
from pyspark import *

def writeDataFrame(dataFrame : DataFrame, fileName : str, parentDirectory : str, sortList : list, ascendingList : list = None) :

    dataFrame = dataFrame.sort(sortList)

    if (ascendingList == None) :
        dataFrame = dataFrame.sort(sortList).coalesce(1)
    else :
        dataFrame = dataFrame.sort(
            sortList, 
            ascending = ascendingList
        ).coalesce(1)

    FileSystemDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)
    HDFSDao.writeDataFrameAsCsv(dataFrame, fileName, parentDirectory)