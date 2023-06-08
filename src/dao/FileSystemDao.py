import csv
import os
from pyspark.sql import *


def writeDataFrameAsCsv(dataFrame : DataFrame, fileName : str, parentPath : str) -> None :

    header = dataFrame.schema.names
    resultList = dataFrame.rdd.collect()

    filePath = os.path.join(parentPath, fileName + ".csv")
    if (os.path.exists(filePath)) :
        mode = "+w"
    else :
        mode = "+x"

    with open(os.path.join(parentPath, fileName + ".csv"), mode) as outputFile :
        writer = csv.writer(outputFile)
        writer.writerow(header)
        writer.writerows(resultList)

    return
