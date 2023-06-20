import csv
import os
from pyspark.sql import *


def writeDataFrameAsCsv(dataFrame : DataFrame, fileName : str, parentPath : str) -> None :

    header = dataFrame.schema.names
    resultList = dataFrame.rdd.collect()

    filePath = os.path.join(parentPath, fileName + ".csv")
    if (not os.path.exists(filePath)) :
        ## Create file and change access mode for it
        with open(filePath, "+x") as outputFile :
            None
        os.chmod(filePath, 0o777)
        

    with open(filePath, "+w") as outputFile :
        writer = csv.writer(outputFile)
        writer.writerow(header)
        writer.writerows(resultList)

    return
