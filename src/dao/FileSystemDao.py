import csv
import os
from pyspark.sql import *


def writeDataFrameAsCsv(dataFrame : DataFrame, fileName : str, parentPath : str) -> None :

    header = dataFrame.schema.names
    resultList = dataFrame.rdd.collect()

    with open(os.path.join(parentPath, fileName + ".csv"), "+x") as outputFile :
        writer = csv.writer(outputFile)
        writer.writerow(header)
        writer.writerows(resultList)

    return
