import csv
import os
from engineering import SparkSingleton

EVALUATION_PATH = "/Results/evaluation/evaluations.csv"

def writeEvaluation(executionTime : float, queryNum : int, dataStructure : str, queryVariant : int = 1) :

    sparkContext = SparkSingleton.getSparkContext()
    workerNodeNum = sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1

    if (not os.path.exists(EVALUATION_PATH)) :
        with open(EVALUATION_PATH, "+x") as outputFile :
            writer = csv.writer(outputFile)
            writer.writerow(["QueryNum", "QueryVariant", "DataStructure", "WorkerNodeNum", "Time"])
    
    
    with open(EVALUATION_PATH, "a") as outputFile :
        writer = csv.writer(outputFile)
        writer.writerow([queryNum, queryVariant, dataStructure, workerNodeNum, executionTime])