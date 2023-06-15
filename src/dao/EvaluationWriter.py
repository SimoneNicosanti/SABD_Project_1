import csv
import os
from engineering import SparkSingleton

EVALUATION_PATH = "/Results/evaluation/evaluations.csv"

def writeEvaluation(evaluationTime : float, query : str, run : int = 1) :

    sparkContext = SparkSingleton.getSparkContext()
    workerNodeNum = sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1

    if (not os.path.exists(EVALUATION_PATH)) :
        with open(EVALUATION_PATH, "+x") as outputFile :
            writer = csv.writer(outputFile)
            writer.writerow(["Query", "WorkerNodeNum", "Time"])
    
    
    with open(EVALUATION_PATH, "a") as outputFile :
        writer = csv.writer(outputFile)
        writer.writerow([query, workerNodeNum, evaluationTime])