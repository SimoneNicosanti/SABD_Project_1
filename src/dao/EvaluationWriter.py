import csv
import os

EVALUATION_PATH = "/Results/evaluation/evaluations.csv"

def writeEvaluation(evaluationTime : float, query : str, run : int = 1) :

    if (not os.path.exists(EVALUATION_PATH)) :
        with open(EVALUATION_PATH, "+x") as outputFile :
            writer = csv.writer(outputFile)
            writer.writerow(["Query", "Run", "Time"])
    
    
    with open(EVALUATION_PATH, "a") as outputFile :
        writer = csv.writer(outputFile)
        writer.writerow([query, run, evaluationTime])