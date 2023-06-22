from controller import Controller, Preprocessor
import os
import sys
from engineering import SparkSingleton


RUN_NUMBER = 10

## Arguments
## 1. Query Number (1,2,3 ; else all)
## 2. Framework: 1 == Spark, 2 == SparkSql, else == both
## 3. Write output: 0 = True, else = False
## 4. NO Arguments == python3 0 0 1 with time evaluation writing


def main() :

    Preprocessor.prepareForProcessing()

    setUpEnvironment()

    if (len(sys.argv) == 1) :
        ## For Time Evaluation Only
        for runNum in range(0, RUN_NUMBER) :
            print("Run Number >>> ", runNum)
            for queryNum in range(1, 4) :
                for framework in range(1 , 3) :
                    Controller.controller(queryNum, framework, False, True)
                    SparkSingleton.resetConnection()

    elif (len(sys.argv) == 4) :
        queryNumber = int(sys.argv[1])
        framework = int(sys.argv[2])
        writeOutput = int(sys.argv[3]) == 0

        Controller.controller(queryNumber, framework, writeOutput)
    
    else :
        ## For every other configuration
        print("ERROR: WRONG PARAMETERS NUMBER")
        
    
    input("Press Enter to Exit >>>")


def setUpEnvironment() :

    if (not os.path.isdir("/Results")) :
        os.mkdir("/Results", 0o777)
    if (not os.path.isdir("/Results/spark")) :
        os.mkdir("/Results/spark", 0o777)
    if (not os.path.isdir("/Results/spark_sql")) :
        os.mkdir("/Results/spark_sql", 0o777)
    if (not os.path.isdir("/Results/evaluation")) :
        os.mkdir("/Results/evaluation", 0o777)
    
    
if __name__ == "__main__" :
    main()