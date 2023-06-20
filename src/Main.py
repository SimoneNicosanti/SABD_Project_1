from controller import Controller
import os
import sys
from engineering import SparkSingleton


RUN_NUMBER = 5

## Arguments
## 1. Query Number (1,2,3 ; else all)
## 2. Framework: 1 == Spark, 2 == SparkSql, else == both
## 3. Write output: 0 = True, else = False
## 4. NO Arguments == python3 0 0 1 with time evaluation writing


def main() :

    setUpEnvironment()

    if (len(sys.argv) == 1) :
        ## For Time Evaluation Only
        for i in range(0, RUN_NUMBER) :
            print("Run Number >>> ", i)
            Controller.controller(0, 0, False, True)
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