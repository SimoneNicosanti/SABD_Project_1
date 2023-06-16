from controller import Controller
import os
import shutil
import sys
from engineering import RedisSingleton
import redis
from engineering import SparkSingleton

OUTPUT_DIR = "/Results"
RUN_NUMBER = 5

## Arguments
## 1. Query Number (1,2,3 ; else all)
## 2. Framework: 1 == Spark, 2 == SparkSql, else == both
## 3. Write output: 0 = True, else = False


def main() :

    setUpEnvironment()

    if (len(sys.argv) == 1) :
        for i in range(0, RUN_NUMBER) :
            print("Run Number >>> ", i)
            Controller.controller(0, 0, False, True)
            SparkSingleton.resetConnection()

    if (len(sys.argv) != 4) :
        print("ERROR: WRONG PARAMETERS NUMBER")
        return
    
    queryNumber = int(sys.argv[1])
    framework = int(sys.argv[2])
    writeOutput = int(sys.argv[3]) == 0

    Controller.controller(queryNumber, framework, writeOutput)
    
    input("Press Enter to Exit >>>")


def setUpEnvironment() :

    if (not os.path.isdir("/Results/spark")) :
        os.mkdir("/Results/spark")
    if (not os.path.isdir("/Results/spark_sql")) :
        os.mkdir("/Results/spark_sql")
    if (not os.path.isdir("/Results/evaluation")) :
        os.mkdir("/Results/evaluation")
    
    
if __name__ == "__main__" :
    main()