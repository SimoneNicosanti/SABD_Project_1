from controller import Controller
import os
import shutil
import sys
from engineering import RedisSingleton
import redis
from engineering import SparkSingleton

OUTPUT_DIR = "/Results"
RUN_NUMBER = 10

## Arguments
## 1. Query Number
## 2. Framework: 1 == Spark, 2 == SparkSql, else == both

def main() :

    setUpEnvironment()
    
    if (len(sys.argv) == 1) :
        Controller.controller()
        # for i in range(0, RUN_NUMBER) :
        #     Controller.controller()

    elif (len(sys.argv) == 2) :
        queryNumber = int(sys.argv[1])
        Controller.controller(queryNumber)

    elif (len(sys.argv) == 3) :
        queryNumber = int(sys.argv[1])
        framework = int(sys.argv[2])
        Controller.controller(queryNumber, framework)
    else :
        print("Not Valid Input")
        return

    SparkSingleton.resetConnection()

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