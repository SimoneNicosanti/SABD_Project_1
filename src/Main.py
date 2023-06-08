from controller import Controller
import os
import shutil
import sys

OUTPUT_DIR = "/Results"

## Arguments
## 1. Query Number : 0 for all - else 1, 2, 3
## 2. Evaluate : y/n
## 3. Output : y/n

def main() :

    setUpEnvironment()
    
    if (len(sys.argv) == 1) :
        Controller.controller()
    elif (len(sys.argv) == 2) :
        queryNumber = int(sys.argv[1])
        Controller.controller(queryNumber)
    else :
        print("Not Valid Input")
        return

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