from controller import Controller
import os
import shutil

OUTPUT_DIR = "/Results"

def main() :
    setUpEnvironment()
    Controller.controller()


def setUpEnvironment() :

    for files in os.listdir("/Results"):
        path = os.path.join("/Results", files)
        try:
            shutil.rmtree(path)
        except OSError:
            os.remove(path)

    os.mkdir("/Results/spark")
    os.chmod("/Results/spark", 0o777)

    os.mkdir("/Results/spark_sql")
    os.chmod("/Results/spark_sql", 0o777)
    
    
if __name__ == "__main__" :
    main()