import pandas as pd
import matplotlib.pyplot as plt


def main() :
    dataset = pd.read_csv("../Results/evaluation/evaluations.csv")


    for query in dataset["QueryNum"].drop_duplicates():
        
        axes = plt.subplot()
        
        
        for dataStructure in dataset["DataStructure"].drop_duplicates() :
            workerList = dataset["WorkerNodeNum"].drop_duplicates().sort_values()
            
            variantList = dataset[
                (dataset["QueryNum"] == query) &
                (dataset["DataStructure"] == dataStructure)
            ]["QueryVariant"].drop_duplicates()

            for variant in variantList :
                avgList = []
                stdDevList = []

                for workerNum in workerList :
                    timesSerie = dataset[
                        (dataset["QueryNum"] == query) & 
                        (dataset["WorkerNodeNum"] == workerNum) & 
                        (dataset["DataStructure"] == dataStructure) &
                        (dataset["QueryVariant"] == variant)
                    ]["Time"]
                    avgList.append(timesSerie.mean())
                    stdDevList.append(timesSerie.std())

                if len(variantList) == 1 :
                    plotLabel = dataStructure
                else :
                    plotLabel = dataStructure + " / Var " + str(variant)

                axes.plot(workerList, avgList, marker = "o", label = plotLabel)
            
            axes.set_xticks(workerList)
        
        axes.set_title("Query_" + str(query))
        axes.set_xlabel("Number of Spark Worker")
        axes.set_ylabel("Execution Time [Sec]")
        axes.grid()
        axes.legend()

        plt.tight_layout()
        plt.savefig("../doc/charts/Query_" + str(query))
        plt.clf()
            
    return


if __name__ == "__main__" :
    main()