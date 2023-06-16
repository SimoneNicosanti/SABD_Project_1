import pandas as pd
import matplotlib.pyplot as plt


def main() :
    dataset = pd.read_csv("../Results/evaluation/evaluations.csv")

    aggregated = dataset.groupby(by = ["Query", "WorkerNodeNum"]).aggregate({'Time': ['min', "mean", 'max', "std"]})

    print(aggregated.columns)
    print(aggregated)

    aggregated.plot(subplots=True, rot=0, figsize=(9, 7), layout=(2, 3))
    
    plt.savefig("prova.png")

    return


if __name__ == "__main__" :
    main()