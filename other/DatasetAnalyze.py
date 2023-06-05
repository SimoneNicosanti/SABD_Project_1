import pandas as pd


def main() :
    dataset = pd.read_csv(
        filepath_or_buffer = "../Dataset.csv",
        header = 11,
        skiprows = [12],
        index_col = False,
        low_memory = False)
    
    dataset.to_csv(
        "../dataset/Dataset.csv",
        header = True,
        index = False,
        )
    
    dataset.sort_values("Trading date")

    dataset = dataset[
        dataset["ID"].str.endswith(".ETR")
    ]
    print(dataset[dataset["ID"].str.endswith(".ETR")].count())




if __name__ == "__main__" :
    main()