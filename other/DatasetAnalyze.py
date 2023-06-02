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

    otherDataset = pd.read_csv(
        filepath_or_buffer = "../docker/Dataset.csv",
        index_col = False,
        low_memory = False)
    
    print(otherDataset)




if __name__ == "__main__" :
    main()