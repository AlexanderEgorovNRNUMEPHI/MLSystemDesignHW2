import pandas as pd
from loguru import logger
from pathlib import Path

def split_data():
    data_path = Path("data/ml-latest/ratings.csv")
    train_path = Path("data/train.csv")
    test_path = Path("data/test.csv")

    ratings = pd.read_csv(data_path)

    ratings = ratings.sort_values('timestamp')
    idx = int(len(ratings) * 0.7)

    train = ratings.iloc[:idx]
    test = ratings.iloc[idx:]

    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)

    logger.info(f"Train: ({len(train)}) Test: ({len(test)}).")