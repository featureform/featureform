import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from pathlib import Path

import csv


def generate_data(num_rows: int, num_features: int, key_space: int) -> pd.DataFrame:
    features = [f"feature_{i}" for i in range(num_features)]
    columns = ["entity", "event_timestamp"] + features
    df = pd.DataFrame(0, index=np.arange(num_rows), columns=columns)
    df["event_timestamp"] = datetime.datetime.utcnow()
    for column in ["entity"] + features:
        df[column] = np.random.randint(1, key_space, num_rows)
    df["entity"] = df["entity"].astype(str)
    return df


if __name__ == "__main__":
    df = generate_data(10**4, 250, 10**4)
    df.to_parquet("generated_data.parquet", index=False)
