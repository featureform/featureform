import pandas as pd
from typing import List

df = pd.read_csv("ice_cream.csv")
# df = pd.read_parquet("~/Downloads/2022-10-28 23_55_26.119877.parquet")
# print(df)

def check_for_low_sugar(df: pd.DataFrame):
    df["low_sugar_rate"] = (df.loc[:, "sugar_flow_rate"] < 5).astype(int)
    return df


def smoothen_sugar_flow_rate(df: pd.DataFrame) -> pd.Series:
    df['time_index']= pd.to_datetime(df['time_index'])
    df.set_index('time_index', inplace=True)
    df = df.loc[:, "sugar_flow_rate"].rolling("6H").mean().reset_index()
    df['time_index'] = df['time_index'].astype(str)
    return df

def add_2_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["sum_of_flow_rate_oven_temp"] = df.loc[:, ["sugar_flow_rate", "oven_temperature"]].sum(axis=1)
    return df


with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
  #  print(check_for_low_sugar(df))
    print(smoothen_sugar_flow_rate(df))
  #  print(add_2_cols(df))

