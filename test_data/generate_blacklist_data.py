import pandas as pd
import random
import numpy as np

df = pd.read_csv("transactions.csv")
num_rows = len(df)

unique_users = df["CustomerID"].unique()
num_users = len(unique_users)

num_users_5_percent = int(num_users * 0.05)
chosen_users = np.random.choice(unique_users, size=num_users_5_percent, replace=False)
random.shuffle(chosen_users)

before_ts_users = chosen_users[ : int(num_users_5_percent * 0.4)]
inbetween_ts_users = chosen_users[int(num_users_5_percent * 0.4) : int(num_users_5_percent * 0.9)]
after_ts_users = chosen_users[int(num_users_5_percent * 0.9) :]

blacklisted_df = pd.DataFrame(columns=["CustomerID", "Timestamp"])

for user in before_ts_users:
    user_df = df[df["CustomerID"] == user]
    earliest_ts = pd.to_datetime(user_df["Timestamp"]).min()
    blacklisted_df = blacklisted_df.append({"CustomerID": user, "Timestamp": (earliest_ts - pd.Timedelta(days=1))}, ignore_index=True)

for user in after_ts_users:
    user_df = df[df["CustomerID"] == user]
    earliest_ts = pd.to_datetime(user_df["Timestamp"]).max()
    blacklisted_df = blacklisted_df.append({"CustomerID": user, "Timestamp": (earliest_ts + pd.Timedelta(days=1))}, ignore_index=True)

for user in inbetween_ts_users:
    user_df = df[df["CustomerID"] == user]
    inbetween_ts = np.random.choice(user_df["Timestamp"], size=1, replace=False)
    blacklisted_df = blacklisted_df.append({"CustomerID": user, "Timestamp": inbetween_ts[0]}, ignore_index=True)
    
blacklisted_df.to_csv("blacklisted_transactions.csv", index=False)