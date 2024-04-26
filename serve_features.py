import random
import featureform as ff
from featureform import ServingClient

num = 379383820

client = ff.Client(insecure=True, host="localhost:7878")

# transactions_df = client.dataframe(transaction)
# print(transactions_df.head(10))

# Serve single feature
user_feat = client.features([("CustLocation", f"variant_{num}")], {"user": "C5342380"})
print("User Result: ")
print(user_feat)
