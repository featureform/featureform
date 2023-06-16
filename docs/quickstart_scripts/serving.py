from featureform import Client

client = Client(insecure=True)

user_feat = client.features(["avg_transactions"], {"user": "C1214240"})
print("User Result: ")
print(user_feat)
