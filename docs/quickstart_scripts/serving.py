from featureform import ServingClient

serving = ServingClient(insecure=True)

user_feat = serving.features(["avg_transactions"], {"user": "C1214240"})
print("User Result: ")
print(user_feat)
