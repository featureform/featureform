from featureform import Client

serving = Client(insecure=True)

user_feat = serving.features(["avg_transactions"], {"user": "C1214240"})
print("User Result: ")
print(user_feat)
