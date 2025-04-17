from featureform import Client

serving = Client(insecure=True)

transformation_name = "avg_transactions"
transformation_variant = "quickstart"

user_feat = serving.features(
    [(transformation_name, transformation_variant)], 
    {"user": "C1211038"}
)
print(f"User Result: {user_feat}")
