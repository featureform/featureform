from featureform import ServingClient

serving = ServingClient("localhost:443")

dataset = serving.training_set("fraud_training")
training_dataset = dataset
for i, batch in enumerate(training_dataset):
    print(batch.features(), batch.label())
    if i > 25:
        break


user_feat = serving.features([("avg_transactions", "quickstart")], {"user": "C1214240"})
print("\nUser Result: ")
print(user_feat)
