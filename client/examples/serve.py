from featureform import ServingClient

serving = ServingClient("localhost:443")

dataset = serving.training_set("fraud_training", "quickstart")
training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
for i, feature_batch in enumerate(training_dataset):
    print(feature_batch)
    if i > 25:
        break
    i += 1


user_feat = serving.features([("avg_transactions", "quickstart")], {"user": "C1410926"})
print("\nUser Result: ")
print(user_feat)
