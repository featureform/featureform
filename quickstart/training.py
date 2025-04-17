from featureform import Client

client = Client(insecure=True)

ts_name = "fraud_training"
ts_variant = "quickstart"

dataset = client.training_set(ts_name, ts_variant)

for i, batch in enumerate(dataset):
    print(batch)
