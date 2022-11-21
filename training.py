from featureform import ServingClient

client = ServingClient(insecure=True)
dataset = client.training_set("fraud_training")

for i, batch in enumerate(dataset):
    print(batch)
    if i > 25:
        break