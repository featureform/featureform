from featureform import ServingClient

serving = ServingClient("localhost:8000")

dataset = serving.dataset("f1", "v1")
b = dataset.batch(5)
dataset.shuffle(7)
dataset.repeat(3)
for r in dataset:
    print(r)
print(serving.features([("f1", "v1")], {"user": "a"}))