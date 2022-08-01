import featureform as ff

client = ff.ServingClient(local=True)
fpf = client.features([("SepalLength", "join"),("SepalWidth", "join"), ("PetalLength", "join"), ("PetalWidth", "join")],("Id", 2))
print("features")
print(fpf)
# # fpf = client.features([("SepalLength", "centimeters")], {"CustomerID": "1"})
# # Run features through model
training_set = client.training_set("join", "v1")
for r in training_set:
    print(r)