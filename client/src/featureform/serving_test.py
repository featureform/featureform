import featureform as ff

client = ff.ServingLocalClient()
fpf = client.features([("SepalLength", "join"),("SepalWidth", "join"), ("PetalLength", "join"), ("PetalWidth", "join")],("Id", 2))
print("features")
print(fpf)
training_set = client.training_set("join", "v1")
for r in training_set:
    print(r)
