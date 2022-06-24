import featureform as ff

client = ff.ServingLocalClient()
fpf = client.features([("SepalLength", "centimeters")], ("Id", 1))
print(fpf)
# fpf = client.features([("SepalLength", "centimeters")], {"CustomerID": "1"})
# Run features through model