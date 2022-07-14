import featureform as ff

client = ff.ServingLocalClient()
fpf = client.features([("SepalLength", "join"),("SepalWidth", "join"), ("PetalLength", "join"), ("PetalWidth", "join")],("Id", 2))
print("features")
print(fpf)
# # fpf = client.features([("SepalLength", "centimeters")], {"CustomerID": "1"})
# # Run features through model
training_set = client.training_set("jjoin", "v1")
for r in training_set:
    print(r)

# ff.register_user("featureformer").make_default_owner()
# ff.register_training_set(
#     "fraud_trainxing", "quickstart",
#     label=("fraxudulent", "quickstart"),
#     features=[("avg_transaxtions", "quickstart")],
# )

# ff.ServingLocalClient().training_set("fraud_trainxing", "quickstart")