import featureform as ff

client = ff.ServingLocalClient()

training_set = client.training_set("fraud_training", "quickstart")
i=0
for r in training_set:
    print(r)
    i+=1
    if i > 10:
        break

client = ff.ServingLocalClient()
fpf = client.features([("avg_transactions", "quickstart")], ("CustomerID", "C1410926"))
print(fpf)