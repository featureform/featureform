import featureform as ff

client = ff.Client(local=True)

training_set = client.training_set("fraud_training", "courageous_jones")
i = 0
for r in training_set:
    print(r)
    i += 1
    if i > 10:
        break

client = ff.Client(local=True)
fpf = client.features([("avg_transactions", "courageous_jones")], {"user": "C1410989"})
print(fpf)
