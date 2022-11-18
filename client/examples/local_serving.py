import featureform as ff

client = ff.ServingClient(local=True)

training_set = client.training_set("fraud_training", "quickstart")

columns = training_set.columns() # NEW: returns a list of all column names
feature_columns = training_set.feature_columns() # NEW: returns a list of all feature column names
label_column = training_set.label_column() # NEW: returns the label column
i=0
for r in training_set:
    print(r.features()) # prints features
    print(r.label()) # prints label
    print(r.to_numpy()) # returns row as numpy array

    print(r.to_df()) # NEW: returns row as dataframe array with column names
    
    i+=1
    if i > 10:
        break

client = ff.ServingClient(local=True)
fpf = client.features([("avg_transactions", "quickstart")], {"CustomerID": "C1410926"})
print(fpf)


