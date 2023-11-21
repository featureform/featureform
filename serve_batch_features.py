import random
import featureform as ff
from featureform import ServingClient

serving = ServingClient(host="localhost:7878", insecure=True)

# Serve batch features
batch_features = serving.iterate_feature_set(("transaction_feature", "variant_43636"), ("balance_feature", "variant_436536"), ("perc_feature", "variant_436536"))

for i, batch in enumerate(batch_features):
    print(batch)