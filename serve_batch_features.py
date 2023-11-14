import random
import featureform as ff
from featureform import ServingClient

serving = ServingClient(host="localhost:7878", insecure=True)

# Serve batch features
batch_features = serving.iterate_feature_set(("boolean_feature", "batch_serving_test_15"), ("numerical_feature", "batch_serving_test_15"))

for i, batch in enumerate(batch_features):
    print(batch)