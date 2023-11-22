import random
import featureform as ff
from featureform import ServingClient

serving = ServingClient(host="localhost:7878", insecure=True)

# Serve batch features
batch_features = serving.batch_features(("table1_feature", "variant_934075"), ("table2_feature", "variant_934075"), ("table3_feature", "variant_934075"), ("table4_feature", "variant_934075"))

for i, batch in enumerate(batch_features):
    print(batch)