import featureform as ff
from datetime import datetime
import pandas as pd

df = pd.read_parquet("../generated_data.parquet")
entities = df["entity"].unique()
print(entities)

client = ff.ServingClient(host="benchmark.featureform.com")
features = []
for i in range(1):
    features.append(f"feature_{i}")
start_time = datetime.now()
for i in range(1):
    feature = client.features(features, {"entity": entities[i]})
    print(feature)
end_time = datetime.now()

print(f"Time to get features: {end_time - start_time}")
