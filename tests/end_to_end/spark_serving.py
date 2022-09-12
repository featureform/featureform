import os
from dotenv import load_dotenv

import featureform as ff

load_dotenv("/Users/kempytoor/Documents/Featureform/repos/featureform/.env")

VERSION=os.getenv("TEST_CASE_VERSION")

def serve_data():
    client = ff.ServingClient()
    dataset = client.training_set(f"fraud_training_{VERSION}", "quickstart")
    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for i, feature_batch in enumerate(training_dataset):
        print(feature_batch.to_list())
        if i >= 5:
            return

serve_data()