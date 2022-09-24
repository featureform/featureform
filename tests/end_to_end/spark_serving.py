import os
import time

from dotenv import load_dotenv

import featureform as ff


featureform_location = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
env_file_path = os.path.join(featureform_location, ".env")
load_dotenv(env_file_path)

VERSION=os.getenv("TEST_CASE_VERSION")

SLEEP_DURATION = 30
NUMBER_OF_SLEEPS = 20

def serve_data():
    client = ff.ServingClient()

    for _ in range(NUMBER_OF_SLEEPS):
        try: 
            dataset = client.training_set(f"fraud_training_{VERSION}", "quickstart")
            training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
            for i, feature_batch in enumerate(training_dataset):
                print(feature_batch.to_list())
                if i >= 5:
                    return
        except Exception as e:
            print(f"\twaiting for {SLEEP_DURATION} seconds")
            time.sleep(SLEEP_DURATION)
    
    raise Exception(f"Serving for {VERSION} could not be completed.")

serve_data()

