import os
import time

from dotenv import load_dotenv

import featureform as ff


SLEEP_DURATION = 30
NUMBER_OF_SLEEPS = 20

FILE_DIRECTORY = os.getenv("FEATUREFORM_TEST_PATH", "")
featureform_location = os.path.dirname(os.path.dirname(FILE_DIRECTORY))
env_file_path = os.path.join(featureform_location, ".env")
load_dotenv(env_file_path)

def read_version():
    global FILE_DIRECTORY
    try:
        with open(f"{FILE_DIRECTORY}/version.txt", "r") as f:
            version = f.read().strip()
    except:
        version = None
    
    return version
        

VERSION=os.getenv("TEST_CASE_VERSION", read_version())
if VERSION == None:
    raise TypeError("VERSION is set to None.")

client = ff.ServingClient()

def serve_data():
    for _ in range(NUMBER_OF_SLEEPS):
        try: 
            dataset = client.training_set("sentiment_prediction", VERSION)
            training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
            for i, feature_batch in enumerate(training_dataset):
                if i >= 10:
                    return
                print(feature_batch.to_list())
                
        except Exception as e:
            print(f"\twaiting for {SLEEP_DURATION} seconds")
            
            time.sleep(SLEEP_DURATION)
    
    raise Exception(f"Serving for {VERSION} could not be completed.")

def serve_feature():
    fpf = client.features([(f"review_text", VERSION)], {"order": "73fc7af87114b39712e6da79b0a377eb"})
    print(fpf)

print(f"Serving the training set (sentiment_prediction : variant({VERSION}))")
serve_data()

print("\n")

print(f"Serving the feature (review_text : variant ({VERSION})")
serve_feature()