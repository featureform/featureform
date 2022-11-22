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

def read_file(filename):
    global FILE_DIRECTORY
    try:
        with open(f"{FILE_DIRECTORY}/{filename}", "r") as f:
            data = f.read().strip()
            return data
    except:
        return None

def parse_versions(data):
    versions = data.split(":")
    feature_name, feature_variant = versions[0].split(",")
    training_name, training_variant = versions[1].split(",")
    return feature_name, feature_variant, training_name, training_variant

def parse_feature(data):
    return data.split(":")

FEATURE_NAME, FEATURE_VARIANT, TRAININGSET_NAME, TRAININGSET_VARIANT = parse_versions(read_file("versions.txt"))
FEATURE_ENTITY, FEATURE_VALUE = parse_feature(read_file("feature.txt"))
if FEATURE_NAME == None or FEATURE_VARIANT == None or TRAININGSET_NAME == None or TRAININGSET_VARIANT == None:
    raise TypeError("VERSION is set to None.")

client = ff.ServingClient()

def serve_data():
    try:
        dataset = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait()
        training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
        for i, feature_batch in enumerate(training_dataset):
            print(feature_batch.to_list())
        
    except Exception as e:
        print(f"Serving for {TRAININGSET_NAME}:{TRAININGSET_VARIANT} could not be completed: {e}")

def serve_feature():
    try:
        fpf = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait()
        print(fpf)
    except Exception as e:
        print(f"Serving for {FEATURE_NAME}:{FEATURE_VARIANT} could not be completed: {e}")


print(f"Serving the training set ({TRAININGSET_NAME}:{TRAININGSET_VARIANT})")
serve_data()

print("\n")

print(f"Serving the feature for ({FEATURE_NAME}:{FEATURE_VARIANT})")
serve_feature()

