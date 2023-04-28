import os
from datetime import timedelta

from dotenv import load_dotenv

import featureform as ff


FILE_DIRECTORY = os.getenv("FEATUREFORM_TEST_PATH", "")
featureform_location = os.path.dirname(os.path.dirname(FILE_DIRECTORY))
env_file_path = os.path.join(featureform_location, ".env")
print(env_file_path)
load_dotenv(env_file_path)


def get_random_string():
    import random
    import string

    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))


def save_to_file(filename, data):
    global FILE_DIRECTORY
    with open(f"{FILE_DIRECTORY}/{filename}", "w+") as f:
        f.write(data)


VERSION = get_random_string()
os.environ["TEST_CASE_VERSION"] = VERSION

FEATURE_NAME = f"ice_cream_feature_{VERSION}"
FEATURE_VARIANT = "lag_features"
TRAININGSET_NAME = f"ice_cream_training_{VERSION}"
TRAININGSET_VARIANT = "lag_features"

FEATURE_SERVING = f"farm:farm"
VERSIONS = f"{FEATURE_NAME},{FEATURE_VARIANT}:{TRAININGSET_NAME},{TRAININGSET_VARIANT}"

save_to_file("feature.txt", FEATURE_SERVING)
save_to_file("version.txt", VERSIONS)


# Start of Featureform Definitions
azure_blob = ff.register_blob_store(
    name=f"k8s_blob_store_{VERSION}",
    account_name=os.getenv("AZURE_ACCOUNT_NAME", None),
    account_key=os.getenv("AZURE_ACCOUNT_KEY", None),
    container_name=os.getenv("AZURE_CONTAINER_NAME", None),
    root_path="testing/ff",
)

redis = ff.register_redis(
    name=f"redis-quickstart_{VERSION}",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart",
)

k8s = ff.register_k8s(
    name=f"k8s_{VERSION}",
    store=azure_blob,
    docker_image=os.getenv("K8S_RUNNER_BASE_IMAGE", "featureformcom/k8s_runner:latest"),
)

ice_cream = k8s.register_file(
    name=f"ice_cream_{VERSION}",
    variant=VERSION,
    description="A dataset of ice cream",
    path="testing/ff/data/ice_cream_100rows.csv",
)


@k8s.df_transformation(
    name=f"ice_cream_entity_{VERSION}", variant=VERSION, inputs=[ice_cream]
)
def ice_cream_entity_transformation(df):
    """the ice cream dataset with entity"""
    df["entity"] = "farm"
    return df


specs = ff.K8sResourceSpecs(
    cpu_request="100m",
    cpu_limit="500m",
    memory_request="1Gi",
    memory_limit="2Gi",
)


@k8s.df_transformation(
    name=f"ice_cream_transformation_{VERSION}",
    variant=VERSION,
    inputs=[(f"ice_cream_entity_{VERSION}", VERSION)],
    docker_image=os.getenv(
        "K8S_RUNNER_SCIKIT", "featureformcom/k8s_runner:latest-scikit"
    ),
    resource_specs=specs,
)
def scikit_test(df):
    """the ice cream dataset"""
    from sklearn import datasets

    iris = datasets.load_iris()
    print(iris.items())
    return df


farm = ff.register_entity("farm")

# Register a column from our transformation as a feature
scikit_test.register_resources(
    entity=farm,
    entity_column="entity",
    timestamp_column="time_index",
    inference_store=azure_blob,
    features=[
        {
            "name": FEATURE_NAME,
            "variant": FEATURE_VARIANT,
            "column": "dairy_flow_rate",
            "type": "float32",
        },
    ],
)

# Register label from our base Transactions table
ice_cream_entity_transformation.register_resources(
    entity=farm,
    entity_column="entity",
    timestamp_column="time_index",
    labels=[
        {
            "name": f"ice_cream_label_{VERSION}",
            "variant": FEATURE_VARIANT,
            "column": "quality_score",
            "type": "float32",
        },
    ],
)

ff.register_training_set(
    TRAININGSET_NAME,
    TRAININGSET_VARIANT,
    label=(f"ice_cream_label_{VERSION}", FEATURE_VARIANT),
    features=[
        (f"ice_cream_feature_{VERSION}", FEATURE_VARIANT),
    ],
)
