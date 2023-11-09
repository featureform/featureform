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
redis = ff.register_redis(
    name=f"redis-kcf-e2e_{VERSION}",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart",
)

aws_creds = ff.AWSCredentials(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", None),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY", None),
)

s3 = ff.register_s3(
    name=f"kcf-s3-{VERSION}",
    credentials=aws_creds,
    bucket_name=os.getenv("S3_BUCKET_PATH", None),
    bucket_region=os.getenv("S3_BUCKET_REGION", None),
    path=f"end_to_end_tests/{VERSION}",
)

k8s = ff.register_k8s(
    name=f"k8s_s3_{VERSION}",
    store=s3,
    docker_image=os.getenv("K8S_RUNNER_BASE_IMAGE", "featureformcom/k8s_runner:latest"),
)

ice_cream = k8s.register_file(
    name=f"ice_cream_{VERSION}",
    variant=VERSION,
    description="A dataset of ice cream",
    path="featureform/tests/ice_cream.parquet",
)


@k8s.df_transformation(
    name=f"ice_cream_entity_{VERSION}", variant=VERSION, inputs=[ice_cream]
)
def ice_cream_entity_transformation(df):
    """the ice cream dataset with entity"""
    df["entity"] = "farm"
    return df


@k8s.df_transformation(variant=VERSION, inputs=[ice_cream])
def ordering_transformation_1(df):
    """a transformation to test out ordering"""
    df["transformation_1"] = "true"
    return df


@k8s.df_transformation(variant=VERSION, inputs=[ice_cream])
def ordering_transformation_2(df):
    """a transformation to test out ordering"""
    df["transformation_2"] = "true"
    return df


@k8s.df_transformation(variant=VERSION, inputs=[ice_cream])
def ordering_transformation_3(df):
    """a transformation to test out ordering"""
    df["transformation_3"] = "true"
    return df


@k8s.df_transformation(
    variant=VERSION,
    inputs=[
        ordering_transformation_1,
        ordering_transformation_2,
        ordering_transformation_3,
    ],
)
def test_ordering(df1, df2, df3):
    """a transformation to test out ordering"""
    df1 = df1[["time_index", "dairy_flow_rate", "transformation_1"]]
    df2 = df2[["time_index", "sugar_flow_rate", "transformation_2"]]
    df3 = df3[["time_index", "oven_temperature", "transformation_3"]]

    df = df1.merge(df2, on="time_index").merge(df3, on="time_index")

    return df


farm = ff.register_entity("farm")

# Register a column from our transformation as a feature
ice_cream_entity_transformation.register_resources(
    entity=farm,
    entity_column="entity",
    timestamp_column="time_index",
    inference_store=redis,
    features=[
        {
            "name": FEATURE_NAME,
            "variant": FEATURE_VARIANT,
            "column": "quality_score",
            "type": "float32",
        },
    ],
)

test_ordering.register_resources(
    entity=farm,
    entity_column="entity",
    timestamp_column="time_index",
    inference_store=redis,
    features=[
        {
            "name": "ordering_test_1",
            "variant": VERSION,
            "column": "transformation_1",
            "type": "string",
        },
        {
            "name": "ordering_test_2",
            "variant": VERSION,
            "column": "transformation_2",
            "type": "string",
        },
        {
            "name": "ordering_test_3",
            "variant": VERSION,
            "column": "transformation_3",
            "type": "string",
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
        (FEATURE_NAME, FEATURE_VARIANT),
        ("ordering_test_1", VERSION),
        ("ordering_test_2", VERSION),
        ("ordering_test_3", VERSION),
    ],
)
