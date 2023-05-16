import os
from datetime import timedelta

from dotenv import load_dotenv

import featureform as ff


FILE_DIRECTORY = os.getenv("FEATUREFORM_TEST_PATH", "")
featureform_location = os.path.dirname(os.path.dirname(FILE_DIRECTORY))
env_file_path = os.path.join(featureform_location, "../../../.env")
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

FEATURE_NAME = f"spark_e2e_{VERSION}"
FEATURE_VARIANT = "generic_gcs"
TRAININGSET_NAME = f"spark_e2e_training_{VERSION}"
TRAININGSET_VARIANT = "generic_gcs"

FEATURE_SERVING = f"farm:farm1"
VERSIONS = f"{FEATURE_NAME},{FEATURE_VARIANT}:{TRAININGSET_NAME},{TRAININGSET_VARIANT}"

TEST_NAME = "spark_generic_gcs"
save_to_file(f"feature_{TEST_NAME}.txt", FEATURE_SERVING)
save_to_file(f"version_{TEST_NAME}.txt", VERSIONS)


# Start of Featureform Definitions
redis = ff.register_redis(
    name=f"redis-spark-e2e_{VERSION}",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart",
)

spark_creds = ff.SparkCredentials(
    master=os.getenv("SPARK_MASTER", "local"),
    deploy_mode="client",
    python_version="3.7.16",
)

gcp_creds = ff.GCPCredentials(
    project_id=os.getenv("GCP_PROJECT_ID"),
    credentials_path=os.getenv("GCP_CREDENTIALS_FILE"),
)

gcs = ff.register_gcs(
    name=f"gcs-quickstart_{VERSION}",
    credentials=gcp_creds,
    bucket_name=os.getenv("GCS_BUCKET_NAME", None),
    bucket_path=f"end_to_end_tests/{VERSION}",
)

spark = ff.register_spark(
    name=f"spark-generic-gcs_{VERSION}",
    description="A Spark deployment we created for the Featureform quickstart",
    team="featureform-team",
    executor=spark_creds,
    filestore=gcs,
)

ice_cream_dataset = spark.register_parquet_file(
    name=f"ice_cream_{VERSION}",
    variant=VERSION,
    description="A dataset of ice cream",
    file_path="gs://featureform-test/ice_cream.parquet",
)


@spark.df_transformation(
    name=f"ice_cream_transformation_{VERSION}",
    variant=VERSION,
    inputs=[(f"ice_cream_{VERSION}", VERSION)],
)
def ice_cream_transformation(df):
    """the ice cream dataset"""
    return df


farm = ff.register_entity("farm")

# Register a column from our transformation as a feature
ice_cream_transformation.register_resources(
    entity=farm,
    entity_column="strawberry_source",
    inference_store=redis,
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
ice_cream_transformation.register_resources(
    entity=farm,
    entity_column="strawberry_source",
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
    ],
)
