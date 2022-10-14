import os
from dotenv import load_dotenv

import featureform as ff


featureform_location = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
env_file_path = os.path.join(featureform_location, ".env")
load_dotenv(env_file_path)

def get_random_string():
    import random
    import string
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))

def save_version(version):
    with open("../version.txt", "w") as f:
        f.write(version)

VERSION=get_random_string()
os.environ["TEST_CASE_VERSION"]=VERSION
save_version(VERSION)

redis = ff.register_redis(
    name = f"redis-quickstart_{VERSION}",
    host="featureform-quickstart-redis", # The internal dns name for redis
    port=6379,
    description = "A Redis deployment we created for the Featureform quickstart"
)

args = {
            "name": f"testing_spark_definition_{VERSION}",
            "description": "test",
            "team": "featureform",
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_KEY"),
            "bucket_path": os.getenv("S3_BUCKET_PATH"),
            "bucket_region": os.getenv("S3_BUCKET_REGION"),
            "emr_cluster_id": os.getenv("AWS_EMR_CLUSTER_ID"),
            "emr_cluster_region": os.getenv("AWS_EMR_CLUSTER_REGION"),
        }
spark = ff.register_spark(**args)

ff.register_user(f"featureformer_{VERSION}").make_default_owner()

file = spark.register_parquet_file(
    name=f"transaction_short",
    variant="test_variant",
    owner=f"featureformer_{VERSION}",
    file_path="s3://featureform-spark-testing/featureform/source_datasets/transaction_short/",
)


@spark.sql_transformation(name=f"avg_transactions_{VERSION}", variant="quickstart")
def average_user_score():
    """the average score for a user"""
    return f"SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{{{ transaction_short.test_variant }}}} GROUP BY user_id"


user = ff.register_entity("user")
average_user_score.VERSION = VERSION
average_user_score.register_resources(
    entity=user,
    owner=f"featureformer_{VERSION}",
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": f"avg_transaction_{VERSION}", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
    ],
)


# Register label from our base file table
file.register_resources(
    entity=user,
    owner=f"featureformer_{VERSION}",
    entity_column="CustomerID",
    labels=[
        {"name": f"fraudulent_{VERSION}", "variant": "quickstart", "column": "isfraud", "type": "bool"},
    ],
)

ff.register_training_set(
    f"fraud_training_{VERSION}", "quickstart",
    owner=f"featureformer_{VERSION}",
    label=(f"fraudulent_{VERSION}", "quickstart"),
    features=[(f"avg_transaction_{VERSION}", "quickstart")],
)

