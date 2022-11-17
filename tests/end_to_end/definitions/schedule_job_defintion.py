import os
from datetime import timedelta 

from dotenv import load_dotenv

import featureform as ff


FILE_DIRECTORY = os.getenv("FEATUREFORM_TEST_PATH", "")
featureform_location = os.path.dirname(os.path.dirname(FILE_DIRECTORY))
env_file_path = os.path.join(featureform_location, ".env")
load_dotenv(env_file_path)

UPDATE_EVERY_MINUTE = "*/1 * * * *"

def get_random_string():
    import random
    import string
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))

def save_version(version):
    global FILE_DIRECTORY
    with open(f"{FILE_DIRECTORY}/version-schedule.txt", "w+") as f:
        f.write(version)

VERSION=get_random_string()
os.environ["TEST_CASE_VERSION"]=VERSION
save_version(VERSION)

# Start of Featureform Definitions
ff.register_user("featureformer").make_default_owner()

azure_blob = ff.register_blob_store(
    name=f"k8s_blob_store_{VERSION}",
    account_name= os.getenv("AZURE_ACCOUNT_NAME", None),
    account_key= os.getenv("AZURE_ACCOUNT_KEY", None),
    container_name= os.getenv("AZURE_CONTAINER_NAME", None),
    root_path="testing/ff",
)

redis = ff.register_redis(
    name = f"redis-quickstart_{VERSION}",
    host="quickstart-redis", # The internal dns name for redis
    port=6379,
    description = "A Redis deployment we created for the Featureform quickstart"
)

k8s = ff.register_k8s(
    name=f"k8s_{VERSION}",
    store=azure_blob
)

transactions = k8s.register_file(
    name=f"transactions_{VERSION}",
    variant="quickstart",
    description="A dataset of fraudulent transactions",
    path="featureform/testing/primary/name/variant/transactions_short_short.csv"
)

@k8s.df_transformation(name=f"average_user_transaction_{VERSION}", 
                        variant="quickstart",
                        inputs=[(f"transactions_{VERSION}", "quickstart")]
                        schedule=UPDATE_EVERY_MINUTE)
def average_user_transaction(transactions):
    """the average transaction amount for a user """
    user_tsc = transactions[["CustomerID","TransactionAmount","Timestamp"]]
    return user_tsc.groupby("CustomerID").agg({'TransactionAmount':'mean','Timestamp':'max'})

user = ff.register_entity("user")

# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="CustomerID",
    inference_store=redis,
    features=[
        {"name": f"avg_transactions_{VERSION}", "variant": "quickstart", "column": "TransactionAmount", "type": "float32", "schedule": UPDATE_EVERY_MINUTE},
    ],
)

# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="CustomerID",
    labels=[
        {"name": f"fraudulent_{VERSION}", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
    ],
)

ff.register_training_set(
    f"fraud_training_{VERSION}", "quickstart",
    label=(f"fraudulent_{VERSION}", "quickstart"),
    features=[
        (f"avg_transactions_{VERSION}", "quickstart"),
        {"feature": f"avg_transactions_{VERSION}", "variant": "quickstart", "name": "avg_transaction_new_name"},
        {"feature": f"avg_transactions_{VERSION}", "variant": "quickstart", "name": "avg_transaction_lag_1d", "lag": timedelta(days=1)},
    ],
    schedule=UPDATE_EVERY_MINUTE
)






################################################################
transactions = k8s.register_file(
    name=f"ice_cream_{VERSION}",
    variant="quickstart",
    description="A dataset of ice cream records",
    path="featureform/testing/primary/name/variant/ice_cream_short.csv"
)

@k8s.df_transformation(name=f"ice_cream_dataset_{VERSION}", 
                        variant="quickstart",
                        inputs=[(f"ice_cream_{VERSION}", "quickstart")])
def ice_cream_dataset(transactions):
    """ice cream records"""
    return transactions

user = ff.register_entity("user")

# Register a column from our transformation as a feature
ice_cream_dataset.register_resources(
    entity=user,
    entity_column="farm_id",
    inference_store=redis,
    features=[
        {"name": f"dairy_flow_rate_{VERSION}", "variant": "quickstart", "column": "dairy_flow_rate", "type": "float32"},
    ],
)

# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="farm_id",
    labels=[
        {"name": f"quality_score_{VERSION}", "variant": "quickstart", "column": "quality_score", "type": "float32"},
    ],
)

ff.register_training_set(
    f"dairy_flow_lag_{VERSION}", "quickstart",
    label=(f"quality_score_{VERSION}", "quickstart"),
    features=[
        (f"dairy_flow_rate_{VERSION}", "quickstart"),
        {"feature": f"dairy_flow_rate_{VERSION}", "variant": "quickstart", "name": "dairy_flow_rate_lag_1h", "lag": timedelta(hours=1)},
    ],
)
