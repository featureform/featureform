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
    with open("version.txt", "w") as f:
        f.write(version)

VERSION=get_random_string()
os.environ["TEST_CASE_VERSION"]=VERSION
save_version(VERSION)

# Start of Featureform Definitions
ff.register_user("featureformer").make_default_owner()

redis = ff.register_redis(
    name = "redis-quickstart",
    host="quickstart-redis", # The internal dns name for redis
    port=6379,
    description = "A Redis deployment we created for the Featureform quickstart"
)

k8s = ff.register_k8s_azure(
    name="k8s",
    account_name=os.getenv("AZURE_ACCOUNT_NAME", ""),
    account_key=os.getenv("AZURE_ACCOUNT_KEY", ""),
    container_name=os.getenv("AZURE_CONTAINER_NAME", ""),
    path="testing/ff",
)

transactions = k8s.register_file(
    name=f"transactions",
    variant="quickstart",
    description="A dataset of fraudulent transactions",
    path="featureform/testing/primary/name/variant/transactions_short.csv"
)

@k8s.sql_transformation(name=f"average_user_transaction_{VERSION}", 
                        variant="quickstart")
def average_user_transaction():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.quickstart}} GROUP BY user_id"

user = ff.register_entity("user")

# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="CustomerID",
    inference_store=redis,
    features=[
        {"name": f"avg_transactions_{VERSION}", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
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
    features=[(f"avg_transactions_{VERSION}", "quickstart")],
)
