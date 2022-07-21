import featureform as ff
import os

redis = ff.register_redis(
    name="redis-quickstart",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart"
)

dynamo = ff.register_dynamodb(
    name="test-dynamo",
    description="Test of dynamo-db creation",
    team="featureform",
    access_key=os.getenv("DYNAMO_ACCESS_KEY"),
    secret_key=os.getenv("DYNAMO_SECRET_KEY"),
    region="us-east-1"
)

postgres = ff.register_postgres(
    name="postgres-quickstart",
    host="quickstart-postgres",  # The internal dns name for postgres
    port="5432",
    user="postgres",
    password="password",
    database="postgres",
    description="A Postgres deployment we created for the Featureform quickstart"
)

ff.register_user("featureformer").make_default_owner()

transactions = postgres.register_table(
    name="transactions",
    variant="kaggle",
    description="Fraud Dataset From Kaggle",
    table="Transactions",  # This is the table's name in Postgres
)


@postgres.sql_transformation(variant="quickstart")
def average_user_transaction():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"


user = ff.register_entity("user")

average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
    ],
)

average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=dynamo,
    features=[
        {"name": "avg_transactions_2", "variant": "dynamo", "column": "avg_transaction_amt", "type": "float32"},
    ],
)

# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="customerid",
    labels=[
        {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
    ],
)

ff.register_training_set(
    "fraud_training", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)
