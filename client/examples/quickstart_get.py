import featureform as ff

redis = ff.register_redis(
    name="redis-quickstart",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart"
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

redis_get = ff.get_redis("redis-quickstart")

postgres_get = ff.get_postgres("postgres-quickstart")

ff.register_user("featureformer").make_default_owner()

transactions = postgres_get.register_table(
    name="transactions",
    variant="kaggle",
    description="Fraud Dataset From Kaggle",
    table="Transactions",  # This is the table's name in Postgres
)

@postgres_get.sql_transformation(variant="quickstart")
def average_user_transaction():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"

user = ff.register_entity("user")

user_get = ff.get_entity("user")

average_user_transaction_get = ff.get_source("average_user_transaction", "quickstart")

average_user_transaction_get.register_resources(
    entity=user_get,
    entity_column="user_id",
    inference_store=redis_get,
    features=[
        {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
    ],
)


transactionsGet = ff.get_source("transactions", "kaggle")

# Register label from our base Transactions table
transactionsGet.register_resources(
    entity=user_get,
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
