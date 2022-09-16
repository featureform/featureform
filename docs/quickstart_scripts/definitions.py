import featureform as ff

postgres = ff.register_postgres(
    name="postgres-quickstart",
    host="host.docker.internal",  # The docker dns name for postgres
    port="5432",
    user="postgres",
    password="password",
    database="postgres",
)

redis = ff.register_redis(
    name="redis-quickstart",
    host="host.docker.internal",  # The docker dns name for redis
    port=6379,
)

ff.register_user("featureform_user").make_default_owner()

transactions = postgres.register_table(
    name="transactions",
    table="Transactions",  # This is the table's name in Postgres
)


@postgres.sql_transformation()
def average_user_transaction():
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.default}} GROUP BY user_id"


user = ff.register_entity("user")
# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": "avg_transactions", "column": "avg_transaction_amt", "type": "float32"},
    ],
)
# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="customerid",
    labels=[
        {"name": "fraudulent", "column": "isfraud", "type": "bool"},
    ],
)

ff.register_training_set(
    "fraud_training",
    label="fraudulent",
    features=["avg_transactions"],
)
