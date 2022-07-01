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

ff.register_user("featureformer").make_default_owner()

transactions = postgres.register_table(
    name="transactions",
    variant="kaggle",
    description="Fraud Dataset From Kaggle",
    table="Transactions",  # This is the table's name in Postgres
)


@postgres.sql_transformation(variant="quickstart")
def average_user_transaction_v2():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"


user = ff.register_entity("user")

average_user_transaction_v2.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
        {"name": "avg_transactions", "variant": "prod", "column": "avg_transaction_amt", "type": "float32"},
        {"name": "avg_transactions_v2", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
        {"name": "avg_transactions_v2", "variant": "va", "column": "avg_transaction_amt", "type": "float32"},
    ]
)

# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="customerid",
    labels=[
        {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
        {"name": "fraudulent", "variant": "v1", "column": "isfraud", "type": "bool"},
    ],
)

ff.register_training_set(
    "fraud_training_v2", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)
