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

redisGet = ff.get_redis("redis-quickstart")

postgresGet = ff.get_postgres("postgres-quickstart")

ff.register_user("featureformer").make_default_owner()

transactions = postgresGet.register_table(
    name="transactions",
    variant="kaggle",
    description="Fraud Dataset From Kaggle",
    table="Transactions",  # This is the table's name in Postgres
)


@postgresGet.sql_transformation(variant="quickstart")
def average_user_transaction_v2():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"


u = ff.get_entity("user")

aut = ff.get_source("average_user_transaction_v2", "quickstart")

aut.register_resources(
    entity=u,
    entity_column="user_id",
    inference_store=redisGet,
    features=[
        {"name": "avg_transactions", "variant": "testing", "column": "avg_transaction_amt", "type": "float32"}
    ]
)

t = ff.get_source("transactions", "kaggle")

# Register label from our base Transactions table
t.register_resources(
    entity=u,
    entity_column="customerid",
    labels=[
        {"name": "fraudulent", "variant": "v2", "column": "isfraud", "type": "bool"}
    ],
)

ff.register_training_set(
    "fraud_training_v2", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)
