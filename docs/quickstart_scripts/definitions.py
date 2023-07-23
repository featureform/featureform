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

transactions = postgres.register_table(
    name="transactions",
    table="Transactions",  # This is the table's name in Postgres
)


@postgres.sql_transformation()
def average_user_transaction():
    return (
        "SELECT CustomerID as user_id, avg(TransactionAmount) "
        "as avg_transaction_amt from {{transactions.default}} GROUP BY user_id"
    )


@ff.entity
class User:
    # Register a column from our transformation as a feature
    avg_transactions = ff.Feature(
        average_user_transaction[["user_id", "avg_transaction_amt"]],
        variant: "quickstart",
        type=ff.Float32,
        inference_store=redis,
    )
    # Register label from our base Transactions table
    fraudulent = ff.Label(
        transactions[["customerid", "isfraud"]],
        variant: "quickstart",
        type=ff.Bool,
    )

ff.register_training_set(
    "fraud_training",
    label="fraudulent",
    features=["avg_transactions"],
)
