import featureform as ff


clickhouse = ff.register_clickhouse(
    name="clickhouse-quickstart",
    host="host.docker.internal",  # The docker dns name for ClickHouse
    port=9000,
    user="default",
    password="",
    database="default",
)


redis = ff.register_redis(
    name="redis-quickstart",
    host="host.docker.internal",  # The docker dns name for redis
    port=6379,
)


transactions = clickhouse.register_table(
    name="transactions",
    table="transactions",  # This is the table's name in ClickHouse
)


@clickhouse.sql_transformation(inputs=[transactions])
def average_user_transaction(tr):
    return (
        "SELECT CustomerID as user_id, avg(TransactionAmount) "
        "as avg_transaction_amt from {{tr}} GROUP BY user_id"
    )


@ff.entity
class User:
    avg_transactions = ff.Feature(
        average_user_transaction[
            ["user_id", "avg_transaction_amt"]
        ],  # We can optional include the `timestamp_column` "timestamp" here
        variant="quickstart",
        type=ff.Float32,
        inference_store=redis,
    )

    fraudulent = ff.Label(
        transactions[["CustomerID", "IsFraud"]], 
        variant="quickstart", 
        type=ff.Bool,
    )


ff.register_training_set(
    name="fraud_training",
    label=User.fraudulent,
    features=[User.avg_transactions],
    variant="quickstart",
)



