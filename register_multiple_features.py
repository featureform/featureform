import featureform as ff

num = 3458480

client = ff.Client(host="localhost:7878", insecure=True)

# POSTGRES store
postgres = ff.register_postgres(
    name="postgres-quickstart",
    host="host.docker.internal",  # The docker dns name for postgres
    port="5432",
    user="postgres",
    password="password",
    database="postgres",
)

# Online store
redis = ff.register_redis(
    name="redis-quickstart",
    host="host.docker.internal",  # The docker dns name for redis
    port=6379,
)

# POSTGRES table
transaction = postgres.register_table(
    name="transaction",
    variant=f"variant_{num}",
    table="transactions",  # This is the table's name in Postgres
)



@ff.entity
class User:
    # SINGLE TRANSACTION FEATURE
    table2_feature = ff.Feature(
        transaction[["customerid", "custlocation", "timestamp"]],
        variant=f"variant_{num}",
        type=ff.String,
        inference_store=redis,
    )

client.apply()