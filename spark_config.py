import random
import featureform as ff
from featureform import ServingClient

num = random.randint(0, 1000000)

databricks = ff.DatabricksCredentials(
    host="SOME_HOST",
    token="SOME_TOKEN",
    cluster_id="SOME_CLUSTER_ID",
)

s3 = ff.register_s3(
    name="s3",
    credentials="SOME_CREDENTIALS",
    bucket_name="SOME_SANDBOX",
    path="",
    bucket_region="SOME_REGION"
)

# Offline store
spark = ff.register_spark(
    name="spark_provider",
    description="A Spark deployment we created for the Featureform quickstart",
    team="featureform-team",
    executor=databricks,
    filestore=s3
)

# Online store
redis = ff.register_redis(
    name = "redis-quickstart",
    host="host.docker.internal", # The docker dns name for redis
    port=6379,
)

transactions = spark.register_parquet_file(
    name="transactions",
    variant=f"variant_{num}",
    description="A dataset of average transactions",
    file_path="SOME_PATH"
)

balance = spark.register_file(
    name="balances",
    variant=f"variant_{num}",
    description="A dataset of balances",
    file_path="SOME_PATH"
    )

perc = spark.register_file(
    name="perc",
    variant=f"variant_{num}",
    description="A dataset of perc",
    file_path="SOME_PATH"
)

@ff.entity
class User:
    transaction_feature = ff.Feature(
        transactions[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.Float32,
        inference_store=redis,
    )
    balance_feature = ff.Feature(
        balance[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.Float32,
        inference_store=redis,
    )
    perc_feature = ff.Feature(
        perc[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.String,
        inference_store=redis,
    )