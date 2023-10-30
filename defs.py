import featureform as ff
import os
from dotenv import load_dotenv

load_dotenv(".env")
ff.set_run()

spark = ff.register_spark(
    name="spark",
    executor=ff.DatabricksCredentials(
        host=os.getenv("DATABRICKS_HOST", None),
        token=os.getenv("DATABRICKS_TOKEN", None),
        cluster_id=os.getenv("DATABRICKS_CLUSTER", None),
    ),
    filestore=ff.register_blob_store(
        name=f"azure",
        account_name=os.getenv("AZURE_ACCOUNT_NAME", None),
        account_key=os.getenv("AZURE_ACCOUNT_KEY", None),
        container_name="test",
        root_path="behave",
    ),
)

redis = ff.register_redis(
    name="redis-quickstart",
    host="host.docker.internal",  # The docker dns name for redis
    port=6379,
    password="",
)

transactions = spark.register_file(
    name="transactions",
    file_path="abfss://test@testingstoragegen.dfs.core.windows.net/data/transactions_short.csv",
)


@spark.df_transformation(inputs=[transactions])
def average_user_transaction(df):
    from pyspark.sql.functions import avg

    df.groupBy("CustomerID").agg(
        avg("TransactionAmount").alias("average_user_transaction")
    )
    return df


user = ff.register_entity("user")
# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="CustomerID",
    inference_store=redis,
    features=[
        {
            "name": "avg_transactions",
            "column": "TransactionAmount",
            "type": "float32",
        },
    ],
)
