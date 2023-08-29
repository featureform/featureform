from behave import *
from featureform import register_spark
import featureform as ff
import os


@when("I generate a random variant name")
def step_impl(context):
    ff.set_run()


@when("I register Spark")
def step_impl(context):
    context.spark_name = "test_spark"
    try:
        context.spark = register_spark(
            name=context.spark_name,
            executor=context.spark_credentials,
            filestore=context.cloud_credentials,
        )
    except Exception as e:
        context.exception = e


@when("I register redis")
def step_impl(context):
    context.redis = ff.register_redis(
        name="redis-quickstart",
        host="host.docker.internal",  # The docker dns name for redis
        port=6379,
    )
    context.client.apply()


@when("I register databricks")
def step_impl(context):
    from dotenv import load_dotenv

    load_dotenv("../../.env")
    databricks = ff.DatabricksCredentials(
        host=os.getenv("DATABRICKS_HOST", None),
        token=os.getenv("DATABRICKS_TOKEN", None),
        cluster_id=os.getenv("DATABRICKS_CLUSTER", None),
    )

    azure_blob = ff.register_blob_store(
        name="k8s",
        account_name=os.getenv("AZURE_ACCOUNT_NAME", None),
        account_key=os.getenv("AZURE_ACCOUNT_KEY", None),
        container_name=os.getenv("AZURE_CONTAINER_NAME", None),
        path="end_to_end_tests/behave",
    )

    spark = ff.register_spark(
        name="spark",
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=databricks,
        filestore=azure_blob.config(),
    )
    context.spark_name = "test_spark"
    try:
        context.spark = spark
        context.client.apply()
    except Exception as e:
        context.exception = e


@when("I register the file")
def step_impl(context):
    context.file = context.spark.register_file(
        name="transactions",
        file_path=f"abfss://test@testingstoragegen.dfs.core.windows.net/{context.filename}",
    )
    context.client.apply()


@then("I should be able to pull the file as a dataframe")
def step_impl(context):
    df = context.client.dataframe(context.file)
    assert (
        len(df) == context.file_length
    ), f"Expected {context.file_length} rows, got {len(df)} rows"
    print(len(df))
    print(df)


@when("I register a transformation")
def step_impl(context):
    @context.spark.df_transformation(
        name="transactions_transformation",
        inputs=[context.file],
    )
    def ice_cream_transformation(df):
        """Unedited transactions"""
        return df

    context.transformation = ice_cream_transformation
    context.client.apply()


@then("I should be able to pull the transformation as a dataframe")
def step_impl(context):
    df = context.client.dataframe(context.transformation)
    assert (
        len(df) == context.file_length
    ), f"Expected {context.file_length} rows, got {len(df)} rows"
    print(len(df))
    print(df)


@when("I register a feature")
def step_impl(context):
    @ff.entity
    class User:
        avg_transactions = ff.Feature(
            context.transformation[["CustomerID", "TransactionAmount"]],
            type=ff.Float32,
            inference_store=context.redis,
        )

    context.feature_name = "avg_transactions"
    context.client.apply()


@then("I should be able to pull the feature as a dataframe")
def step_impl(context):
    feature = context.client.features(
        [(context.feature_name, ff.get_run())], {"user": "C1578767"}
    )
    print("FEATURE VALUE: ", feature)


@when("I register a label")
def step_impl(context):
    @ff.entity
    class User:
        fraudulent = ff.Label(
            context.transformation[["CustomerID", "IsFraud"]],
            type=ff.Bool,
        )

    context.label_name = "fraudulent"
    context.client.apply()


@when("I register a training set")
def step_impl(context):
    ff.register_training_set(
        "fraud_training",
        label=(context.label_name, ff.get_run()),
        features=[(context.feature_name, ff.get_run())],
    )
    context.client.apply()


@then("I should be able to pull the trainingset as a dataframe")
def step_impl(context):
    dataset = context.client.training_set("fraud_training", ff.get_run())
    df = dataset.dataframe()
    print(len(df))
    print(df)
