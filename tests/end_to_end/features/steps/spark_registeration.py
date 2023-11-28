import os
import time
import random
import string
from time import sleep

from behave import *
import featureform as ff
from featureform import register_spark


@when("I generate a random variant name")
def step_impl(context):
    run_id = "".join(random.choice(string.ascii_lowercase) for _ in range(10))

    ff.set_variant_prefix(run_id)


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
        host="172.17.0.1",  # The docker dns name for redis
        port=6379,
    )
    context.client.apply()


@when(
    'I register "{storage_provider}" filestore with bucket "{bucket}" and root path "{root_path}"'
)
def step_impl(context, storage_provider, bucket, root_path):
    from dotenv import load_dotenv

    run = ff.get_run()

    if root_path == "empty":
        root_path = ""

    context.filestore = storage_provider
    if storage_provider == "azure":
        context.storage_provider = ff.register_blob_store(
            name=f"azure-{run}",
            account_name=os.getenv("AZURE_ACCOUNT_NAME", None),
            account_key=os.getenv("AZURE_ACCOUNT_KEY", None),
            container_name=bucket,
            root_path=root_path,
        )

    elif storage_provider == "s3":
        context.storage_provider = ff.register_s3(
            name=f"s3-{run}",
            credentials=ff.AWSCredentials(
                access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
                secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
            ),
            bucket_name=bucket,
            bucket_region="us-east-2",
            path=root_path,
        )

    elif storage_provider == "gcs":
        context.storage_provider = ff.register_gcs(
            name=f"gcs-{run}",
            credentials=ff.GCPCredentials(
                project_id=os.getenv("GCP_PROJECT_ID"),
                credentials_path=os.getenv("GCP_CREDENTIALS_FILE"),
            ),
            bucket_name=bucket,
            root_path=root_path,
        )

    else:
        raise NotImplementedError(
            f"Storage provider {storage_provider} is not implemented"
        )


@when("I register databricks")
def step_impl(context):
    from dotenv import load_dotenv

    run = ff.get_run()

    load_dotenv("../../.env")
    databricks = ff.DatabricksCredentials(
        host=os.getenv("DATABRICKS_HOST", None),
        token=os.getenv("DATABRICKS_TOKEN", None),
        cluster_id=os.getenv("DATABRICKS_CLUSTER", None),
    )

    name = f"databricks-{context.filestore}-{run}"
    spark = ff.register_spark(
        name=name,
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=databricks,
        filestore=context.storage_provider,
    )
    context.spark_name = name
    try:
        context.spark = spark
        context.client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        context.exception = e


@when("I register the file")
def step_impl(context):
    context.file = context.spark.register_file(
        name="transactions",
        file_path=context.cloud_file_path,
    )
    context.client.apply(asynchronous=False, verbose=True)


@then("I should be able to pull the file as a dataframe")
def step_impl(context):
    try:
        df = context.client.dataframe(context.file)
    except Exception as e:
        context.exception = e
        return
    assert (
        len(df) == context.file_length
    ), f"Expected {context.file_length} rows, got {len(df)} rows"
    context.exception = None


@when(
    'I register a "{transformation_type}" transformation named "{name}" from "{sources}"'
)
def step_impl(context, transformation_type, name, sources):
    source_list = sources.split(",")
    if transformation_type == "DF":

        @context.spark.df_transformation(
            name=name,
            inputs=source_list,
        )
        def some_transformation(df):
            """Unedited transactions"""
            return df

    elif transformation_type == "SQL":

        @context.spark.sql_transformation(
            name=name,
        )
        def some_transformation():
            """Unedited transactions"""
            return "SELECT * FROM {{ transactions }}"

    context.transformation = some_transformation
    context.client.apply(asynchronous=False, verbose=True)


@then("I should be able to pull the transformation as a dataframe")
def step_impl(context):
    df = context.client.dataframe(
        context.transformation,
    )
    assert (
        len(df) == context.file_length
    ), f"Expected {context.file_length} rows, got {len(df)} rows"


@when('I register a feature from a "{source_type}"')
def step_impl(context, source_type):
    if source_type == "transformation":

        @ff.entity
        class User:
            avg_transactions = ff.Feature(
                context.transformation[["CustomerID", "TransactionAmount"]],
                type=ff.Float32,
                inference_store=context.redis,
            )

    elif source_type == "primary":

        @ff.entity
        class User:
            avg_transactions = ff.Feature(
                context.file[["CustomerID", "TransactionAmount"]],
                type=ff.Float32,
                inference_store=context.redis,
            )

    context.feature_name = "avg_transactions"
    context.client.apply(asynchronous=False, verbose=True)


@then("I should be able to pull the feature as a dataframe")
def step_impl(context):
    feature = context.client.features(
        [(context.feature_name, ff.get_run())], {"user": "C1578767"}
    )


@when('I register a label from a "{source_type}"')
def step_impl(context, source_type):
    if source_type == "transformation":

        @ff.entity
        class User:
            fraudulent = ff.Label(
                context.transformation[["CustomerID", "IsFraud"]],
                type=ff.Bool,
            )

    elif source_type == "primary":

        @ff.entity
        class User:
            fraudulent = ff.Label(
                context.file[["CustomerID", "IsFraud"]],
                type=ff.Bool,
            )

    context.label_name = "fraudulent"
    context.client.apply(asynchronous=False, verbose=True)


@when("I register a training set")
def step_impl(context):
    ff.register_training_set(
        "fraud_training",
        label=(context.label_name, ff.get_run()),
        features=[(context.feature_name, ff.get_run())],
    )
    context.client.apply(asynchronous=False, verbose=True)


@then("I should be able to pull the trainingset as a dataframe")
def step_impl(context):
    dataset = context.client.training_set("fraud_training", ff.get_run())
    context.training_set_dataframe = dataset.dataframe()
