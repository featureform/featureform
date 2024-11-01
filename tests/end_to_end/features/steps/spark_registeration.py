#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import logging
import os
import time
import random
import string
from time import sleep

import featureform as ff
from behave import when, then, step
from dotenv import load_dotenv
from featureform import GlueCatalog, register_spark


@when("I generate a random variant name")
def step_impl(context):
    run_id = "".join(random.choice(string.ascii_lowercase) for _ in range(15))

    ff.set_run(run_id)
    context.variant = run_id


#   ff.set_variant_prefix(run_id)


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


@when("I get or register redis")
def step_impl(context):
    name = "redis-quickstart"
    try:
        redis = context.client.get_redis(name)
        context.redis = redis
        return
    except Exception as e:
        if "Key Not Found" not in str(e):
            print("Exception:", e)
            raise e

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
    run = ff.get_run()

    if root_path == "empty":
        root_path = ""

    context.filestore = storage_provider
    if storage_provider == "azure":
        storage_provider_name = f"azure-{run}"
        storage_provider = ff.register_blob_store(
            name=context.storage_provider_name,
            account_name=os.getenv("AZURE_ACCOUNT_NAME", None),
            account_key=os.getenv("AZURE_ACCOUNT_KEY", None),
            container_name=bucket,
            root_path=root_path,
        )

    elif storage_provider == "s3":
        storage_provider_name = f"s3-{run}"
        storage_provider = ff.register_s3(
            name=f"s3-{run}",
            credentials=ff.AWSStaticCredentials(
                access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
                secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
            ),
            bucket_name=bucket,
            bucket_region=os.getenv("S3_BUCKET_REGION", "us-east-2"),
            path=root_path,
        )

    elif storage_provider == "gcs":
        storage_provider_name = f"gcs-{run}"
        storage_provider = ff.register_gcs(
            name=context.storage_provider_name,
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

    context.storage_provider_name = storage_provider_name
    context.storage_provider = storage_provider
    try:
        context.client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        context.exception = e


@when("I register databricks")
def step_impl(context):
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
        print("Exception:", e)
        raise e


@when("I get or register databricks")
def step_impl(context):
    name = f"databricks-{context.filestore}-{ff.get_run()}"
    context.spark_name = name

    try:
        spark = context.client.get_spark(name)
        context.spark = spark
        return
    except Exception as e:
        if "Key Not Found" not in str(e):
            print("Exception:", e)
            raise e

    load_dotenv("../../.env")
    databricks = ff.DatabricksCredentials(
        host=os.getenv("DATABRICKS_HOST", None),
        token=os.getenv("DATABRICKS_TOKEN", None),
        cluster_id=os.getenv("DATABRICKS_CLUSTER", None),
    )

    storage_provider = None
    try:
        print(f"Getting {context.filestore} filestore {context.storage_provider_name}")
        if context.filestore == "azure":
            storage_provider = context.client.get_blob_store(
                context.storage_provider_name
            )
        elif context.filestore == "s3":
            storage_provider = context.client.get_s3(context.storage_provider_name)
        elif context.filestore == "gcs":
            storage_provider = context.client.get_gcs(context.storage_provider_name)
        else:
            raise NotImplementedError(
                f"Storage provider {context.filestore} is not implemented"
            )
    except Exception as e:
        print("Exception getting storage provider:", e)
        if "Key Not Found" not in str(e):
            print("Exception:", e)
            raise e

    if storage_provider is None:
        raise ValueError(
            f"Storage provider {context.storage_provider_name} ({context.filestore}) not found"
        )
    else:
        print(f"Got {context.filestore} filestore {context.storage_provider_name}")

    spark = ff.register_spark(
        name=name,
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=databricks,
        filestore=storage_provider,
    )
    try:
        context.spark = spark
        context.client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        context.exception = e
        print("Exception:", e)
        raise e


@when("I get spark")
def step_impl(context):
    context.spark = context.client.get_spark(context.spark_name)


@when("I register EMR with glue")
def step_impl(context):
    from dotenv import load_dotenv

    run = ff.get_run()

    load_dotenv("../../.env")
    name = f"emr-{context.filestore}-{run}"
    emr = ff.EMRCredentials(
        emr_cluster_id=os.getenv("AWS_EMR_CLUSTER_ID", None),
        emr_cluster_region=os.getenv("AWS_EMR_CLUSTER_REGION", "us-east-1"),
        credentials=ff.AWSStaticCredentials(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
        ),
    )

    catalog = GlueCatalog(
        warehouse=os.getenv(
            "GLUE_WAREHOUSE", "s3://ali-aws-lake-house-iceberg-blog-demo/demo2/"
        ),
        database="featureform_db",
        region="us-east-1",
    )

    spark = ff.register_spark(
        name=name,
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=emr,
        filestore=context.storage_provider,
        catalog=catalog,
    )

    context.spark_name = name
    try:
        context.spark = spark
        context.client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        context.exception = e


@when("I get or register EMR with glue")
def step_impl(context):
    name = f"emr-{context.filestore}"
    context.spark_name = name

    try:
        spark = context.client.get_spark(name)
        context.spark = spark
        return
    except Exception as e:
        if "Key Not Found" not in str(e):
            print("Exception:", e)
            raise e

    load_dotenv("../../.env")

    emr = ff.EMRCredentials(
        emr_cluster_id=os.getenv("AWS_EMR_CLUSTER_ID", None),
        emr_cluster_region=os.getenv("AWS_EMR_CLUSTER_REGION", "us-east-1"),
        credentials=ff.AWSStaticCredentials(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
        ),
    )

    catalog = GlueCatalog(
        warehouse=os.getenv(
            "GLUE_WAREHOUSE", "s3://ali-aws-lake-house-iceberg-blog-demo/demo2/"
        ),
        database="featureform_db",
        region="us-east-1",
    )

    spark = ff.register_spark(
        name=name,
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=emr,
        filestore=context.storage_provider,
        catalog=catalog,
    )

    try:
        context.spark = spark
        context.client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        context.exception = e
        print("Exception:", e)
        raise e


@step("I get or register EMR with S3")
def step_impl(context):
    name = "emr-s3-no-glue"
    context.spark_name = name

    try:
        spark = context.client.get_spark(name)
        context.spark = spark
        return
    except Exception as e:
        if "Key Not Found" not in str(e):
            print("Exception:", e)
            raise e

    load_dotenv("../../.env")

    aws_creds = ff.AWSStaticCredentials(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    emr = ff.EMRCredentials(
        credentials=aws_creds,
        emr_cluster_id=os.getenv("AWS_EMR_CLUSTER_ID"),
        emr_cluster_region=os.getenv("AWS_EMR_CLUSTER_REGION"),
    )

    s3 = ff.register_s3(
        name="s3-no-glue",
        description="S3 bucket for development",
        credentials=aws_creds,
        bucket_name=os.getenv("S3_BUCKET_PATH"),
        bucket_region=os.getenv("S3_BUCKET_REGION"),
        path="",
    )

    spark = ff.register_spark(
        name=name,
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=emr,
        filestore=s3,
    )

    try:
        context.spark = spark
        context.client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        context.exception = e
        print("Exception:", e)
        raise e


@when("I register the file")
def step_impl(context):
    context.file = context.spark.register_file(
        name="transactions",
        file_path=context.cloud_file_path,
    )
    context.client.apply(asynchronous=False, verbose=True)


@when("I register an iceberg table")
def step_impl(context):
    trx = context.spark.register_iceberg_table(
        name="transactions",
        table="transactions",
        database="ff",
    )

    context.primary = trx
    context.transactions = trx

    context.file_length = 10_000

    context.client.apply(asynchronous=False, verbose=True)


@when("I register a delta lake table")
def step_impl(context):
    trx = context.spark.register_delta_table(
        name="transactions",
        table="transactions",
        database="delta_poc",
    )

    context.primary = trx
    context.transactions = trx

    context.file_length = 10_000

    context.client.apply(asynchronous=False, verbose=True)


@when("I register transactions_short.csv")
def step_impl(context):
    context.transactions = context.spark.register_file(
        name="transactions",
        file_path="s3://featureform-spark-testing/data/transactions_short.csv",
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
    #  source_list = [ff.get_source(s, ff.get_run()) for s in source_list]
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


@when(
    'I register a "{transformation_type}" transformation named "{name}" from "{sources}" (v2)'
)
def step_impl(context, transformation_type, name, sources):
    source_list = sources.split(",")
    # get sources as objects from context
    source_list = [getattr(context, s) for s in source_list]
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
            inputs=source_list,
        )
        def some_transformation(df):
            """Unedited transactions"""
            return "SELECT * FROM {{ df }}"

    context.transformation = some_transformation
    context.client.apply(asynchronous=False, verbose=True)


@then("I should be able to pull the transformation as a dataframe")
def step_impl(context):
    df = context.client.dataframe(
        context.transformation,
    )
    assert (
        len(df) == context.file_length
    ), f"Expected {context.file_length} rows, got {len(df)} rows."


@then("I should be able to pull the iceberg transformation as a dataframe")
def step_impl(context):
    df = context.client.dataframe(
        context.transformation,
        iceberg=True,
    )
    assert (
        len(df) == context.file_length
    ), f"Expected {context.file_length} rows, got {len(df)} rows."


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


@when('I register a feature from a "{source_type}" (v2)')
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
                context.primary[["CustomerID", "TransactionAmount"]],
                type=ff.Float32,
                inference_store=context.redis,
            )

    context.client.apply(asynchronous=False, verbose=True)
    context.feature = User.avg_transactions


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


@when('I register "{label_name}" label from a "{source_type}" (v2)')
def step_impl(context, label_name, source_type):
    if source_type == "transformation":

        @ff.entity
        class User:
            fraudulent = ff.Label(
                context.transformation[["CustomerID", "IsFraud"]],
                type=ff.Bool,
                name=label_name,
            )

    elif source_type == "primary":

        @ff.entity
        class User:
            fraudulent = ff.Label(
                context.primary[["CustomerID", "IsFraud"]],
                type=ff.Bool,
                name=label_name,
            )

    context.client.apply(asynchronous=False, verbose=True)
    context.label = User.fraudulent


@when("I register a training set")
def step_impl(context):
    ff.register_training_set(
        "fraud_training",
        label=(context.label_name, ff.get_run()),
        features=[(context.feature_name, ff.get_run())],
    )
    context.client.apply(asynchronous=False, verbose=True)


@when('I register "{training_set_name}" training set (v2)')
def step_impl(context, training_set_name):
    ts = ff.register_training_set(
        training_set_name,
        label=context.label,
        features=[context.feature],
    )

    context.client.apply(asynchronous=False, verbose=True)
    context.training_set = ts


@then("I should be able to pull the trainingset as a dataframe")
def step_impl(context):
    dataset = context.client.training_set("fraud_training", ff.get_run())
    context.training_set_dataframe = dataset.dataframe()


@then("I should be able to pull the trainingset as a dataframe (v2)")
def step_impl(context):
    dataset = context.client.training_set(context.training_set)
    context.training_set_dataframe = dataset.dataframe()
