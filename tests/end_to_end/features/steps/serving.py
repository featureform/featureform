import os
import random

import requests
import numpy as np
from behave import *
import featureform as ff
from collections import Counter


@given("The Snowflake env variables are available")
def step_impl(context):
    context.snowflake_username = os.getenv("SNOWFLAKE_USERNAME", "")
    context.snowflake_password = os.getenv("SNOWFLAKE_PASSWORD", "")
    context.snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    context.snowflake_organization = os.getenv("SNOWFLAKE_ORG", "")

    if context.snowflake_username == "":
        raise Exception("Snowflake username is not set")
    if context.snowflake_password == "":
        raise Exception("Snowflake password is not set")
    if context.snowflake_account == "":
        raise Exception("Snowflake account is not set")
    if context.snowflake_organization == "":
        raise Exception("Snowflake organization is not set")


@given("The Databricks env variables are available")
def step_impl(context):
    context.databricks_host = os.getenv("DATABRICKS_HOST", None)
    context.databrucks_token = os.getenv("DATABRICKS_TOKEN", None)
    context.databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER", None)

    if context.databricks_host is None:
        raise Exception("Databricks host is not set")
    if context.databrucks_token is None:
        raise Exception("Databricks token is not set")
    if context.databricks_cluster_id is None:
        raise Exception("Databricks cluster id is not set")


@given("The S3 env variables are available")
def step_impl(context):
    context.s3_credentials = (
        ff.AWSCredentials(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        ),
    )

    context.s3_bucket_name = os.getenv("S3_BUCKET_PATH", "")
    context.s3_bucket_region = os.getenv("S3_BUCKET_REGION", "")

    if context.s3_bucket_name == "":
        raise Exception("S3 bucket name is not set")
    if context.s3_bucket_region == "":
        raise Exception("S3 bucket region is not set")


@when("I register Spark with Databricks S3")
def step_impl(context):
    context.snowflake_name = "test_spark"
    databricks = ff.DatabricksCredentials(
        host=context.databricks_host,
        token=context.databricks_token,
        cluster_id=context.databricks_cluster_id,
    )

    s3 = ff.register_s3(
        name="s3",
        credentials=context.s3_credentials,
        bucket_name=context.s3_bucket_name,
        path="",
        bucket_region=context.s3_bucket_region,
    )

    # Offline store
    context.spark = ff.register_spark(
        name="spark_provider",
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=databricks,
        filestore=s3,
    )
    context.client.apply()


@when("I register Snowflake")
def step_impl(context):
    context.snowflake = ff.register_snowflake(
        name="test_snowflake",
        description="Offline store",
        team="Featureform",
        username=context.snowflake_username,
        password=context.snowflake_password,
        account=context.snowflake_account,
        organization=context.snowflake_organization,
        database="0884D0DD-468D-4C3A-8109-3C2BAAD72EF7",
        schema="PUBLIC",
    )
    context.client.apply()


@when("I register the tables from the database")
def step_impl(context):
    context.boolean_table = context.snowflake.register_table(
        name="boolean_table",
        table="featureform_resource_feature__08b1cc23-18ce-4ae7-9ee0-d68216f19079__2e2a8e99-7a60-4e10-98e2-1d17e44ba476",
    )
    context.number_table = context.snowflake.register_table(
        name="number_table",
        table="featureform_resource_feature__1926ce54-6d29-4094-a291-6f6516d84eed__b63c0ba7-23d8-437d-bbc9-bb0f2c821f0c",
    )
    context.string_table = context.snowflake.register_table(
        name="string_table",
        table="featureform_materialization_string_feature",
    )
    context.client.apply()


@when("I register the files from the database")
def step_impl(context):
    context.transactions = context.spark.register_file(
        name="transactions",
        description="A dataset of average transactions",
        file_path="s3://featureform-spark-testing/data/avg_trans.snappy.parquet",
    )

    context.balance = context.spark.register_file(
        name="balances",
        description="A dataset of balances",
        file_path="s3://featureform-spark-testing/data/balance.snappy.parquet",
    )

    context.perc = context.spark.register_file(
        name="perc",
        description="A dataset of perc",
        file_path="s3://featureform-spark-testing/data/perc.snappy.parquet",
    )
    context.client.apply()


@then("I serve batch features for snowflake")
def step_impl(context):
    context.expected = [
        ("a", ["", 343, "343"]),
        ("b", [True, 546, "546"]),
        ("c", [True, 7667, "7667"]),
        ("d", [False, 32, "32"]),
        ("e", [True, 53, "53"]),
        ("f", ["", 64556, "64556"]),
    ]
    context.iter = context.client.batch_features(
        [
            ("boolean_feature", ff.get_run()),
            ("numerical_feature", ff.get_run()),
            ("string_feature", ff.get_run()),
        ]
    )


@then("I serve batch features for spark")
def step_impl(context):
    context.expected = [
        ("C1010012", [1499.0, "24204.49", 0.06193065832000591]),
        ("C1010024", [5000.0, "87058.65", 0.05743254690946851]),
        ("C1010039", [915.0, "11027.18", 0.08297679007688276]),
        ("C1010068", [546.0, "46741.73", 0.011681210772472478]),
        ("C1010081", [1661.3333333333333, "1584.18", 0.2708025603151157]),
        ("C1010085", [225.0, "319080.2", 0.0007051518709089439]),
    ]
    context.iter = context.client.batch_features(
        [
            ("transaction_feature", ff.get_run()),
            ("balance_feature", ff.get_run()),
            ("perc_feature", ff.get_run()),
        ]
    )


@then(
    "I serve batch features for spark with submit params that exceed the 10K-byte API limit"
)
def step_impl(context):
    context.expected = 30
    context.iter = context.client.batch_features(
        [
            *([("transaction_feature", ff.get_run())] * 10),
            *([("balance_feature", ff.get_run())] * 10),
            *([("perc_feature", ff.get_run())] * 10),
        ]
    )


@then("I can get a list containing the entity name and a tuple with all the features")
def step_impl(context):
    i = 0
    for entity, features in context.iter:
        if i >= len(context.expected):
            break
        print(entity, features)
        assert entity == context.expected[i][0]
        assert Counter(features) == Counter(context.expected[i][1])
        i += 1


@then("I can get a list containing the correct number of features")
def step_impl(context):
    i = 0
    for entity, features in context.iter:
        if i >= context.expected:
            break
        print(entity, features)
        assert len(features) == context.expected
        i += 1


@when("I define a SnowflakeUser and register features")
def step_impl(context):
    @ff.entity
    class SnowflakeUser:
        boolean_feature = ff.Feature(
            context.boolean_table[["entity", " value", "ts"]],
            type=ff.Bool,
        )
        numerical_feature = ff.Feature(
            context.number_table[["entity", " value", "ts"]],
            type=ff.Float32,
        )
        string_feature = ff.Feature(
            context.string_table[["entity", " value", "ts"]],
            type=ff.String,
        )

    context.client.apply()


@when("I define a SparkUser and register features")
def step_impl(context):
    @ff.entity
    class SparkUser:
        transaction_feature = ff.Feature(
            context.transactions[["entity", " value", "ts"]],
            type=ff.Float32,
        )
        balance_feature = ff.Feature(
            context.balance[["entity", " value", "ts"]],
            type=ff.String,
        )
        perc_feature = ff.Feature(
            context.perc[["entity", " value", "ts"]],
            type=ff.Float32,
        )

    context.client.apply()
