import os
import random

import requests
from behave import *
from dotenv import load_dotenv


@given("The Databricks env variables are available")
def step_impl(context):
    featureform_location = os.path.dirname(os.path.dirname(""))
    env_file_path = os.path.join(featureform_location, ".env")
    load_dotenv(env_file_path)

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
    context.s3_credentials = context.featureform.AWSCredentials(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "")
        ),

    context.s3_bucket_name = os.getenv("S3_BUCKET_PATH", "")
    context.s3_bucket_region = os.getenv("S3_BUCKET_REGION", "")

    if context.s3_bucket_name == "":
        raise Exception("S3 bucket name is not set")
    if context.s3_bucket_region == "":
        raise Exception("S3 bucket region is not set")


@when("I register Snowflake")
def step_impl(context):
    context.snowflake_name = "test_snowflake"
    try:
        context.snowflake = context.featureform.register_snowflake(
            name = context.snowflake_name,
            description = "Offline store",
            team = context.snowflake_team,
            username = context.snowflake_username,
            password = context.snowflake_password,
            account = context.snowflake_account,
            organization = context.snowflake_organization,
            database = "0884D0DD-468D-4C3A-8109-3C2BAAD72EF7",
            schema = "PUBLIC",
        )
    except Exception as e:
        context.exception = e

@when("I register Spark with Databricks S3")
def step_impl(context):
    context.snowflake_name = "test_spark"
    try:
        databricks = context.featureform.DatabricksCredentials(
            host=context.databricks_host,
            token=context.databricks_token,
            cluster_id=context.databricks_cluster_id,
        )

        s3 = context.featureform.register_s3(
            name="s3",
            credentials=context.s3_credentials,
            bucket_name=context.s3_bucket_name,
            path="",
            bucket_region= context.s3_bucket_region,
        )

        # Offline store
        context.spark = context.featureform.register_spark(
            name="spark_provider",
            description="A Spark deployment we created for the Featureform quickstart",
            team="featureform-team",
            executor=databricks,
            filestore=s3
        )
    except Exception as e:
        context.exception = e

@when('I register the tables from the database')
def step_impl(context):
    context.boolean_table = context.snowflake.register_table(
    name="boolean_table",
    table= "featureform_resource_feature__8a49dead-41a6-48c39ee7-7ed75d783fe4__0efdd949-f967-4796-8ed3-6c6f736344e8",
    )
    context.number_table = context.snowflake.register_table(
    name="number_table",
    table="featureform_resource_feature__f4d42278-0889-44d7-9928-8aef22d23c16__6a6e8ff4-8a8f-4217-8096-bb360ae1e99b",
    )

@when('I register the files from the database')
def step_impl(context):
    num = random.randint(0, 1000000)
    context.transactions = context.spark.register_file(
        name="transactions",
        variant=f"variant_{num}",
        description="A dataset of average transactions",
        file_path="s3://featureform-internal-sandbox/featureform/Materialization/avg_trans/databricks-webinar/2023-11-14-13-54-43-521232/part-00000-tid-3732459543582589042-bf8734da-26f4-47ea-9920-fe7bd4b4bc8c-201-1-c000.snappy.parquet"
    )

    context.balance = context.spark.register_file(
        name="balances",
        variant=f"variant_{num}",
        description="A dataset of balances",
        file_path="s3://featureform-internal-sandbox/featureform/Materialization/balance/databricks-webinar/2023-11-14-13-54-44-141964/part-00000-tid-6907362395273500345-7899298e-e815-42d8-9e7e-98b7cab7e9be-205-1-c000.snappy.parquet"
        )

    context.perc = context.spark.register_file(
        name="perc",
        variant=f"variant_{num}",
        description="A dataset of perc",
        file_path="s3://featureform-internal-sandbox/featureform/Materialization/perc/databricks-webinar/2023-11-14-13-54-45-900722/part-00000-tid-8883202944540201246-2c2cc6c1-7d6b-49a3-86b6-36576fadf072-207-1-c000.snappy.parquet"
    )


@then("I serve batch features {features}")
def step_impl(context, features):
    try:
        context.iter = context.client.iterate_feature_set(features)
    except Exception as e:
        context.exception = e
        return
    context.exception = None

@then("I can get a list containing the entity name and a tuple with all the features")
def step_impl(context):
    try:
        for row in context.iter:
            assert (len(row) == 2)
    except Exception as e:
        context.exception = e
        return
    context.exception = None


@when("I define a Snowflake User and register features")
def step_impl(context):
    class SnowflakeUser:
        context.boolean_feature = context.featureform.Feature(
            context.boolean_table[['entity',' value', 'ts']],
            variant="batch_serving_test_15",
            type=context.featureform.Boolean,
            inference_store=context.redis,
        )
        context.numerical_feature = context.featureform.Feature(
            context.number_table[['entity',' value', 'ts']],
            variant="batch_serving_test_15",
            type=context.featureform.Float32,
            inference_store=context.redis,
        )
        context.string_feature = context.featureform.Feature(
            context.number_table[['entity',' value', 'ts']],
            variant="batch_serving_test_15",
            type=context.featureform.String,
            inference_store=context.redis,
        )

    context.featureform.entity(SnowflakeUser)

@when("I define a Spark User and register features")
def step_impl(context):
    class SparkUser:
        context.transaction_feature = ff.Feature(
            transactions[['entity',' value', 'ts']],
            variant=f"variant_{num}",
            type=ff.Float32,
            inference_store=redis,
        )
        context.balance_feature = ff.Feature(
            balance[['entity',' value', 'ts']],
            variant=f"variant_{num}",
            type=ff.Float32,
            inference_store=redis,
        )
        context.perc_feature = ff.Feature(
            perc[['entity',' value', 'ts']],
            variant=f"variant_{num}",
            type=ff.String,
            inference_store=redis,
        )

    context.featureform.entity(SparkUser)
