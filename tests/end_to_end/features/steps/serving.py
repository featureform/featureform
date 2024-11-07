#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import random

import requests
import numpy as np
from behave import given, when, then, step
import featureform as ff
from collections import Counter


@given("The Snowflake env variables are available")
def step_impl(context):
    context.snowflake_username = os.getenv("SNOWFLAKE_USERNAME", "")
    context.snowflake_password = os.getenv("SNOWFLAKE_PASSWORD", "")
    context.snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    context.snowflake_organization = os.getenv("SNOWFLAKE_ORG", "")
    context.snowflake_external_volume = os.getenv("SNOWFLAKE_EXTERNAL_VOLUME", "")
    context.snowflake_base_location = os.getenv("SNOWFLAKE_BASE_LOCATION", "")
    context.snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "")

    if context.snowflake_username == "":
        raise Exception("Snowflake username is not set")
    if context.snowflake_password == "":
        raise Exception("Snowflake password is not set")
    if context.snowflake_account == "":
        raise Exception("Snowflake account is not set")
    if context.snowflake_organization == "":
        raise Exception("Snowflake organization is not set")
    if context.snowflake_external_volume == "":
        raise Exception("Snowflake external volume is not set")
    if context.snowflake_base_location == "":
        raise Exception("Snowflake base location is not set")
    if context.snowflake_warehouse == "":
        raise Exception("Snowflake warehouse is not set")


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
        ff.AWSStaticCredentials(
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
        database="DEMO2",
        schema="PUBLIC",
        warehouse=context.snowflake_warehouse,
        catalog=ff.SnowflakeCatalog(
            external_volume=context.snowflake_external_volume,
            base_location=context.snowflake_base_location,
        ),
    )
    context.client.apply()


@step('I register Snowflake with "{db_name}" database')
def step_impl(context, db_name):
    context.snowflake = ff.register_snowflake(
        name=f"snowflake_{db_name}",
        description="Offline store",
        team="Featureform",
        username=context.snowflake_username,
        password=context.snowflake_password,
        account=context.snowflake_account,
        organization=context.snowflake_organization,
        database=db_name,
        schema="PUBLIC",
        warehouse=context.snowflake_warehouse,
        catalog=ff.SnowflakeCatalog(
            external_volume=context.snowflake_external_volume,
            base_location=context.snowflake_base_location,
        ),
    )
    context.client.apply()


@when("I register the tables from the database")
def step_impl(context):
    context.boolean_table = context.snowflake.register_table(
        name="boolean_table",
        table="CICD_BOOLEAN_TABLE",
    )
    context.number_table = context.snowflake.register_table(
        name="number_table",
        table="CICD_NUMBER_TABLE",
    )
    context.string_table = context.snowflake.register_table(
        name="string_table",
        table="CICD_STRING_TABLE",
    )
    context.client.apply()


@step('I register the "{table_name}" table with Snowflake')
def step_impl(context, table_name):
    context.snowflake_table = context.snowflake.register_table(
        name=f"snowflake_{table_name.lower()}",
        table=table_name,
    )
    context.client.apply()


@step(
    'I serve the snowflake table with client dataframe with expected "{num_records}" records'
)
def step_impl(context, num_records):
    expected_num_records = int(num_records)
    df = context.client.dataframe(context.snowflake_table)
    assert df is not None, "Dataframe is None"
    assert len(df) > 0, "Dataframe is empty"
    assert len(df.columns) > 0, "Dataframe has no columns"
    assert (
        len(df) == expected_num_records
    ), f"Dataframe has incorrect number of rows; expected {expected_num_records}, got {len(df)}"


@step(
    'I can create spark "{transformation_type}" transformation with Snowflake table as source'
)
def step_impl(context, transformation_type):
    if transformation_type == "sql":

        @context.spark.sql_transformation(inputs=[context.snowflake_table])
        def snowflake_transformation(table):
            return "SELECT * FROM {{ table }}"

    elif transformation_type == "df":

        @context.spark.df_transformation(inputs=[context.snowflake_table])
        def snowflake_transformation(df):
            return df

    context.client.apply()
    context.transformation = snowflake_transformation


@step("I can create sql transformation with FF_LAST_RUN_TIMESTAMP")
def step_impl(context):
    @context.snowflake.sql_transformation(inputs=[context.snowflake_table])
    def variable_transformation(table):
        return "SELECT CUSTOMERID FROM {{ table }} WHERE CAST(timestamp AS TIMESTAMP_NTZ(6)) > CAST(FF_LAST_RUN_TIMESTAMP AS TIMESTAMP_NTZ(6))"

    context.client.apply()
    context.transformation = variable_transformation


@step("I can serve the transformation with client dataframe")
def step_impl(context):
    expected_num_records = 1048124
    df = context.client.dataframe(context.transformation)
    assert df is not None, "Dataframe is None"
    assert len(df) > 0, "Dataframe is empty"
    assert len(df.columns) > 0, "Dataframe has no columns"
    assert (
        len(df) == expected_num_records
    ), f"Dataframe has incorrect number of rows; expected {expected_num_records}, got {len(df)}"


@when('I register the "{data_source_size}" files from the database')
def step_impl(context, data_source_size):
    if data_source_size == "short":
        source_0 = "s3a://featureform-spark-testing/data/avg_trans_short"
        source_1 = "s3a://featureform-spark-testing/data/balance_short"
        source_2 = "s3a://featureform-spark-testing/data/perc_short"
    elif data_source_size == "long":
        source_0 = "s3a://featureform-spark-testing/data/avg_trans.snappy.parquet"
        source_1 = "s3a://featureform-spark-testing/data/balance.snappy.parquet"
        source_2 = "s3a://featureform-spark-testing/data/perc.snappy.parquet"
    else:
        raise Exception("Data source size not recognized", data_source_size)

    context.transactions = context.spark.register_file(
        name="transactions",
        description="A dataset of average transactions",
        file_path=source_0,
    )

    context.balance = context.spark.register_file(
        name="balances",
        description="A dataset of balances",
        file_path=source_1,
    )

    context.perc = context.spark.register_file(
        name="perc",
        description="A dataset of perc",
        file_path=source_2,
    )

    context.client.apply()


@then("I serve batch features for snowflake")
def step_impl(context):
    context.expected = {
        "C9038530": [True, 1, "8/12/89"],
        "C8437481": [False, 1, "19/3/78"],
        "C6228537": [False, 2, "22/8/91"],
        "C1124365": [True, 3, "10/1/84"],
        "C2028517": [False, 1, "16/6/93"],
    }
    context.iter = context.client.batch_features(
        [
            context.snowflake_user.boolean_feature,
            context.snowflake_user.numerical_feature,
            context.snowflake_user.string_feature,
        ]
    )


@then("I serve batch features for spark")
def step_impl(context):
    context.expected = {
        "C1010011": [2553.0, "120180.54", 0.002962210021689036],
        "C1010012": [1499.0, "24204.49", 0.06193065832000591],
        "C1010014": [727.5, "38377.14", 0.03139890049128205],
        "C1010018": [30.0, "496.18", 0.06046192913861905],
        "C1010024": [5000.0, "87058.65", 0.05743254690946851],
        "C1010028": [557.0, "296828.37", 0.0018765052680106017],
        "C1010031": [932.0, "1754.1", 0.23031754175930677],
        "C1010035": [375.0, "378013.09", 0.001851787725128778],
        "C1010036": [208.0, "355430.17", 0.0005852063711980331],
        "C1010037": [19680.0, "95859.17", 0.20530117254301283],
    }
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


@then(
    'I can get a list containing the entity name and a tuple with all the features from "{provider}"'
)
def step_impl(context, provider):
    i = 0
    for entity, features in context.iter:
        assert entity in context.expected
        assert features == context.expected[entity]
        i += 1
    if i == 0:
        raise Exception("No entities were found")
    if i < len(context.expected):
        raise Exception("Not all entities were found")


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

    context.snowflake_user = SnowflakeUser
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
