#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
import featureform as ff


# The markers can be used to identify to run a subset of tests
# pytest -m spark
pytestmark = [pytest.mark.spark, pytest.mark.databricks]


@pytest.mark.parametrize(
    "spark_fixture, file_name, file_path",
    [
        pytest.param(
            "databricks_s3_spark_fixture",
            "transactions_s3",
            "s3://ff-spark-testing/data/transactions_short.csv",
            marks=[pytest.mark.s3],
        ),
    ],
)
def test_file_registration(
    ff_client, num_records_limit, spark_fixture, file_name, file_path, request
):
    spark = request.getfixturevalue(spark_fixture)
    file = spark.register_file(
        name=file_name,
        file_path=file_path,
    )
    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(file, limit=num_records_limit)
    assert len(df) == num_records_limit


# TODO: Parametrize the test to run for different filestores
def test_databricks_sql_transformation(
    ff_client,
    databricks_s3_spark_fixture,
    databricks_s3_transactions_dataset,
    num_records_limit,
):
    spark = databricks_s3_spark_fixture

    @spark.sql_transformation(inputs=[databricks_s3_transactions_dataset])
    def databricks_sql_transformation(transactions):
        return "SELECT * FROM {{ transactions }} LIMIT 1000"

    ff_client.apply(asynchronous=False, verbose=True)

    transformed_df = ff_client.dataframe(
        databricks_sql_transformation, limit=num_records_limit
    )
    assert len(transformed_df) == num_records_limit


# TODO: Parametrize the test to run for different filestores
def test_databricks_df_transformation(
    ff_client,
    databricks_s3_spark_fixture,
    databricks_s3_transactions_dataset,
):
    spark = databricks_s3_spark_fixture

    @spark.df_transformation(inputs=[databricks_s3_transactions_dataset])
    def databricks_df_transformation(df):
        return df.limit(900)

    ff_client.apply(asynchronous=False, verbose=True)

    transformed_df = ff_client.dataframe(databricks_df_transformation)
    assert len(transformed_df) == 900


# TODO: Parametrize the test to run for different filestores
def test_databricks_feature_label_registration(
    ff_client, redis_fixture, databricks_s3_transactions_dataset
):
    transactions = databricks_s3_transactions_dataset

    @ff.entity
    class User:
        avg_transactions = ff.Feature(
            transactions[["CustomerID", "TransactionAmount"]],
            type=ff.Float32,
            inference_store=redis_fixture,
        )
        fraudulent = ff.Label(
            transactions[["CustomerID", "IsFraud"]],
            type=ff.Bool,
        )

    ff_client.apply(asynchronous=False, verbose=True)

    feature = ff_client.features([User.avg_transactions], {"user": "C4819567"})
    assert feature[0] == 143.6199951171875


# TODO: Parametrize the test to run for different filestores
def test_databricks_training_set_registration(
    ff_client, redis_fixture, databricks_s3_transactions_dataset
):
    transactions = databricks_s3_transactions_dataset

    @ff.entity
    class User:
        avg_transactions = ff.Feature(
            transactions[["CustomerID", "TransactionAmount"]],
            type=ff.Float32,
            inference_store=redis_fixture,
        )
        fraudulent = ff.Label(
            transactions[["CustomerID", "IsFraud"]],
            type=ff.Bool,
        )

    training_set = ff.register_training_set(
        "databricks_s3_fraud_training",
        features=[User.avg_transactions],
        label=User.fraudulent,
    )

    ff_client.apply(asynchronous=False, verbose=True)

    ts = ff_client.training_set(training_set)
    df = ts.dataframe()
    assert len(df) == 9980
