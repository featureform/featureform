#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os

import pytest
import featureform as ff


# The markers can be used to identify to run a subset of tests
# pytest -m spark
pytestmark = [pytest.mark.spark]


@pytest.fixture(scope="module")
def s3_bucket_fixture(client):
    s3_bucket = ff.register_s3(
        name="s3-bucket-fixture",
        credentials=ff.AWSStaticCredentials(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
        ),
        bucket_name=os.getenv("S3_BUCKET_NAME", "ff-spark-testing"),
        bucket_region=os.getenv("S3_BUCKET_REGION", "us-east-2"),
        path="pytest_end_to_end",
    )

    client.apply()

    return s3_bucket


@pytest.fixture(scope="module")
def databricks_s3_spark_fixture(client, s3_bucket_fixture):
    databricks = ff.DatabricksCredentials(
        host=os.getenv("DATABRICKS_HOST", None),
        token=os.getenv("DATABRICKS_TOKEN", None),
        cluster_id=os.getenv("DATABRICKS_CLUSTER", None),
    )

    spark = ff.register_spark(
        name="databricks-s3-spark-fixture",
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=databricks,
        filestore=s3_bucket_fixture,
    )

    try:
        client.apply(asynchronous=False, verbose=True)
    except Exception as e:
        print("Exception:", e)
        raise e

    return spark


@pytest.fixture(scope="module")
def databricks_s3_transactions_dataset(
    client, databricks_s3_spark_fixture, num_records_limit
):
    spark = databricks_s3_spark_fixture
    transactions = spark.register_file(
        name="transactions_input",
        file_path="s3://ff-spark-testing/data/transactions_short.csv",
    )

    client.apply(asynchronous=False, verbose=True)
    df = client.dataframe(transactions, limit=num_records_limit)
    assert len(df) == num_records_limit

    return transactions
