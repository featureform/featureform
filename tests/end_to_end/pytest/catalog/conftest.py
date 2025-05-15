#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os

import pytest
import featureform as ff


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
def glue_fixture():
    glue = ff.GlueCatalog(
        database="featureform_db",
        warehouse="s3://ali-aws-lake-house-iceberg-blog-demo/demo2/",
        region="us-east-1",
    )

    return glue


@pytest.fixture(scope="module")
def emr_fixture(glue_fixture, s3_bucket_fixture):
    emr = ff.EMRCredentials(
        emr_cluster_id=os.getenv("AWS_EMR_CLUSTER_ID", None),
        emr_cluster_region=os.getenv("AWS_EMR_CLUSTER_REGION", "us-east-1"),
        credentials=ff.AWSStaticCredentials(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
        ),
    )

    spark = ff.register_spark(
        name="emr-s3-spark-fixture2",
        description="A Spark deployment we created for the Featureform quickstart",
        team="featureform-team",
        executor=emr,
        filestore=s3_bucket_fixture,
        catalog=glue_fixture,
    )

    return spark


@pytest.fixture(scope="module")
def iceberg_transactions_dataset(emr_fixture):
    transactions = emr_fixture.register_iceberg_table(
        name="transactions",
        table="transactions2",
        database="ff",
    )

    return transactions
