#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import sys
from argparse import Namespace

import dill
import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.fixture(scope="module")
def sql_all_arguments():
    input_args = [
        "sql",
        "--job_type",
        "Transformation",
        "--output",
        '{"outputLocation":"s3://featureform-testing/fake-path","locationType": "filestore"}',
        "--sql_query",
        "SELECT * FROM source_0",
        "--sources",
        '{"location": "s3://path", "provider": "SPARK_OFFLINE", "locationType": "filestore"}',
        '{"location": "s3://path", "provider": "SPARK_OFFLINE", "locationType": "filestore"}',
    ]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output={
            "outputLocation": "s3://featureform-testing/fake-path",
            "locationType": "filestore",
        },
        sql_query="SELECT * FROM source_0",
        sources=[
            {
                "location": "s3://path",
                "provider": "SPARK_OFFLINE",
                "locationType": "filestore",
            },
            {
                "location": "s3://path",
                "provider": "SPARK_OFFLINE",
                "locationType": "filestore",
            },
        ],
        spark_config={},
        credential={},
        store_type=None,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
        is_update=False,
        direct_copy_use_iceberg=False,
        direct_copy_target=None,
        direct_copy_table_name=None,
        direct_copy_feature_name=None,
        direct_copy_feature_variant=None,
        direct_copy_entity_column=None,
        direct_copy_value_column=None,
        direct_copy_timestamp_column=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_local_all_arguments():
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output={
            "outputLocation": f"{dir_path}/test_files/output/test_sql_transformation",
            "locationType": "filestore",
        },
        sql_query="SELECT * FROM source_0",
        sources=[
            {
                "location": f"{dir_path}/test_files/input/transaction.parquet",
                "locationType": "filestore",
            }
        ],
        store_type="local",
        spark_config={},
        credential={},
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
        is_update=False,
    )
    return expected_args


@pytest.fixture(scope="module")
def sql_partial_arguments():
    input_args = [
        "sql",
        "--job_type",
        "Transformation",
        "--output",
        '{"outputLocation":"s3://featureform-testing/fake-path", "locationType": "filestore"}',
    ]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output={
            "outputLocation": "s3://featureform-testing/fake-path",
            "locationType": "filestore",
        },
        sql_query=None,
        sources=None,
        spark_config={},
        credential={},
        store_type=None,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
        is_update=False,
        direct_copy_use_iceberg=False,
        direct_copy_target=None,
        direct_copy_table_name=None,
        direct_copy_feature_name=None,
        direct_copy_feature_variant=None,
        direct_copy_entity_column=None,
        direct_copy_value_column=None,
        direct_copy_timestamp_column=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_invalid_arguments():
    input_args = ["sql", "--job_type", "Transformation", "--hi"]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output={
            "outputLocation": "s3://featureform-testing/fake-path",
            "locationType": "filestore",
        },
        sql_query="SELECT * FROM source_0",
        sources=[
            {
                "location": "s3://path s3://path",
                "provider": "SPARK_OFFLINE",
                "locationType": "filestore",
            },
            {
                "location": "s3://path s3://path",
                "provider": "SPARK_OFFLINE",
                "locationType": "filestore",
            },
        ],
        spark_config={},
        credential={},
        store_type=None,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_invalid_local_arguments():
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transform",
        output={
            "outputLocation": "s3://featureform-testing/fake-path",
            "locationType": "filestore",
        },
        sql_query="SELECT * FROM source_0",
        sources=["NONE"],
        store_type=None,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return expected_args


@pytest.fixture(scope="module")
def df_all_arguments():
    input_args = [
        "df",
        "--output",
        '{"outputLocation":"s3://featureform-testing/fake-path","locationType": "filestore"}',
        "--code",
        "code",
        "--sources",
        '{"location": "s3://featureform/transaction", "provider": "SPARK_OFFLINE", "locationType": "filestore"}',
        '{"location": "s3://featureform/account", "provider": "SPARK_OFFLINE", "locationType": "filestore"}',
    ]
    expected_args = Namespace(
        transformation_type="df",
        output={
            "outputLocation": "s3://featureform-testing/fake-path",
            "locationType": "filestore",
        },
        code="code",
        sources=[
            {
                "location": "s3://featureform/transaction",
                "provider": "SPARK_OFFLINE",
                "locationType": "filestore",
            },
            {
                "location": "s3://featureform/account",
                "provider": "SPARK_OFFLINE",
                "locationType": "filestore",
            },
        ],
        spark_config={},
        credential={},
        store_type=None,
        headers="include",
        submit_params_uri=None,
        is_update=False,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def df_partial_arguments():
    input_args = ["df", "--job_type", "Transformation"]
    expected_args = Namespace(
        transformation_type="df",
        output=None,
        spark_config=None,
        credential=None,
        store_type=None,
        last_run_timestamp=None,
        is_update=False,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def df_invaild_arguments():
    input_args = ["df", "--job_type", "Transformation", "--hi"]
    expected_args = Namespace(
        transformation_type="df",
        output={
            "outputLocation": "s3://featureform-testing/fake-path",
            "locationType": "filestore",
        },
        spark_config={},
        credential={},
        store_type=None,
        last_run_timestamp=None,
        is_update=False,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_databricks_all_arguments():
    input_args = [
        "sql",
        "--job_type",
        "Transformation",
        "--output",
        '{"outputLocation":"abfss://<container-name>@<storage-account-name>.blob.core.windows.net/output/test_transformation.csv", "locationType": "filestore"}',
        "--sql_query",
        "SELECT * FROM source_0",
        "--store_type",
        "azure_blob_store",
        "--spark_config",
        "fs.azure.account.key.account_name.dfs.core.windows.net=adfjaidfasdklciadsj==",
        "--credential",
        "key=value",
        "--sources",
        '{"location": "abfss://<container-name>@<storage-account-name>.blob.core.windows.net/ice_cream_100rows.csv", "provider": "SPARK_OFFLINE", "locationType": "filestore"}',
    ]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output={
            "outputLocation": "abfss://<container-name>@<storage-account-name>.blob.core.windows.net/output/test_transformation.csv",
            "locationType": "filestore",
        },
        sql_query="SELECT * FROM source_0",
        sources=[
            {
                "location": "abfss://<container-name>@<storage-account-name>.blob.core.windows.net/ice_cream_100rows.csv",
                "locationType": "filestore",
                "provider": "SPARK_OFFLINE",
            }
        ],
        store_type="azure_blob_store",
        spark_config={
            "fs.azure.account.key.account_name.dfs.core.windows.net": "adfjaidfasdklciadsj=="
        },
        credential={"key": "value"},
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
        is_update=False,
        direct_copy_use_iceberg=False,
        direct_copy_target=None,
        direct_copy_table_name=None,
        direct_copy_feature_name=None,
        direct_copy_feature_variant=None,
        direct_copy_entity_column=None,
        direct_copy_value_column=None,
        direct_copy_timestamp_column=None,
    )
    return input_args, expected_args


@pytest.fixture(scope="module")
def invalid_arguments():
    input_args = ["invalid_arg"]
    expected_args = Namespace()
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def df_local_all_arguments(df_transformation):
    expected_args = Namespace(
        transformation_type="df",
        output={
            "outputLocation": f"{dir_path}/test_files/output/test_df_transformation",
            "locationType": "filestore",
        },
        code=df_transformation,
        sources=[
            {
                "location": f"{dir_path}/test_files/input/transaction.parquet",
                "locationType": "filestore",
                "provider": "SPARK_OFFLINE",
            }
        ],
        spark_config={},
        credential={},
        store_type="local",
        is_update=False,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return expected_args


@pytest.fixture(scope="module")
def df_local_pass_none_code_failure():
    expected_args = Namespace(
        transformation_type="df",
        output_uri=f"{dir_path}/test_files/output/test_transformation",
        code="s3://featureform-testing/fake-path/code",
        sources=[
            {
                "locations": f"{dir_path}/test_files/input/transaction.parquet",
                "provider": "SPARK_OFFLINE",
            }
        ],
        spark_config={},
        credential={},
        is_update=False,
    )
    return expected_args


@pytest.fixture(scope="module")
def df_transformation():
    file_path = f"{dir_path}/test_files/transformations/same_df.pkl"

    def transformation(transaction):
        return transaction

    with open(file_path, "wb") as f:
        dill.dump(transformation.__code__, f)
    return file_path


@pytest.fixture(scope="module")
def sparkbuilder():
    return SparkSession.builder.appName("Testing App")


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Testing App").getOrCreate()


@pytest.fixture(scope="module")
def container_client():
    # get the path to .env in root directory
    env_file = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(real_path))))
    )
    load_dotenv(f"{env_file}/.env")
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(
        os.getenv("AZURE_CONTAINER_NAME")
    )
    return container_client


@pytest.fixture(scope="module")
def dill_python_version_error():
    version = sys.version_info
    python_version = f"{version.major}.{version.minor}.{version.micro}"
    error_message = f"""This error is most likely caused by different Python versions between the client and Spark provider. Check to see if you are running Python version '{python_version}' on the client."""
    return Exception(error_message)


@pytest.fixture(scope="module")
def generic_error():
    return Exception("generic error")
