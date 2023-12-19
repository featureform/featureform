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
        "--output_uri",
        "s3://featureform-testing/fake-path",
        "--sql_query",
        "SELECT * FROM source_0",
        "--source_list",
        "s3://path",
        "s3://path",
    ]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output_uri="s3://featureform-testing/fake-path",
        sql_query="SELECT * FROM source_0",
        source_list=["s3://path", "s3://path"],
        spark_config={},
        credential={},
        store_type=None,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_local_all_arguments():
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output_uri=f"{dir_path}/test_files/output/test_sql_transformation",
        sql_query="SELECT * FROM source_0",
        source_list=[f"{dir_path}/test_files/input/transaction.parquet"],
        store_type="local",
        spark_config={},
        credential={},
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return expected_args


@pytest.fixture(scope="module")
def sql_partial_arguments():
    input_args = [
        "sql",
        "--job_type",
        "Transformation",
        "--output_uri",
        "s3://featureform-testing/fake-path",
    ]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output_uri="s3://featureform-testing/fake-path",
        sql_query=None,
        source_list=None,
        spark_config={},
        credential={},
        store_type=None,
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_invaild_arguments():
    input_args = ["sql", "--job_type", "Transformation", "--hi"]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output_uri="s3://featureform-testing/fake-path",
        sql_query="SELECT * FROM source_0",
        source_list=["s3://path s3://path"],
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
        output_uri="s3://featureform-testing/fake-path",
        sql_query="SELECT * FROM source_0",
        source_list=["NONE"],
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
        "--output_uri",
        "s3://featureform-testing/fake-path",
        "--code",
        "code",
        "--source",
        "s3://featureform/transaction",
        "s3://featureform/account",
    ]
    expected_args = Namespace(
        transformation_type="df",
        output_uri="s3://featureform-testing/fake-path",
        code="code",
        source=["s3://featureform/transaction", "s3://featureform/account"],
        spark_config={},
        credential={},
        store_type=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def df_partial_arguments():
    input_args = ["df", "--job_type", "Transformation"]
    expected_args = Namespace(
        transformation_type="df",
        output_uri=None,
        spark_config=None,
        credential=None,
        store_type=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def df_invaild_arguments():
    input_args = ["df", "--job_type", "Transformation", "--hi"]
    expected_args = Namespace(
        transformation_type="df",
        output_uri="s3://featureform-testing/fake-path",
        spark_config={},
        credential={},
        store_type=None,
    )
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def sql_databricks_all_arguments():
    input_args = [
        "sql",
        "--job_type",
        "Transformation",
        "--output_uri",
        "abfss://<container-name>@<storage-account-name>.blob.core.windows.net/output/test_transformation.csv",
        "--sql_query",
        "SELECT * FROM source_0",
        "--store_type",
        "azure_blob_store",
        "--spark_config",
        "fs.azure.account.key.account_name.dfs.core.windows.net=adfjaidfasdklciadsj==",
        "--credential",
        "key=value",
        "--source_list",
        "abfss://<container-name>@<storage-account-name>.blob.core.windows.net/ice_cream_100rows.csv",
    ]
    expected_args = Namespace(
        transformation_type="sql",
        job_type="Transformation",
        output_uri="abfss://<container-name>@<storage-account-name>.blob.core.windows.net/output/test_transformation.csv",
        sql_query="SELECT * FROM source_0",
        source_list=[
            "abfss://<container-name>@<storage-account-name>.blob.core.windows.net/ice_cream_100rows.csv"
        ],
        store_type="azure_blob_store",
        spark_config={
            "fs.azure.account.key.account_name.dfs.core.windows.net": "adfjaidfasdklciadsj=="
        },
        credential={"key": "value"},
        output_format="parquet",
        headers="include",
        submit_params_uri=None,
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
        output_uri=f"{dir_path}/test_files/output/test_df_transformation",
        code=df_transformation,
        source=[f"{dir_path}/test_files/input/transaction.parquet"],
        spark_config={},
        credential={},
        store_type="local",
    )
    return expected_args


@pytest.fixture(scope="module")
def df_local_pass_none_code_failure():
    expected_args = Namespace(
        transformation_type="df",
        output_uri=f"{dir_path}/test_files/output/test_transformation",
        code="s3://featureform-testing/fake-path/code",
        source=[f"{dir_path}/test_files/input/transaction.parquet"],
        spark_config={},
        credential={},
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
