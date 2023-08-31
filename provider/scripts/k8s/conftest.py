import os
from argparse import Namespace

import dill
import uuid
import pytest
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

from offline_store_pandas_runner import (
    POSTGRES,
    PostgresStore
)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.fixture(scope="module")
def local_variables_success():
    return {
        "MODE": "local",
        "BLOB_STORE_TYPE": "local",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def local_variables_parquet_success():
    return {
        "MODE": "local",
        "BLOB_STORE_TYPE": "local",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transaction_short",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def local_df_variables_success():
    return {
        "MODE": "local",
        "BLOB_STORE_TYPE": "local",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": f"{dir_path}/tests/test_files/transformations/same_df.pkl",
    }


@pytest.fixture(scope="module")
def local_df_parquet_variables_success():
    return {
        "MODE": "local",
        "BLOB_STORE_TYPE": "local",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transaction_short",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": f"{dir_path}/tests/test_files/transformations/same_df.pkl",
    }


@pytest.fixture(scope="module")
def local_variables_failure():
    return {}


@pytest.fixture(scope="module")
def k8s_sql_variables_success():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "local",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def k8s_df_variables_success():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "azure",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "ETCD_HOST": "127.0.0.1",
        "ETCD_PORT": "2379,2380",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }


@pytest.fixture(scope="module")
def k8s_s3_df_variables_success():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "s3",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "ETCD_HOST": "127.0.0.1",
        "ETCD_PORT": "2379,2380",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }


@pytest.fixture(scope="module")
def k8s_postgres_df_variables_success():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "postgres",
        "OUTPUT_URI": "featureform__test_name__test_variant",
        "SOURCES": "featureform__test_source_name__test_source_variant",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USERNAME": "postgres",
        "POSTGRES_PASSWORD": "password",
        "POSTGRES_DATABASE": "postgres",
        "POSTGRES_SSLMODE": "disable",
    }


@pytest.fixture(scope="module")
def k8s_s3_df_variables_failure():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "s3",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "ETCD_HOST": "127.0.0.1",
        "ETCD_PORT": "2379,2380",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }


@pytest.fixture(scope="module")
def not_supported_blob_store():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "not_supported",
    }


@pytest.fixture(scope="module")
def k8s_df_variables_single_port_success():
    return {
        "MODE": "k8s",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "ETCD_HOST": "127.0.0.1",
        "ETCD_PORT": "2379",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }


@pytest.fixture(scope="module")
def k8s_gs_df_variables_success():
    return {}


@pytest.fixture(scope="module")
def k8s_variables_failure():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "azure",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def k8s_variables_port_not_provided_failure():
    return {
        "MODE": "k8s",
        "BLOB_STORE_TYPE": "azure",
        "OUTPUT_URI": f"{dir_path}/tests/test_files/output/local_test",
        "SOURCES": f"{dir_path}/tests/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
        "ETCD_HOST": "HOST_PATH",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }


@pytest.fixture(scope="module")
def df_transformation():
    file_path = f"{dir_path}/tests/test_files/transformations/same_df.pkl"

    def transformation(transaction):
        return transaction

    with open(file_path, "wb") as f:
        dill.dump(transformation.__code__, f)
    return file_path


@pytest.fixture(scope="module")
def container_client():
    connection_string = os.getenv("AZURE_CONNECTION_STRING")
    if connection_string is None:
        # get the path to .env in root directory
        env_file = os.path.dirname(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.dirname(real_path)))
            )
        )
        load_dotenv(f"{env_file}/.env")

        connection_string = os.getenv("AZURE_CONNECTION_STRING")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(
        os.getenv("AZURE_CONTAINER_NAME")
    )
    return container_client


@pytest.fixture(scope="module")
def postgres_store():
    credentials = Namespace(
        type=POSTGRES,
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        username=os.getenv("POSTGRES_USERNAME", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        database=os.getenv("POSTGRES_DATABASE", "postgres"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "disable"),
    )
    pgs = PostgresStore(credentials)

    create_df_transformation_table(credentials)

    return pgs


@pytest.fixture(scope="module")
def sample_data():
    data = {
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Age': [25, 30, 35, 28],
        'City': ['New York', 'Los Angeles', 'Chicago', 'Houston']
    }

    return Namespace(
        name="source",
        variant=str(uuid.uuid4()).replace("-", "_"),
        df=pd.DataFrame(data),
    )


@pytest.fixture(scope="module")
def sample_transformation():
    def transformation(df):
        df["country"] = "United States"
        return df

    name = "name"
    variant = str(uuid.uuid4()).replace("-", "_")
    serialized_transformation = dill.dumps(transformation)
    data = {
        "name": [name],
        "variant": [variant],
        "transformation": [serialized_transformation],
    }

    transformationDF = pd.DataFrame(data=data)

    return Namespace(
        name=name,
        variant=variant,
        df=transformationDF,
    )


def create_df_transformation_table(creds):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        host=creds.host,
        port=creds.port,
        database=creds.database,
        user=creds.username,
        password=creds.password,
        sslmode=creds.sslmode,
    )

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Define the CREATE TABLE statement with the desired columns and data types
    create_table_query = """
    CREATE TABLE IF NOT EXISTS featureform__transformations (
        name VARCHAR,
        variant VARCHAR,
        transformation BYTEA
    )
    """

    # Execute the CREATE TABLE statement
    cursor.execute(create_table_query)

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()
