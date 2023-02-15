import csv
import os
import pytest
import sys
import shutil
import stat
from tempfile import NamedTemporaryFile
sys.path.insert(0, 'client/src/')
from featureform.register import (
    Registrar,
    OfflineSparkProvider,
    LocalProvider,
    OfflineSQLProvider,
    ResourceClient,
)
from featureform.serving import ServingClient
from featureform.resources import (
    AWSCredentials,
    AzureFileStoreConfig,
    DatabricksCredentials,
    EMRCredentials,
    LocalConfig,
    S3StoreConfig,
    SparkConfig,
    PostgresConfig,
    Provider,
)
import featureform as ff

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

pytest_plugins = [
    'connection_test',
]

@pytest.fixture(scope="module")
def spark_provider():
    r = Registrar()
    r.set_default_owner("tester")

    databricks = DatabricksCredentials(username="a", password="b", cluster_id="c_id")
    azure_blob = AzureFileStoreConfig(account_name="", account_key="", container_name="", root_path="")   
    
    config = SparkConfig(executor_type=databricks.type(), executor_config=databricks.config(), store_type=azure_blob.store_type(), store_config=azure_blob.config())
    provider = Provider(name="spark", function="OFFLINE", description="", team="", config=config)
    
    return OfflineSparkProvider(r, provider)

@pytest.fixture(scope="module")
def avg_user_transaction():
    def average_user_transaction(df):
        """doc string"""
        from pyspark.sql.functions import avg
        df.groupBy("CustomerID").agg(avg("TransactionAmount").alias("average_user_transaction"))
        return df
    
    return average_user_transaction


@pytest.fixture(scope="module")
def aws_credentials():
    return AWSCredentials("id", "secret")


@pytest.fixture(scope="module")
def s3_config(aws_credentials):
    config = S3StoreConfig("bucket_path", "bucket_region", aws_credentials)

    expected_config = {
        "Credentials": aws_credentials.config(),
        "BucketRegion": "bucket_region",
        "BucketPath": "bucket_path",
    }

    return config, expected_config


@pytest.fixture()
def s3_config_slash_ending(aws_credentials):
    config = S3StoreConfig("bucket_path/", "bucket_region", aws_credentials)

    return config, {}

@pytest.fixture()
def s3_config_slash(aws_credentials):
    config = S3StoreConfig("/", "bucket_region", aws_credentials)

    return config, {}


@pytest.fixture(scope="module")
def azure_file_config():
    config = AzureFileStoreConfig("account_name", "account_key", "container_name", "root_path")

    expected_config = {
        "AccountName": "account_name",
        "AccountKey": "account_key",
        "ContainerName": "container_name",
        "Path": "root_path",
    }

    return config, expected_config


@pytest.fixture(scope="module")
def databricks_config():
    config = DatabricksCredentials(username="username", password="password", cluster_id="cluster_id")

    expected_config = {
        "Username": "username",
        "Password": "password",
        "Host": "",
        "Token": "",
        "Cluster": "cluster_id"
    }
    return config, expected_config


@pytest.fixture(scope="module")
def emr_config(aws_credentials):
    config = EMRCredentials("emr_cluster_id", "emr_cluster_region", aws_credentials)

    expected_config = {
        "Credentials": aws_credentials.config(),
        "ClusterName": "emr_cluster_id",
        "ClusterRegion": "emr_cluster_region"
    }

    return config, expected_config

@pytest.fixture(scope="module")
def databricks():
    return DatabricksCredentials(username="a", password="b", cluster_id="c_id")


@pytest.fixture(scope="module")
def emr(aws_credentials):
    return EMRCredentials("emr_cluster_id", "emr_cluster_region", aws_credentials)


@pytest.fixture(scope="module")
def azure_blob():
    return AzureFileStoreConfig(account_name="", account_key="", container_name="", root_path="")

@pytest.fixture(scope="module")
def s3(aws_credentials):
    return S3StoreConfig("bucket_path", "bucket_region", aws_credentials)


@pytest.fixture(scope="module")
def local_registrar_provider_source():
        ff.register_user("test_user").make_default_owner()
        provider = ff.register_local()
        source = provider.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions.",
            path=f"{dir_path}/test_files/input_files/transactions.csv"
        )
        return (ff, provider, source)


@pytest.fixture(scope="module")
def serving_client():
    def get_clients_for_context(is_local):
            return ServingClient(local=is_local)
    
    return get_clients_for_context


@pytest.fixture(scope="module")
def setup_teardown():
    def clear_state_and_reset():
        ff.clear_state()
        shutil.rmtree('.featureform', onerror=del_rw)

    return clear_state_and_reset

def del_rw(action, name, exc):
    if os.path.exists(name):
        os.chmod(name, stat.S_IWRITE)
        os.remove(name)

@pytest.fixture(scope="module")
def hosted_sql_provider_and_source():
    ff.register_user("test_user").make_default_owner()

    provider = ff.register_postgres(
        name = "postgres-quickstart",
        host="0.0.0.0",
        port="5432",
        user="postgres",
        password="password",
        database="postgres",
        description = "A Postgres deployment we created for the Featureform quickstart"
    )

    source = provider.register_table(
        name = "transactions",
        variant = "kaggle",
        description = "Fraud Dataset From Kaggle",
        table = "Transactions", # This is the table's name in Postgres
    )

    return (ff, provider, source)
