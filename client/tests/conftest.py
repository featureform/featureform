import pytest
import sys
sys.path.insert(0, 'client/src/')
from featureform.register import Registrar, OfflineSparkProvider
from featureform.resources import SparkConfig, Provider, DatabricksCredentials, AzureFileStoreConfig, AWSCredentials, S3StoreConfig, EMRCredentials

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
