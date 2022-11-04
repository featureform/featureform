import pytest
import sys

sys.path.insert(0, 'client/src/')

from featureform.register import Registrar, OfflineSparkProvider
from featureform.resources import SparkConfig, Provider, DatabricksCredentials, AzureFileStoreConfig

pytest_plugins = [
    'connection_test',
]

@pytest.fixture(scope="module")
def spark_provider():
    r = Registrar()
    r.set_default_owner("tester")
    databricks = DatabricksCredentials(username="a", password="b")
    azure_blob = AzureFileStoreConfig(account_name="", account_key="", container_name="", root_path="")   
    
    config = SparkConfig(executor=databricks, filestore=azure_blob)
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
