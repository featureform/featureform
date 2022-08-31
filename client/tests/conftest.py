import pytest

from featureform.register import Registrar, OfflineSparkProvider
from featureform.resources import SparkAWSConfig, Provider

pytest_plugins = [
    'connection_test',
]

@pytest.fixture(scope="module")
def spark_provider():
    r = Registrar()
    r.set_default_owner("tester")
    config = SparkAWSConfig(emr_cluster_id="",bucket_path="",emr_cluster_region="",bucket_region="",aws_access_key_id="",aws_secret_access_key="")
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
