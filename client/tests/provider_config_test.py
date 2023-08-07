import json
import sys
import pytest
import os

sys.path.insert(0, "client/src/")
from featureform.resources import (
    BigQueryConfig,
    FirestoreConfig,
    RedisConfig,
    PineconeConfig,
    WeaviateConfig,
    GCSFileStoreConfig,
    GCPCredentials,
    AzureFileStoreConfig,
    S3StoreConfig,
    AWSCredentials,
    HDFSConfig,
)

connection_configs = json.load(open("provider/connection/connection_configs.json"))
mock_credentials = json.load(open("provider/connection/mock_credentials.json"))

# get rid of this line:
# expected_config = connection_configs["WeaviateConfig"]
# test each of the connections is there, etc. scrape/store the config classes somehow?


def test_bigquery():
    expected_config = connection_configs["BigQuery"]
    conf = BigQueryConfig(
        project_id=expected_config["ProjectID"],
        dataset_id=expected_config["DatasetID"],
        credentials_path="provider/connection/gcp_test_credentials.json",
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


def test_redis():
    expected_config = connection_configs["RedisConfig"]
    conf = RedisConfig(
        host="host",
        port="port",
        password="password",
        db=1,
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


def test_pinecone():
    expected_config = connection_configs["PineconeConfig"]
    conf = PineconeConfig(project_id=1, environment="local", api_key="api_key")
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config


def test_weaviate():
    expected_config = connection_configs["WeaviateConfig"]
    conf = WeaviateConfig(url="url", api_key="api_key")
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


def test_gcsfilestore():
    expected_config = connection_configs["GCSFileStoreConfig"]
    conf = GCSFileStoreConfig(
        credentials=GCPCredentials(
            project_id="id",
            credentials_path="provider/connection/mock_credentials.json",
        ),
        bucket_name="bucket_name",
        bucket_path="bucket_path",
    )
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config


def test_azurefilestore():
    expected_config = connection_configs["AzureFileStoreConfig"]
    conf = AzureFileStoreConfig(
        account_name="name", account_key="key", container_name="name", root_path="/path"
    )
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config


def test_s3store():
    expected_config = connection_configs["S3StoreConfig"]
    conf = S3StoreConfig(
        bucket_path="bucket_path",
        bucket_region="bucket_region",
        credentials=AWSCredentials(aws_access_key_id="id", aws_secret_access_key="key"),
    )
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config


def test_hdfs():
    expected_config = connection_configs["HDFSConfig"]
    conf = HDFSConfig(host="host", port="port", path="/path", username="username")
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config


def test_hdfs():
    expected_config = connection_configs["HDFSConfig"]
    conf = HDFSConfig(host="host", port="port", path="/path", username="username")
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config


def test_firestore():
    expected_config = connection_configs["Firestore"]
    conf = FirestoreConfig(
        project_id="some-project-id",
        collection="some-collection-id",
        credentials_path="provider/connection/gcp_test_credentials.json",
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


"""
    def test_():
    expected_config = connection_configs["_Config"]
    conf = _Config(project_id=1, environment="local", api_key="api_key")
    serialized_config = conf.serialize()
    print(serialized_config)
    assert json.loads(serialized_config) == expected_config
"""
