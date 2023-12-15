import json
import sys

import pytest

sys.path.insert(0, "client/src/")
from featureform.resources import (
    BigQueryConfig,
    ClickHouseConfig,
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
    OnlineBlobConfig,
    CassandraConfig,
    DynamodbConfig,
    MongoDBConfig,
    SnowflakeConfig,
    PostgresConfig,
    SparkConfig,
    K8sConfig,
    RedshiftConfig,
)
import featureform.resources as resources
import inspect

connection_configs = json.load(open("provider/connection/connection_configs.json"))
mock_credentials = json.load(open("provider/connection/mock_credentials.json"))


@pytest.mark.local
def test_config_list():
    """assert that each config is present, if this test fails
    you likely added a config, but forgot to add a test and associated config schema in connection_configs.json.
    The file is used in provider_config_test.(go|py), offline_test.go, and online_test.go.
    """
    config_list = [
        x
        for x in dir(resources)
        if inspect.isclass(getattr(resources, x)) and x.endswith("Config")
    ]
    for config_class in config_list:
        assert config_class in connection_configs


@pytest.mark.local
def test_redis():
    expected_config = connection_configs["RedisConfig"]
    conf = RedisConfig(
        host="host",
        port=1,
        password="password",
        db=1,
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_pinecone():
    expected_config = connection_configs["PineconeConfig"]
    conf = PineconeConfig(project_id="1", environment="local", api_key="api_key")
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_weaviate():
    expected_config = connection_configs["WeaviateConfig"]
    conf = WeaviateConfig(url="url", api_key="api_key")
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
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
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_azurefilestore():
    expected_config = connection_configs["AzureFileStoreConfig"]
    conf = AzureFileStoreConfig(
        account_name="name", account_key="key", container_name="name", root_path="/path"
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_s3store():
    expected_config = connection_configs["S3StoreConfig"]
    conf = S3StoreConfig(
        bucket_path="bucket_path",
        bucket_region="bucket_region",
        credentials=AWSCredentials(access_key="id", secret_key="key"),
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_hdfs():
    expected_config = connection_configs["HDFSConfig"]
    conf = HDFSConfig(host="host", port="port", path="/path", username="username")
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_hdfs():
    expected_config = connection_configs["HDFSConfig"]
    conf = HDFSConfig(host="host", port="port", path="/path", username="username")
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_onlineblob():
    expected_config = connection_configs["OnlineBlobConfig"]
    config = {
        "AccountName": "name",
        "AccountKey": "key",
        "ContainerName": "name",
        "Path": "/path",
    }
    conf = OnlineBlobConfig(store_type="LOCAL_FILESYSTEM", store_config=config)
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_firestore():
    expected_config = connection_configs["FirestoreConfig"]
    conf = FirestoreConfig(
        project_id="some-project-id",
        collection="some-collection-id",
        credentials=GCPCredentials(
            project_id="id",
            credentials_path="provider/connection/gcp_test_credentials.json",
        ),
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_cassandra():
    expected_config = connection_configs["CassandraConfig"]
    conf = CassandraConfig(
        keyspace="keyspace",
        host="host",
        port=0,
        username="username",
        password="password",
        consistency="consistency",
        replication=1,
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_dynamodb():
    expected_config = connection_configs["DynamodbConfig"]
    conf = DynamodbConfig(
        region="region",
        access_key="access_key",
        secret_key="secret_key",
        should_import_from_s3=False,
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_mongodb():
    expected_config = connection_configs["MongoDBConfig"]
    conf = MongoDBConfig(
        username="username",
        password="password",
        host="host",
        port="port",
        database="database",
        throughput=1,
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_snowflake():
    expected_config = connection_configs["SnowflakeConfig"]
    conf = SnowflakeConfig(
        username="username",
        password="password",
        schema="schema",
        account="account",
        organization="organization",
        warehouse="warehouse",
        role="role",
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_postgres():
    expected_config = connection_configs["PostgresConfig"]
    conf = PostgresConfig(
        host="host",
        port="port",
        database="database",
        user="username",
        password="password",
        sslmode="sslmode",
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_clickhouse():
    expected_config = connection_configs["ClickHouseConfig"]
    conf = ClickHouseConfig(
        host="host",
        port=9000,
        database="database",
        user="username",
        password="password",
        ssl=False,
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_redshift():
    expected_config = connection_configs["RedshiftConfig"]
    conf = RedshiftConfig(
        host="host",
        port=0,
        database="database",
        user="username",
        password="password",
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_bigquery():
    expected_config = connection_configs["BigQueryConfig"]
    conf = BigQueryConfig(
        project_id=expected_config["ProjectID"],
        dataset_id=expected_config["DatasetID"],
        credentials=GCPCredentials(
            project_id="id",
            credentials_path="provider/connection/gcp_test_credentials.json",
        ),
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_spark():
    expected_config = connection_configs["SparkConfig"]
    conf = SparkConfig(
        executor_type="executor_type",
        executor_config=dict(),
        store_type="LOCAL_FILESYSTEM",
        store_config=dict(),
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_k8sconfig():
    expected_config = connection_configs["K8sConfig"]
    conf = K8sConfig(
        store_type="store_type",
        store_config=dict(),
        docker_image="docker_image",
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config
