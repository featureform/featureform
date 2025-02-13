#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

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
    AWSStaticCredentials,
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
    SnowflakeCatalog,
    Config,
)
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
        for x in dir(Config)
        if inspect.isclass(getattr(Config, x)) and x.endswith("Config")
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
        credentials=AWSStaticCredentials(access_key="id", secret_key="key"),
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
def test_hdfs():
    expected_config = connection_configs["HDFSConfig"]
    conf = HDFSConfig(
        host="host",
        port="port",
        path="/path",
        hdfs_site_file="provider/connection/site_file.xml",
        core_site_file="provider/connection/site_file.xml",
    )
    serialized_config = conf.serialize()
    # assert json.loads(serialized_config) == expected_config


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
        credentials=AWSStaticCredentials(access_key="id", secret_key="key"),
        table_tags={"owner": "featureform"},
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == expected_config


@pytest.mark.local
@pytest.mark.parametrize(
    "table_tags, expected_errors",
    [
        (  # Valid case
            {"ValidKey": "ValidValue"},
            [],
        ),
        (  # Invalid: Key exceeds 128 characters
            {"K" * 129: "ValidValue"},
            [
                "Key '{}' exceeds the maximum length of 128 characters.".format(
                    "K" * 129
                )
            ],
        ),
        (  # Invalid: Value exceeds 256 characters
            {"ValidKey": "V" * 257},
            ["Value for key 'ValidKey' exceeds the maximum length of 256 characters."],
        ),
        (  # Invalid: Key contains disallowed characters
            {"Invalid!Key": "ValidValue"},
            ["Key 'Invalid!Key' contains invalid characters."],
        ),
        (  # Invalid: Value contains disallowed characters
            {"ValidKey": "Invalid!Value"},
            ["Value 'Invalid!Value' contains invalid characters."],
        ),
        (  # Invalid: Key starts with 'aws'
            {"awsRestrictedKey": "ValidValue"},
            ["Key 'awsRestrictedKey' cannot start with 'aws'."],
        ),
        (  # Multiple errors in one case
            {"awsKey!": "V" * 300},
            [
                "Value for key 'awsKey!' exceeds the maximum length of 256 characters.",
                "Key 'awsKey!' contains invalid characters.",
                "Key 'awsKey!' cannot start with 'aws'.",
            ],
        ),
    ],
)
def test_dynamodb_table_tags(table_tags, expected_errors):
    dummy_credentials = AWSStaticCredentials(access_key="test", secret_key="test")
    config = DynamodbConfig(region="us-east-1", credentials=dummy_credentials)
    errors = config.validate_table_tags(table_tags)

    assert errors == expected_errors, f"Expected {expected_errors}, but got {errors}"


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
def test_snowflake_dynamic_tables():
    expected_config = connection_configs["SnowflakeConfigDynamicTables"]
    conf = SnowflakeConfig(
        username="username",
        password="password",
        schema="schema",
        account="account",
        organization="organization",
        warehouse="warehouse",
        role="role",
        catalog=SnowflakeCatalog(
            external_volume="sf_ext_vol",
            base_location="dynamic_tables",
        ),
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
        port="0",
        database="database",
        user="username",
        password="password",
        sslmode="disable",
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
