#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import sys

sys.path.insert(0, "client/src/")
import featureform as ff
from featureform.resources import (
    PostgresConfig,
    RedshiftConfig,
    ClickHouseConfig,
    GCPCredentials,
    BigQueryConfig,
    CassandraConfig,
    AWSStaticCredentials,
    DynamodbConfig,
    RedisConfig,
    MongoDBConfig,
    FirestoreConfig,
    SnowflakeConfig,
    PineconeConfig,
    S3StoreConfig,
    EMRCredentials,
    SparkConfig,
    AzureFileStoreConfig,
    GCSFileStoreConfig,
    HDFSConfig,
    BasicCredentials,
    OnlineBlobConfig,
    DatabricksCredentials,
    SparkCredentials,
)
from featureform.proto import metadata_pb2 as pb

#  Offline Providers
postgres_fields = {
    "name": "my_postgres",
    "description": "some description",
    "type": "POSTGRES_OFFLINE",
    "software": "provider",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
    "host": "my_host",
    "port": "1234",
    "database": "my_database",
    "user": "me",
    "password": "mypassword",
    "sslmode": "disabled",
}

clickhouse_fields = {
    "name": "my_clickhouse",
    "description": "some description",
    "type": "CLICKHOUSE_OFFLINE",
    "software": "provider",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
    "host": "my_host",
    "port": 1234,
    "database": "my_database",
    "user": "me",
    "password": "mypassword",
    "sslmode": True,
}

redshift_fields = {
    "name": "my_redshift",
    "description": "some description",
    "type": "REDSHIFT_OFFLINE",
    "software": "provider",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
    "host": "my_host",
    "port": "1234",
    "database": "my_database",
    "user": "me",
    "password": "mypassword",
    "sslmode": "disabled",
}

bigquery_fields = {
    "name": "my_bigquery",
    "project_id": "my_project",
    "type": "BIGQUERY_OFFLINE",
    "dataset_id": "my_dataset",
    "credentials_path": "client/tests/test_files/bigquery_dummy_credentials.json",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

snowflake_legacy_fields = {
    "name": "my_snowflake_legacy",
    "type": "SNOWFLAKE_LEGACY_OFFLINE",
    "username": "me",
    "password": "mypassword",
    "account_locator": "my_account_locator",
    "database": "my_database",
    "schema": "PUBLIC",
    "description": "some description",
    "team": "some team",
    "warehouse": "my_warehouse",
    "role": "my_role",
    "tags": ["tag1", "tag2"],
    "properties": {},
    "catalog": None,
}

snowflake_fields = {
    "name": "my_snowflake",
    "type": "SNOWFLAKE_OFFLINE",
    "username": "me",
    "password": "mypassword",
    "account": "my_account",
    "organization": "my_organization",
    "database": "my_database",
    "schema": "PUBLIC",
    "description": "some description",
    "team": "some team",
    "warehouse": "my_warehouse",
    "role": "my_role",
    "tags": ["tag1", "tag2"],
    "properties": {},
    "catalog": None,
}

# Online Providers
cassandra_fields = {
    "name": "my_cassandra",
    "type": "CASSANDRA_ONLINE",
    "host": "my_host",
    "port": 1234,
    "username": "me",
    "password": "mypassword",
    "keyspace": "my_keyspace",
    "consistency": "TWO",
    "replication": 4,
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

firestore_fields = {
    "name": "my_firestore",
    "type": "FIRESTORE_ONLINE",
    "collection": "my_collection",
    "project_id": "my_project",
    "credentials_path": "client/tests/test_files/bigquery_dummy_credentials.json",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

dynamodb_fields = {
    "name": "my_dynamodb",
    "type": "DYNAMODB_ONLINE",
    "aws_access_key_id": "my_access_key",
    "aws_secret_access_key": "my_secret",
    "region": "us-west-2",
    "should_import_from_s3": False,
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

redis_fields = {
    "name": "my_redis",
    "host": "my_redis_host",
    "port": 1234,
    "password": "my_password",
    "database": 0,
    "type": "REDIS_ONLINE",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

mongodb_fields = {
    "name": "my_mongodb",
    "type": "MONGODB_ONLINE",
    "username": "me",
    "password": "mypassword",
    "database": "my_database",
    "host": "my_host",
    "port": "1234",
    "throughput": 1321,
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

pinecone_fields = {
    "name": "my_pinecone",
    "type": "PINECONE_ONLINE",
    "project_id": "my_project",
    "api_key": "my_api_key",
    "environment": "my_environment",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

#  Filestores
s3_fields = {
    "name": "my_s3",
    "access_key": "my_access_key",
    "secret_key": "my_secret",
    "bucket_region": "us-west-2",
    "bucket_name": "my_bucket",
    "path": "my_path",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

azure_fields = {
    "name": "my_azure",
    "account_name": "my_account",
    "account_key": "my_key",
    "container_name": "my_container",
    "root_path": "my_path",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

gcs_fields = {
    "name": "my_gcs",
    "bucket_name": "my_bucket",
    "bucket_path": "my_path",
    "project_id": "my_project",
    "credentials_path": "client/tests/test_files/bigquery_dummy_credentials.json",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

hdfs_fields = {
    "name": "my_hdfs",
    "host": "my_host",
    "port": "1234",
    "path": "my_path",
    "hdfs_site_file": "client/tests/test_files/yarn_files/yarn-site.xml",
    "core_site_file": "client/tests/test_files/yarn_files/core-site.xml",
    "username": "me",
    "password": "mypassword",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

# Executors
emr_fields = {
    "cluster_id": "my_cluster_id",
    "cluster_region": "us-west-2",
    "access_key": "my_access_key",
    "secret_key": "my_secret",
}

databricks_fields = {
    "username": "my_username",
    "password": "my_password",
    "cluster_id": "1003-211215-n0pjeh9x",
}

generic_fields = {
    "master": "local",
    "deploy_mode": "client",
    "python_version": "3.10",
    "core_site_path": "client/tests/test_files/yarn_files/core-site.xml",
    "yarn_site_path": "client/tests/test_files/yarn_files/yarn-site.xml",
}

# Spark
spark_fields = {
    "name_emr_s3": "my_spark_emr_s3",
    "name_emr_hdfs": "my_spark_emr_hdfs",
    "name_emr_gcs": "my_spark_emr_gcs",
    "name_emr_azure": "my_spark_emr_azure",
    "name_databricks_s3": "my_spark_databricks_s3",
    "name_databricks_hdfs": "my_spark_databricks_hdfs",
    "name_databricks_gcs": "my_spark_databricks_gcs",
    "name_databricks_azure": "my_spark_databricks_azure",
    "name_generic_s3": "my_spark_generic_s3",
    "name_generic_hdfs": "my_spark_generic_hdfs",
    "name_generic_gcs": "my_spark_generic_gcs",
    "name_generic_azure": "my_spark_generic_azure",
    "description": "some description",
    "team": "some team",
    "tags": ["tag1", "tag2"],
    "properties": {},
}

emr_executor = EMRCredentials(
    emr_cluster_id=emr_fields["cluster_id"],
    emr_cluster_region=emr_fields["cluster_region"],
    credentials=AWSStaticCredentials(
        access_key=emr_fields["access_key"], secret_key=emr_fields["secret_key"]
    ),
)

databricks_executor = DatabricksCredentials(
    username=databricks_fields["username"],
    password=databricks_fields["password"],
    # host=databricks_fields["host"],
    # token=databricks_fields["token"],
    cluster_id=databricks_fields["cluster_id"],
)

generic_executor = SparkCredentials(
    master=generic_fields["master"],
    deploy_mode=generic_fields["deploy_mode"],
    python_version=generic_fields["python_version"],
    core_site_path=generic_fields["core_site_path"],
    yarn_site_path=generic_fields["yarn_site_path"],
)

s3_store = S3StoreConfig(
    bucket_path=s3_fields["bucket_name"],
    bucket_region=s3_fields["bucket_region"],
    credentials=AWSStaticCredentials(
        access_key=s3_fields["access_key"], secret_key=s3_fields["secret_key"]
    ),
    path=s3_fields["path"],
)

hdfs_store = HDFSConfig(
    host=hdfs_fields["host"],
    port=hdfs_fields["port"],
    path=hdfs_fields["path"],
    hdfs_site_file=hdfs_fields["hdfs_site_file"],
    core_site_file=hdfs_fields["core_site_file"],
    credentials=BasicCredentials(
        username=hdfs_fields["username"], password=hdfs_fields["password"]
    ),
)

gcs_store = GCSFileStoreConfig(
    bucket_name=gcs_fields["bucket_name"],
    bucket_path=gcs_fields["bucket_path"],
    credentials=GCPCredentials(
        project_id=gcs_fields["project_id"],
        credentials_path=gcs_fields["credentials_path"],
    ),
)

azure_store = AzureFileStoreConfig(
    account_name=azure_fields["account_name"],
    account_key=azure_fields["account_key"],
    container_name=azure_fields["container_name"],
    root_path=azure_fields["root_path"],
)
blob_store = OnlineBlobConfig(store_type="AZURE", store_config=azure_store.config())


def postgres_proto():
    return pb.Provider(
        name=postgres_fields["name"],
        description=postgres_fields["description"],
        type=postgres_fields["type"],
        software=postgres_fields["software"],
        team=postgres_fields["team"],
        tags=pb.Tags(tag=postgres_fields["tags"]),
        properties=postgres_fields["properties"],
        serialized_config=PostgresConfig(
            host=postgres_fields["host"],
            port=postgres_fields["port"],
            database=postgres_fields["database"],
            user=postgres_fields["user"],
            password=postgres_fields["password"],
            sslmode=postgres_fields["sslmode"],
        ).serialize(),
    )


def clickhouse_proto():
    return pb.Provider(
        name=clickhouse_fields["name"],
        description=clickhouse_fields["description"],
        type=clickhouse_fields["type"],
        software=clickhouse_fields["software"],
        team=clickhouse_fields["team"],
        tags=pb.Tags(tag=clickhouse_fields["tags"]),
        properties=clickhouse_fields["properties"],
        serialized_config=ClickHouseConfig(
            host=clickhouse_fields["host"],
            port=clickhouse_fields["port"],
            database=clickhouse_fields["database"],
            user=clickhouse_fields["user"],
            password=clickhouse_fields["password"],
            ssl=clickhouse_fields["sslmode"],
        ).serialize(),
    )


def redshift_proto():
    return pb.Provider(
        name=redshift_fields["name"],
        description=redshift_fields["description"],
        type=redshift_fields["type"],
        software=redshift_fields["software"],
        team=redshift_fields["team"],
        tags=pb.Tags(tag=redshift_fields["tags"]),
        properties=redshift_fields["properties"],
        serialized_config=RedshiftConfig(
            host=redshift_fields["host"],
            port=redshift_fields["port"],
            database=redshift_fields["database"],
            user=redshift_fields["user"],
            password=redshift_fields["password"],
            sslmode=redshift_fields["sslmode"],
        ).serialize(),
    )


def bigquery_proto():
    return pb.Provider(
        name=bigquery_fields["name"],
        description=bigquery_fields["description"],
        type=bigquery_fields["type"],
        team=bigquery_fields["team"],
        tags=pb.Tags(tag=bigquery_fields["tags"]),
        properties=bigquery_fields["properties"],
        serialized_config=BigQueryConfig(
            project_id=bigquery_fields["project_id"],
            dataset_id=bigquery_fields["dataset_id"],
            credentials=GCPCredentials(
                project_id=bigquery_fields["project_id"],
                credentials_path=bigquery_fields["credentials_path"],
            ),
        ).serialize(),
    )


def cassandra_proto():
    return pb.Provider(
        name=cassandra_fields["name"],
        description=cassandra_fields["description"],
        type=cassandra_fields["type"],
        team=cassandra_fields["team"],
        tags=pb.Tags(tag=cassandra_fields["tags"]),
        properties=cassandra_fields["properties"],
        serialized_config=CassandraConfig(
            host=cassandra_fields["host"],
            port=cassandra_fields["port"],
            username=cassandra_fields["username"],
            password=cassandra_fields["password"],
            keyspace=cassandra_fields["keyspace"],
            consistency=cassandra_fields["consistency"],
            replication=cassandra_fields["replication"],
        ).serialize(),
    )


def firestore_proto():
    return pb.Provider(
        name=firestore_fields["name"],
        description=firestore_fields["description"],
        type=firestore_fields["type"],
        team=firestore_fields["team"],
        tags=pb.Tags(tag=firestore_fields["tags"]),
        properties=firestore_fields["properties"],
        serialized_config=FirestoreConfig(
            collection=firestore_fields["collection"],
            project_id=firestore_fields["project_id"],
            credentials=GCPCredentials(
                project_id=firestore_fields["project_id"],
                credentials_path=firestore_fields["credentials_path"],
            ),
        ).serialize(),
    )


def dynamo_proto():
    return pb.Provider(
        name=dynamodb_fields["name"],
        description=dynamodb_fields["description"],
        type=dynamodb_fields["type"],
        team=dynamodb_fields["team"],
        tags=pb.Tags(tag=dynamodb_fields["tags"]),
        properties=dynamodb_fields["properties"],
        serialized_config=DynamodbConfig(
            credentials=AWSStaticCredentials(
                access_key=dynamodb_fields["aws_access_key_id"],
                secret_key=dynamodb_fields["aws_secret_access_key"],
            ),
            region=dynamodb_fields["region"],
            should_import_from_s3=dynamodb_fields["should_import_from_s3"],
        ).serialize(),
    )


def redis_proto():
    return pb.Provider(
        name=redis_fields["name"],
        description=redis_fields["description"],
        type=redis_fields["type"],
        team=redis_fields["team"],
        tags=pb.Tags(tag=redis_fields["tags"]),
        properties=redis_fields["properties"],
        serialized_config=RedisConfig(
            host=redis_fields["host"],
            port=redis_fields["port"],
            password=redis_fields["password"],
            db=redis_fields["database"],
        ).serialize(),
    )


def mongo_proto():
    return pb.Provider(
        name=mongodb_fields["name"],
        description=mongodb_fields["description"],
        type=mongodb_fields["type"],
        team=mongodb_fields["team"],
        tags=pb.Tags(tag=mongodb_fields["tags"]),
        properties=mongodb_fields["properties"],
        serialized_config=MongoDBConfig(
            username=mongodb_fields["username"],
            password=mongodb_fields["password"],
            database=mongodb_fields["database"],
            host=mongodb_fields["host"],
            port=mongodb_fields["port"],
            throughput=mongodb_fields["throughput"],
        ).serialize(),
    )


def snowflake_legacy_proto():
    return pb.Provider(
        name=snowflake_legacy_fields["name"],
        description=snowflake_legacy_fields["description"],
        type=snowflake_legacy_fields["type"],
        team=snowflake_legacy_fields["team"],
        tags=pb.Tags(tag=snowflake_legacy_fields["tags"]),
        properties=snowflake_legacy_fields["properties"],
        serialized_config=SnowflakeConfig(
            username=snowflake_legacy_fields["username"],
            password=snowflake_legacy_fields["password"],
            schema=snowflake_legacy_fields["schema"],
            database=snowflake_legacy_fields["database"],
            warehouse=snowflake_legacy_fields["warehouse"],
            role=snowflake_legacy_fields["role"],
            account_locator=snowflake_legacy_fields["account_locator"],
        ).serialize(),
    )


def snowflake_proto():
    return pb.Provider(
        name=snowflake_fields["name"],
        description=snowflake_fields["description"],
        type=snowflake_fields["type"],
        team=snowflake_fields["team"],
        tags=pb.Tags(tag=snowflake_fields["tags"]),
        properties=snowflake_fields["properties"],
        serialized_config=SnowflakeConfig(
            username=snowflake_fields["username"],
            password=snowflake_fields["password"],
            account=snowflake_fields["account"],
            schema=snowflake_fields["schema"],
            organization=snowflake_fields["organization"],
            database=snowflake_fields["database"],
            warehouse=snowflake_fields["warehouse"],
            role=snowflake_fields["role"],
        ).serialize(),
    )


def pinecone_proto():
    return pb.Provider(
        name=pinecone_fields["name"],
        description=pinecone_fields["description"],
        type=pinecone_fields["type"],
        team=pinecone_fields["team"],
        tags=pb.Tags(tag=pinecone_fields["tags"]),
        properties=pinecone_fields["properties"],
        serialized_config=PineconeConfig(
            project_id=pinecone_fields["project_id"],
            environment=pinecone_fields["environment"],
            api_key=pinecone_fields["api_key"],
        ).serialize(),
    )


def s3_proto():
    return pb.Provider(
        name=s3_fields["name"],
        description=s3_fields["description"],
        type="S3_ONLINE",
        team=s3_fields["team"],
        tags=pb.Tags(tag=s3_fields["tags"]),
        properties=s3_fields["properties"],
        serialized_config=S3StoreConfig(
            credentials=AWSStaticCredentials(
                access_key=s3_fields["access_key"],
                secret_key=s3_fields["secret_key"],
            ),
            bucket_region=s3_fields["bucket_region"],
            bucket_path=s3_fields["bucket_name"],
            path=s3_fields["path"],
        ).serialize(),
    )


def gcs_proto():
    return pb.Provider(
        name=gcs_fields["name"],
        description=gcs_fields["description"],
        type="GCS_ONLINE",
        team=gcs_fields["team"],
        tags=pb.Tags(tag=gcs_fields["tags"]),
        properties=gcs_fields["properties"],
        serialized_config=GCSFileStoreConfig(
            bucket_name=gcs_fields["bucket_name"],
            bucket_path=gcs_fields["bucket_path"],
            credentials=GCPCredentials(
                project_id=gcs_fields["project_id"],
                credentials_path=gcs_fields["credentials_path"],
            ),
        ).serialize(),
    )


def azure_proto():
    return pb.Provider(
        name=azure_fields["name"],
        description=azure_fields["description"],
        type="AZURE_ONLINE",
        team=azure_fields["team"],
        tags=pb.Tags(tag=azure_fields["tags"]),
        properties=azure_fields["properties"],
        serialized_config=AzureFileStoreConfig(
            account_name=azure_fields["account_name"],
            account_key=azure_fields["account_key"],
            container_name=azure_fields["container_name"],
            root_path=azure_fields["root_path"],
        ).serialize(),
    )


def hdfs_proto():
    return pb.Provider(
        name=hdfs_fields["name"],
        description=hdfs_fields["description"],
        type="HDFS_ONLINE",
        team=hdfs_fields["team"],
        tags=pb.Tags(tag=hdfs_fields["tags"]),
        properties=hdfs_fields["properties"],
        serialized_config=HDFSConfig(
            host=hdfs_fields["host"],
            port=hdfs_fields["port"],
            path=hdfs_fields["path"],
            hdfs_site_file=hdfs_fields["hdfs_site_file"],
            core_site_file=hdfs_fields["core_site_file"],
            credentials=BasicCredentials(
                username=hdfs_fields["username"], password=hdfs_fields["password"]
            ),
        ).serialize(),
    )


def spark_proto(name, executor, storage):
    return pb.Provider(
        name=name,
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=pb.Tags(tag=spark_fields["tags"]),
        properties=spark_fields["properties"],
        serialized_config=SparkConfig(
            executor_config=executor.config(),
            executor_type=executor.type(),
            store_config=storage.config(),
            store_type=storage.type(),
        ).serialize(),
    )


class MockStub:
    # Get Provider returns a proto
    def GetProviders(self, req):
        name = next(req).name.name
        if name == postgres_fields["name"]:
            yield postgres_proto()
        elif name == clickhouse_fields["name"]:
            yield clickhouse_proto()
        elif name == redshift_fields["name"]:
            yield redshift_proto()
        elif name == bigquery_fields["name"]:
            yield bigquery_proto()
        elif name == cassandra_fields["name"]:
            yield cassandra_proto()
        elif name == firestore_fields["name"]:
            yield firestore_proto()
        elif name == dynamodb_fields["name"]:
            yield dynamo_proto()
        elif name == redis_fields["name"]:
            yield redis_proto()
        elif name == mongodb_fields["name"]:
            yield mongo_proto()
        elif name == snowflake_fields["name"]:
            yield snowflake_proto()
        elif name == snowflake_legacy_fields["name"]:
            yield snowflake_legacy_proto()
        elif name == pinecone_fields["name"]:
            yield pinecone_proto()
        elif name == s3_fields["name"]:
            yield s3_proto()
        elif name == azure_fields["name"]:
            yield azure_proto()
        elif name == gcs_fields["name"]:
            yield gcs_proto()
        elif name == hdfs_fields["name"]:
            yield hdfs_proto()
        elif name == spark_fields["name_emr_s3"]:
            yield spark_proto(name, emr_executor, s3_store)
        elif name == spark_fields["name_emr_hdfs"]:
            yield spark_proto(name, emr_executor, hdfs_store)
        elif name == spark_fields["name_emr_gcs"]:
            yield spark_proto(name, emr_executor, gcs_store)
        elif name == spark_fields["name_emr_azure"]:
            yield spark_proto(name, emr_executor, azure_store)
        elif name == spark_fields["name_databricks_s3"]:
            yield spark_proto(name, databricks_executor, s3_store)
        elif name == spark_fields["name_databricks_hdfs"]:
            yield spark_proto(name, databricks_executor, hdfs_store)
        elif name == spark_fields["name_databricks_gcs"]:
            yield spark_proto(name, databricks_executor, gcs_store)
        elif name == spark_fields["name_databricks_azure"]:
            yield spark_proto(name, databricks_executor, azure_store)
        elif name == spark_fields["name_generic_s3"]:
            yield spark_proto(name, generic_executor, s3_store)
        elif name == spark_fields["name_generic_hdfs"]:
            yield spark_proto(name, generic_executor, hdfs_store)
        elif name == spark_fields["name_generic_gcs"]:
            yield spark_proto(name, generic_executor, gcs_store)
        elif name == spark_fields["name_generic_azure"]:
            yield spark_proto(name, generic_executor, azure_store)
        else:
            raise ValueError(f"Untested Provider {name}")


def test_get_postgres():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_postgres(
        name=postgres_fields["name"],
        host=postgres_fields["host"],
        port=postgres_fields["port"],
        user=postgres_fields["user"],
        password=postgres_fields["password"],
        database=postgres_fields["database"],
        description=postgres_fields["description"],
        team=postgres_fields["team"],
        sslmode=postgres_fields["sslmode"],
        tags=postgres_fields["tags"],
        properties=postgres_fields["properties"],
    )
    got = client.get_postgres(postgres_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_redshift():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_redshift(
        name=redshift_fields["name"],
        host=redshift_fields["host"],
        port=redshift_fields["port"],
        user=redshift_fields["user"],
        password=redshift_fields["password"],
        database=redshift_fields["database"],
        description=redshift_fields["description"],
        team=redshift_fields["team"],
        sslmode=redshift_fields["sslmode"],
        tags=redshift_fields["tags"],
        properties=redshift_fields["properties"],
    )
    got = client.get_redshift(redshift_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_bigquery():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_bigquery(
        name=bigquery_fields["name"],
        project_id=bigquery_fields["project_id"],
        dataset_id=bigquery_fields["dataset_id"],
        credentials=GCPCredentials(
            project_id=bigquery_fields["project_id"],
            credentials_path=bigquery_fields["credentials_path"],
        ),
        description=bigquery_fields["description"],
        team=bigquery_fields["team"],
        tags=bigquery_fields["tags"],
        properties=bigquery_fields["properties"],
    )
    got = client.get_bigquery(bigquery_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_cassandra():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_cassandra(
        name=cassandra_fields["name"],
        host=cassandra_fields["host"],
        port=cassandra_fields["port"],
        username=cassandra_fields["username"],
        password=cassandra_fields["password"],
        keyspace=cassandra_fields["keyspace"],
        consistency=cassandra_fields["consistency"],
        replication=cassandra_fields["replication"],
        description=cassandra_fields["description"],
        team=cassandra_fields["team"],
        tags=cassandra_fields["tags"],
        properties=cassandra_fields["properties"],
    )
    got = client.get_cassandra(cassandra_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_firestore():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_firestore(
        name=firestore_fields["name"],
        collection=firestore_fields["collection"],
        project_id=firestore_fields["project_id"],
        credentials=GCPCredentials(
            project_id=firestore_fields["project_id"],
            credentials_path=firestore_fields["credentials_path"],
        ),
        description=firestore_fields["description"],
        team=firestore_fields["team"],
        tags=firestore_fields["tags"],
        properties=firestore_fields["properties"],
    )
    got = client.get_firestore(firestore_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_dynamodb():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_dynamodb(
        name=dynamodb_fields["name"],
        credentials=AWSStaticCredentials(
            access_key=dynamodb_fields["aws_access_key_id"],
            secret_key=dynamodb_fields["aws_secret_access_key"],
        ),
        region=dynamodb_fields["region"],
        should_import_from_s3=dynamodb_fields["should_import_from_s3"],
        description=dynamodb_fields["description"],
        team=dynamodb_fields["team"],
        tags=dynamodb_fields["tags"],
        properties=dynamodb_fields["properties"],
    )
    got = client.get_dynamodb(dynamodb_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_redis():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_redis(
        name=redis_fields["name"],
        host=redis_fields["host"],
        port=redis_fields["port"],
        password=redis_fields["password"],
        db=redis_fields["database"],
        description=redis_fields["description"],
        team=redis_fields["team"],
        tags=redis_fields["tags"],
        properties=redis_fields["properties"],
    )
    got = client.get_redis(redis_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_mongodb():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_mongodb(
        name=mongodb_fields["name"],
        username=mongodb_fields["username"],
        password=mongodb_fields["password"],
        database=mongodb_fields["database"],
        host=mongodb_fields["host"],
        port=mongodb_fields["port"],
        throughput=mongodb_fields["throughput"],
        description=mongodb_fields["description"],
        team=mongodb_fields["team"],
        tags=mongodb_fields["tags"],
        properties=mongodb_fields["properties"],
    )
    got = client.get_mongodb(mongodb_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_pinecone():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_pinecone(
        name=pinecone_fields["name"],
        project_id=pinecone_fields["project_id"],
        environment=pinecone_fields["environment"],
        api_key=pinecone_fields["api_key"],
        description=pinecone_fields["description"],
        team=pinecone_fields["team"],
        tags=pinecone_fields["tags"],
        properties=pinecone_fields["properties"],
    )
    got = client.get_pinecone(pinecone_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_snowflake():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_snowflake(
        name=snowflake_fields["name"],
        username=snowflake_fields["username"],
        password=snowflake_fields["password"],
        account=snowflake_fields["account"],
        organization=snowflake_fields["organization"],
        database=snowflake_fields["database"],
        warehouse=snowflake_fields["warehouse"],
        description=snowflake_fields["description"],
        role=snowflake_fields["role"],
        team=snowflake_fields["team"],
        tags=snowflake_fields["tags"],
        properties=snowflake_fields["properties"],
    )
    got = client.get_snowflake(snowflake_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_snowflake_legacy():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_snowflake_legacy(
        name=snowflake_legacy_fields["name"],
        username=snowflake_legacy_fields["username"],
        password=snowflake_legacy_fields["password"],
        account_locator=snowflake_legacy_fields["account_locator"],
        database=snowflake_legacy_fields["database"],
        warehouse=snowflake_legacy_fields["warehouse"],
        description=snowflake_legacy_fields["description"],
        role=snowflake_legacy_fields["role"],
        team=snowflake_legacy_fields["team"],
        tags=snowflake_legacy_fields["tags"],
        properties=snowflake_legacy_fields["properties"],
    )
    got = client.get_snowflake_legacy(snowflake_legacy_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_s3():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    credentials = AWSStaticCredentials(
        access_key=s3_fields["access_key"],
        secret_key=s3_fields["secret_key"],
    )

    # Returns a FileStoreProvider
    expected = ff.register_s3(
        name=s3_fields["name"],
        credentials=credentials,
        bucket_region=s3_fields["bucket_region"],
        bucket_name=s3_fields["bucket_name"],
        path=s3_fields["path"],
        description=s3_fields["description"],
        team=s3_fields["team"],
        tags=s3_fields["tags"],
        properties=s3_fields["properties"],
    )
    got = client.get_s3(s3_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_azure():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    # Returns a FileStoreProvider
    expected = ff.register_blob_store(
        name=azure_fields["name"],
        account_name=azure_fields["account_name"],
        account_key=azure_fields["account_key"],
        container_name=azure_fields["container_name"],
        root_path=azure_fields["root_path"],
        description=azure_fields["description"],
        team=azure_fields["team"],
        tags=azure_fields["tags"],
        properties=azure_fields["properties"],
    )
    got = client.get_blob_store(azure_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_gcs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    # Returns a FileStoreProvider
    expected = ff.register_gcs(
        name=gcs_fields["name"],
        bucket_name=gcs_fields["bucket_name"],
        root_path=gcs_fields["bucket_path"],
        credentials=GCPCredentials(
            project_id=gcs_fields["project_id"],
            credentials_path=gcs_fields["credentials_path"],
        ),
        description=gcs_fields["description"],
        team=gcs_fields["team"],
        tags=gcs_fields["tags"],
        properties=gcs_fields["properties"],
    )
    got = client.get_gcs(gcs_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_hdfs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    # Returns a FileStoreProvider
    expected = ff.register_hdfs(
        name=hdfs_fields["name"],
        host=hdfs_fields["host"],
        port=hdfs_fields["port"],
        path=hdfs_fields["path"],
        hdfs_site_file=hdfs_fields["hdfs_site_file"],
        core_site_file=hdfs_fields["core_site_file"],
        credentials=BasicCredentials(
            username=hdfs_fields["username"],
            password=hdfs_fields["password"],
        ),
        description=hdfs_fields["description"],
        team=hdfs_fields["team"],
        tags=hdfs_fields["tags"],
        properties=hdfs_fields["properties"],
    )
    got = client.get_hdfs(hdfs_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_emr_s3():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_emr_s3"],
        executor=emr_executor,
        filestore=client.get_s3(s3_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_emr_s3"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_emr_hdfs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_emr_hdfs"],
        executor=emr_executor,
        filestore=client.get_hdfs(hdfs_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_emr_hdfs"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_emr_gcs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_emr_gcs"],
        executor=emr_executor,
        filestore=client.get_gcs(gcs_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_emr_gcs"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_emr_azure():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_emr_azure"],
        executor=emr_executor,
        filestore=client.get_blob_store(azure_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_emr_azure"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_databricks_s3():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_databricks_s3"],
        executor=databricks_executor,
        filestore=client.get_s3(s3_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_databricks_s3"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_databricks_hdfs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_databricks_hdfs"],
        executor=databricks_executor,
        filestore=client.get_hdfs(hdfs_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_databricks_hdfs"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_databricks_gcs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_databricks_gcs"],
        executor=databricks_executor,
        filestore=client.get_gcs(gcs_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_databricks_gcs"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_databricks_azure():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_databricks_azure"],
        executor=databricks_executor,
        filestore=client.get_blob_store(azure_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_databricks_azure"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_generic_s3():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_generic_s3"],
        executor=generic_executor,
        filestore=client.get_s3(s3_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_generic_s3"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_generic_hdfs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_generic_hdfs"],
        executor=generic_executor,
        filestore=client.get_hdfs(hdfs_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_generic_hdfs"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_generic_gcs():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_generic_gcs"],
        executor=generic_executor,
        filestore=client.get_gcs(gcs_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_generic_gcs"])
    assert type(expected) == type(got)
    assert expected == got


def test_get_spark_generic_azure():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_spark(
        name=spark_fields["name_generic_azure"],
        executor=generic_executor,
        filestore=client.get_blob_store(azure_fields["name"]),
        description=spark_fields["description"],
        team=spark_fields["team"],
        tags=spark_fields["tags"],
        properties=spark_fields["properties"],
    )
    got = client.get_spark(spark_fields["name_generic_azure"])
    assert type(expected) == type(got)
    assert expected == got


def test_clickhouse():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    expected = ff.register_clickhouse(
        name=clickhouse_fields["name"],
        host=clickhouse_fields["host"],
        port=clickhouse_fields["port"],
        user=clickhouse_fields["user"],
        password=clickhouse_fields["password"],
        database=clickhouse_fields["database"],
        description=clickhouse_fields["description"],
        team=clickhouse_fields["team"],
        ssl=clickhouse_fields["sslmode"],
        tags=clickhouse_fields["tags"],
        properties=clickhouse_fields["properties"],
    )
    got = client.get_clickhouse(clickhouse_fields["name"])
    assert type(expected) == type(got)
    assert expected == got


def test_spark_config_serde():
    # serde means serialize deserialize
    spark_config = SparkConfig(
        executor_type="databricks",
        executor_config={"cluster_id": "1234"},
        store_type="s3",
        store_config={"bucket": "test_bucket"},
    )

    spark_config_json = spark_config.serialize()
    spark_config_reconstructed = SparkConfig.deserialize(spark_config_json)

    assert spark_config == spark_config_reconstructed


def test_backwards_compatability_serde():
    spark_config = SparkConfig(
        executor_type="databricks",
        executor_config={"cluster_id": "1234"},
        store_type="s3",
        store_config={"bucket": "test_bucket"},
    )

    # previous json did not include the spark config
    spark_config_json = """
        {
            "ExecutorType": "databricks",
            "StoreType": "s3",
            "ExecutorConfig": {
                "cluster_id": "1234"
            },
            "StoreConfig": {
                "bucket": "test_bucket"
            }
        }
        """

    json_bytes = spark_config_json.encode("utf-8")
    spark_config_reconstructed = SparkConfig.deserialize(json_bytes)
    assert spark_config == spark_config_reconstructed
