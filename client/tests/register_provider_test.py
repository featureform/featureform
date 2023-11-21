import os
import pytest

from featureform.register import (
    OnlineProvider,
    FileStoreProvider,
    OfflineSQLProvider,
    OfflineSparkProvider,
    OfflineK8sProvider,
    Registrar,
    LocalProvider,
)

from featureform.resources import AWSCredentials, GCPCredentials, SparkCredentials

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.mark.local
def test_register_redis():
    reg = Registrar()
    result = reg.register_redis(
        name="quickstart",
        description="desc",
        team="team",
        host="host",
        port=1,
        password="pass",
        db=1,
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_pinecone():
    reg = Registrar()
    result = reg.register_pinecone(
        name="quickstart",
        project_id="id",
        environment="env",
        api_key="api_key",
        description="desc",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_weaviate():
    reg = Registrar()
    result = reg.register_weaviate(
        name="quickstart",
        url="url",
        api_key="api_key",
        description="desc",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_blob_store():
    reg = Registrar()
    result = reg.register_blob_store(
        name="quickstart",
        account_name="account",
        account_key="key",
        container_name="name",
        root_path="path",
        description="desc",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, FileStoreProvider)


@pytest.mark.local
def test_register_s3():
    fake_creds = AWSCredentials("id", "secret")
    reg = Registrar()
    result = reg.register_s3(
        name="quickstart",
        credentials=fake_creds,
        bucket_name="path",
        bucket_region="region",
        path="/path",
        description="desc",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, FileStoreProvider)


@pytest.mark.local
def test_register_gcs():
    reg = Registrar()
    result = reg.register_gcs(
        name="name",
        credentials=GCPCredentials(
            project_id="id",
            credentials_path="provider/connection/mock_credentials.json",
        ),
        bucket_name="name",
        root_path="/path",
        description="description",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, FileStoreProvider)


@pytest.mark.local
def test_register_hdfs():
    reg = Registrar()
    result = reg.register_hdfs(
        name="name",
        host="host",
        port="1",
        username="user",
        path="/path",
        description="description",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, FileStoreProvider)


@pytest.mark.local
def test_register_firestore():
    reg = Registrar()
    result = reg.register_firestore(
        name="name",
        collection="collection",
        project_id="id",
        credentials=GCPCredentials(
            project_id="id",
            credentials_path="provider/connection/mock_credentials.json",
        ),
        description="description",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_cassandra():
    reg = Registrar()
    result = reg.register_cassandra(
        name="name",
        description="description",
        team="team",
        host="host",
        port=1,
        username="user",
        password="password",
        keyspace="space",
        consistency="THREE",
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_dynamodb():
    reg = Registrar()
    result = reg.register_dynamodb(
        name="name",
        description="description",
        team="team",
        access_key="key",
        secret_key="key",
        region="region",
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_mongodb():
    reg = Registrar()
    result = reg.register_mongodb(
        name="name",
        description="description",
        team="team",
        username="user",
        password="pass",
        database="db",
        host="host",
        port="1",
        throughput=1,
        tags=[],
        properties={},
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_register_snowflake_legacy():
    reg = Registrar()
    result = reg.register_snowflake_legacy(
        name="name",
        username="user",
        password="pass",
        account_locator="loci",
        database="db",
        schema="PUBLIC",
        description="description",
        team="team",
        warehouse="wh",
        role="role",
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_register_snowflake():
    reg = Registrar()
    result = reg.register_snowflake(
        name="name",
        username="user",
        password="pass",
        account="account",
        organization="org",
        database="db",
        schema="PUBLIC",
        description="description",
        team="team",
        warehouse="wh",
        role="role",
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_register_postgres():
    reg = Registrar()
    result = reg.register_postgres(
        name="name",
        description="description",
        team="team",
        host="host",
        port="1",
        user="user",
        password="pass",
        database="db",
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_register_redshift():
    reg = Registrar()
    result = reg.register_redshift(
        name="name",
        description="description",
        team="team",
        host="host",
        port=0,
        user="user",
        password="pass",
        database="db",
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_register_bigquery():
    reg = Registrar()
    result = reg.register_bigquery(
        name="name",
        description="description",
        team="team",
        project_id="id",
        dataset_id="id",
        credentials=GCPCredentials(
            project_id="id",
            credentials_path="provider/connection/mock_credentials.json",
        ),
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_register_spark():
    reg = Registrar()
    spark_credentials = SparkCredentials(
        master="local",
        deploy_mode="client",
        python_version="3.8",
    )

    aws_creds = AWSCredentials(
        access_key="id",
        secret_key="key",
    )

    s3 = reg.register_s3(
        name="quickstart",
        credentials=aws_creds,
        bucket_name="path",
        bucket_region="/region",
        path="/path",
    )
    result = reg.register_spark(
        name="name",
        executor=spark_credentials,
        filestore=s3,
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineSparkProvider)


@pytest.mark.local
def test_register_k8s():
    reg = Registrar()
    aws_creds = AWSCredentials(
        access_key="id",
        secret_key="key",
    )
    s3 = reg.register_s3(
        name="quickstart",
        credentials=aws_creds,
        bucket_name="path",
        bucket_region="/region",
        path="/path",
    )
    result = reg.register_k8s(
        name="name",
        store=s3,
        description="description",
        docker_image="image",
        team="team",
        tags=[],
        properties={},
    )
    assert isinstance(result, OfflineK8sProvider)


@pytest.mark.local
def test_register_local():
    assert isinstance(Registrar().register_local(), LocalProvider)
