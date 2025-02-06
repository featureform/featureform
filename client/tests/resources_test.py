#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os.path
import sys
import unittest

sys.path.insert(0, "client/src/")
import pytest
from featureform.resources import (
    DailyPartition,
    DatabricksCredentials,
    FileStore,
    GlueCatalogTable,
    KafkaTopic,
    Location,
    ResourceRedefinedError,
    ResourceState,
    RedisConfig,
    CassandraConfig,
    FirestoreConfig,
    AzureFileStoreConfig,
    SnowflakeConfig,
    PostgresConfig,
    RedshiftConfig,
    BigQueryConfig,
    ClickHouseConfig,
    OnlineBlobConfig,
    K8sConfig,
    SparkConfig,
    SparkFlags,
    User,
    Provider,
    Entity,
    FeatureVariant,
    LabelVariant,
    TrainingSetVariant,
    PrimaryData,
    SQLTable,
    SourceVariant,
    ResourceColumnMapping,
    DynamodbConfig,
    Schedule,
    SQLTransformation,
    DFTransformation,
    K8sArgs,
    K8sResourceSpecs,
    SparkCredentials,
    GCPCredentials,
    SnowflakeCatalog,
    SnowflakeDynamicTableConfig,
)

from featureform import ScalarType, TableFormat, VectorType

from featureform.register import (
    OfflineK8sProvider,
    OfflineSparkProvider,
    Registrar,
    FileStoreProvider,
)

from featureform.proto import metadata_pb2 as pb

from featureform.enums import TableFormat, RefreshMode, Initialize


@pytest.fixture
def postgres_config():
    return PostgresConfig(
        host="localhost",
        port="123",
        database="db",
        user="user",
        password="p4ssw0rd",
        sslmode="disable",
    )


@pytest.fixture
def clickhouse_config():
    return ClickHouseConfig(
        host="localhost",
        port=9000,
        database="default",
        user="user",
        password="",
        ssl=False,
    )


@pytest.fixture
def snowflake_config():
    return SnowflakeConfig(
        account="act",
        database="db",
        organization="org",
        username="user",
        password="pwd",
        schema="schema",
    )


@pytest.fixture
def redis_config():
    return RedisConfig(
        host="localhost",
        port=123,
        password="abc",
        db=3,
    )


@pytest.fixture
def blob_store_config():
    return AzureFileStoreConfig(
        account_name="<account_name>",
        account_key="<account_key>",
        container_name="examplecontainer",
        root_path="example/path",
    )


@pytest.fixture
def online_blob_config(blob_store_config):
    return OnlineBlobConfig(
        store_type="AZURE",
        store_config=blob_store_config.serialize(),
    )


@pytest.fixture
def file_store_provider(blob_store_config):
    return FileStoreProvider(
        registrar=None, provider=None, config=blob_store_config, store_type="AZURE"
    )


@pytest.fixture
def kubernetes_config(file_store_provider):
    return K8sConfig(store_type="K8s", store_config={})


@pytest.fixture
def cassandra_config():
    return CassandraConfig(
        host="localhost",
        port="123",
        username="abc",
        password="abc",
        keyspace="",
        consistency="THREE",
        replication=3,
    )


@pytest.fixture
def firesstore_config():
    return FirestoreConfig(
        collection="abc",
        project_id="abc",
        credentials_path="abc",
    )


@pytest.fixture
def dynamodb_config():
    return DynamodbConfig(region="abc", access_key="abc", secret_key="abc")


@pytest.fixture
def redshift_config():
    return RedshiftConfig(
        host="",
        port="5432",
        database="dev",
        user="user",
        password="p4ssw0rd",
        sslmode="disable",
    )


@pytest.fixture
def bigquery_config():
    path = (
        os.path.abspath(os.getcwd())
        + "/client/tests/test_files/bigquery_dummy_credentials.json"
    )
    return BigQueryConfig(
        project_id="bigquery-project",
        dataset_id="bigquery-dataset",
        credentials=GCPCredentials(
            project_id="bigquery-project",
            credentials_path=path,
        ),
    )


def test_bigquery_config(bigquery_config):
    return bigquery_config.serialize()


@pytest.fixture
def postgres_provider(postgres_config):
    return Provider(
        name="postgres",
        description="desc2",
        function="fn2",
        team="team2",
        config=postgres_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def clickhouse_provider(clickhouse_config):
    return Provider(
        name="clickhouse",
        description="desc2",
        function="fn2",
        team="team2",
        config=clickhouse_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def snowflake_provider(snowflake_config):
    return Provider(
        name="snowflake",
        description="desc3",
        function="fn3",
        team="team3",
        config=snowflake_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def redis_provider(redis_config):
    return Provider(
        name="redis",
        description="desc3",
        function="fn3",
        team="team3",
        config=redis_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def redshift_provider(redshift_config):
    return Provider(
        name="redshift",
        description="desc2",
        function="fn2",
        team="team2",
        config=redshift_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def bigquery_provider(bigquery_config):
    return Provider(
        name="bigquery",
        description="desc2",
        function="fn2",
        team="team2",
        config=bigquery_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def spark_provider(registrar):
    databricks = DatabricksCredentials(
        username="a", password="b", cluster_id="abcd-123def-ghijklmn"
    )
    azure_blob = AzureFileStoreConfig(
        account_name="", account_key="", container_name="", root_path=""
    )

    config = SparkConfig(
        executor_type=databricks.type(),
        executor_config=databricks.config(),
        store_type=azure_blob.store_type(),
        store_config=azure_blob.config(),
    )
    provider = Provider(
        name="spark",
        function="OFFLINE",
        description="",
        team="",
        config=config,
        tags=[],
        properties={},
    )

    return OfflineSparkProvider(registrar, provider)


@pytest.fixture
def core_site_path():
    return "test_files/yarn_files/core-site.xml"


@pytest.fixture
def yarn_site_path():
    return "test_files/yarn_files/yarn-site.xml"


def test_with_paths(core_site_path, yarn_site_path):
    config = {
        "master": "yarn",
        "deploy_mode": "client",
        "python_version": "3.9",
        "core_site_path": core_site_path,
        "yarn_site_path": yarn_site_path,
    }

    assert SparkCredentials(**config)._verify_yarn_config() is None


def test_without_paths():
    config = {
        "master": "yarn",
        "python_version": "3.9",
        "deploy_mode": "client",
    }
    with pytest.raises(Exception):
        SparkCredentials(**config)._verify_yarn_config()


def test_with_missing_core_site_path(yarn_site_path):
    config = {
        "master": "yarn",
        "python_version": "3.9",
        "yarn_site_path": yarn_site_path,
        "deploy_mode": "client",
    }
    with pytest.raises(Exception):
        SparkCredentials(**config)._verify_yarn_config()


def test_with_missing_yarn_site_path(core_site_path):
    config = {
        "master": "yarn",
        "python_version": "3.9",
        "core_site_path": core_site_path,
        "deploy_mode": "client",
    }
    with pytest.raises(Exception):
        SparkCredentials(**config)._verify_yarn_config()


def test_with_non_yarn_master(core_site_path, yarn_site_path):
    config = {
        "master": "local",
        "deploy_mode": "client",
        "python_version": "3.9",
        "core_site_path": core_site_path,
        "yarn_site_path": yarn_site_path,
    }
    assert SparkCredentials(**config)._verify_yarn_config() is None


@pytest.mark.parametrize("image", ["", "my/docker_image:latest"])
def test_k8s_args_apply(image):
    transformation = pb.Transformation()
    specs = K8sResourceSpecs()
    args = K8sArgs(image, specs)
    transformation = args.apply(transformation)
    assert image == transformation.kubernetes_args.docker_image


@pytest.mark.parametrize(
    "query,image",
    [("SELECT * FROM {{ X.Y }}", ""), ("SELECT * FROM {{ X.Y }}", "my/docker:image")],
)
def test_sql_k8s_image(query, image):
    transformation = SQLTransformation(query, K8sArgs(image, K8sResourceSpecs()))
    recv_query = transformation.kwargs()["transformation"].SQLTransformation.query
    recv_image = transformation.kwargs()["transformation"].kubernetes_args.docker_image
    assert recv_query == query
    assert recv_image == image


def test_sql_k8s_image_none():
    query = "SELECT * FROM {{ X.Y }}"
    transformation = SQLTransformation(query)
    recv_query = transformation.kwargs()["transformation"].SQLTransformation.query
    recv_image = transformation.kwargs()["transformation"].kubernetes_args.docker_image
    assert recv_query == query
    assert recv_image == ""


@pytest.mark.parametrize("query,image", [(bytes(), ""), (bytes(), "my/docker:image")])
def test_df_k8s_image(query, image):
    transformation = DFTransformation(query, [], K8sArgs(image, K8sResourceSpecs()))
    recv_query = transformation.kwargs()["transformation"].DFTransformation.query
    recv_image = transformation.kwargs()["transformation"].kubernetes_args.docker_image
    assert recv_query == query
    assert recv_image == image


def test_df_k8s_image_none():
    query = bytes()
    transformation = DFTransformation(query, [])
    recv_query = transformation.kwargs()["transformation"].DFTransformation.query
    recv_image = transformation.kwargs()["transformation"].kubernetes_args.docker_image
    assert recv_query == query
    assert recv_image == ""


@pytest.fixture
def mock_provider(kubernetes_config):
    return Provider(
        name="provider-name",
        function="OFFLINE",
        description="provider-description",
        team="team",
        config=kubernetes_config,
        tags=[],
        properties={},
    )


@pytest.fixture
def registrar():
    r = Registrar()
    r.set_default_owner("tester")
    return r


def get_transformation_config(registrar):
    provider_source = registrar.get_resources()[0]
    config = provider_source.definition.kwargs()["transformation"]
    return config


@pytest.mark.parametrize("image", ["", "docker/image:latest"])
def test_k8s_sql_provider(registrar, mock_provider, image):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.sql_transformation(owner="mock-owner", docker_image=image)
    def mock_transform():
        return "SELECT * FROM {{ X.Y }}"

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert image == docker_image


def test_k8s_sql_provider_empty(registrar, mock_provider):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.sql_transformation(owner="mock-owner")
    def mock_transform():
        return "SELECT * FROM {{ X.Y }}"

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert docker_image == ""


@pytest.mark.parametrize("image", ["", "docker/image:latest"])
def test_k8s_df_provider(registrar, mock_provider, image):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.df_transformation(
        owner="mock-owner", docker_image=image, inputs=[("df", "var")]
    )
    def mock_transform(df):
        return df

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert image == docker_image


def test_k8s_df_provider_empty(registrar, mock_provider):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.df_transformation(owner="mock-owner")
    def mock_transform():
        return "SELECT * FROM {{ X.Y }}"

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert docker_image == ""


def init_feature(input):
    FeatureVariant(
        created=None,
        name="feature",
        variant="v1",
        source=("a", "b"),
        description="feature",
        value_type=input,
        entity="user",
        owner="Owner",
        location=ResourceColumnMapping(
            entity="abc",
            value="def",
            timestamp="ts",
        ),
        provider="redis-name",
        tags=[],
        properties={},
    )


@pytest.mark.parametrize(
    "input,fail",
    [
        ("int", False),
        ("int32", False),
        ("int64", False),
        ("float32", False),
        ("float64", False),
        ("string", False),
        ("bool", False),
        ("datetime", False),
        ("datetime", False),
        (ScalarType.FLOAT32, False),
        (VectorType("float32", 128, True), False),
        (VectorType("float32", 128, False), False),
        (VectorType(ScalarType.FLOAT32, 128, False), False),
        ("none", True),
        ("str", True),
    ],
)
def test_valid_feature_column_types(input, fail):
    if not fail:
        init_feature(input)
    if fail:
        with pytest.raises(ValueError):
            init_feature(input)


def init_label(input):
    LabelVariant(
        name="feature",
        variant="v1",
        source=("a", "b"),
        description="feature",
        value_type=input,
        entity="user",
        owner="Owner",
        location=ResourceColumnMapping(
            entity="abc",
            value="def",
            timestamp="ts",
        ),
        provider="redis-name",
        tags=[],
        properties={},
    )


@pytest.mark.parametrize(
    "input,fail",
    [
        ("int", False),
        ("int32", False),
        ("int64", False),
        ("float32", False),
        ("float64", False),
        ("string", False),
        ("bool", False),
        ("datetime", False),
        ("datetime", False),
        ("none", True),
        ("str", True),
    ],
)
def test_valid_label_column_types(input, fail):
    if not fail:
        init_label(input)
    if fail:
        with pytest.raises(ValueError):
            init_label(input)


@pytest.fixture
def all_resources_set(redis_provider):
    return [
        redis_provider,
        User(name="Featureform", tags=[], properties={}),
        Entity(name="user", description="A user", tags=[], properties={}),
        SourceVariant(
            created=None,
            name="primary",
            variant="abc",
            definition=PrimaryData(location=SQLTable("table")),
            owner="someone",
            description="desc",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        FeatureVariant(
            created=None,
            name="feature",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            entity="user",
            owner="Owner",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            provider="redis-name",
            tags=[],
            properties={},
        ),
        LabelVariant(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        TrainingSetVariant(
            created=None,
            name="training-set",
            variant="v1",
            description="desc",
            owner="featureform",
            label=("label", "var"),
            feature_lags=[],
            features=[("f1", "var")],
            tags=[],
            properties={},
        ),
    ]


@pytest.fixture
def all_resources_strange_order(redis_provider):
    return [
        TrainingSetVariant(
            created=None,
            name="training-set",
            variant="v1",
            description="desc",
            owner="featureform",
            feature_lags=[],
            label=("label", "var"),
            features=[("f1", "var")],
            tags=[],
            properties={},
        ),
        LabelVariant(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            value_type="float32",
            entity="user",
            owner="Owner",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        FeatureVariant(
            created=None,
            name="feature",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            entity="user",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            owner="Owner",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        Entity(name="user", description="A user", tags=[], properties={}),
        SourceVariant(
            created=None,
            name="primary",
            variant="abc",
            definition=PrimaryData(location=SQLTable("table")),
            owner="someone",
            description="desc",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        redis_provider,
        User(name="Featureform", tags=[], properties={}),
    ]


def test_create_all_provider_types(
    redis_provider,
    snowflake_provider,
    postgres_provider,
    redshift_provider,
    bigquery_provider,
    clickhouse_provider,
):
    providers = [
        redis_provider,
        snowflake_provider,
        postgres_provider,
        redshift_provider,
        bigquery_provider,
        clickhouse_provider,
    ]
    state = ResourceState()
    for provider in providers:
        state.add(provider)


def test_redefine_provider(redis_config, snowflake_config):
    providers = [
        Provider(
            name="name",
            description="desc",
            function="fn",
            team="team",
            config=redis_config,
            tags=[],
            properties={},
        ),
        Provider(
            name="name",
            description="desc2",
            function="fn2",
            team="team2",
            config=snowflake_config,
            tags=[],
            properties={},
        ),
    ]
    state = ResourceState()
    state.add(providers[0])
    with pytest.raises(ResourceRedefinedError):
        state.add(providers[1])


def test_add_all_resource_types(all_resources_strange_order, redis_config):
    state = ResourceState()
    for resource in all_resources_strange_order:
        state.add(resource)
    assert state.sorted_list() == [
        User(name="Featureform", tags=[], properties={}),
        Provider(
            name="redis",
            description="desc3",
            function="fn3",
            team="team3",
            config=redis_config,
            tags=[],
            properties={},
        ),
        SourceVariant(
            created=None,
            name="primary",
            variant="abc",
            definition=PrimaryData(location=SQLTable("table")),
            owner="someone",
            description="desc",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        Entity(name="user", description="A user", tags=[], properties={}),
        FeatureVariant(
            created=None,
            name="feature",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        LabelVariant(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        TrainingSetVariant(
            created=None,
            name="training-set",
            variant="v1",
            description="desc",
            owner="featureform",
            label=("label", "var"),
            features=[("f1", "var")],
            feature_lags=[],
            tags=[],
            properties={},
        ),
    ]


def test_resource_types_differ(all_resources_set):
    types = set()
    for resource in all_resources_set:
        t = resource.get_resource_type()
        assert t not in types
        types.add(t)


def test_invalid_providers(snowflake_config):
    provider_args = [
        {
            "description": "desc",
            "function": "fn",
            "team": "team",
            "config": snowflake_config,
        },
        {
            "name": "name",
            "description": "desc",
            "team": "team",
            "config": snowflake_config,
        },
        {"name": "name", "description": "desc", "function": "fn", "team": "team"},
    ]
    for args in provider_args:
        with pytest.raises(TypeError):
            Provider(**args)


def test_invalid_users():
    user_args = [{}]
    for args in user_args:
        with pytest.raises(TypeError):
            User(**args)


@pytest.mark.parametrize(
    "args",
    [
        {
            "name": "name",
            "variant": "var",
            "owner": "featureform",
            "description": "abc",
            "label": ("var"),
            "features": [("a", "var")],
        },
        {
            "name": "name",
            "variant": "var",
            "owner": "featureform",
            "label": ("", "var"),
            "description": "abc",
            "features": [("a", "var")],
        },
        {
            "name": "name",
            "variant": "var",
            "owner": "featureform",
            "label": ("a", "var"),
            "features": [],
            "description": "abc",
        },
        {
            "name": "name",
            "variant": "var",
            "owner": "featureform",
            "label": ("a", "var"),
            "features": [("a", "var"), ("var")],
            "description": "abc",
        },
        {
            "name": "name",
            "variant": "var",
            "owner": "featureform",
            "label": ("a", "var"),
            "features": [("a", "var"), ("", "var")],
            "description": "abc",
        },
        {
            "name": "name",
            "variant": "var",
            "owner": "featureform",
            "label": ("a", "var"),
            "features": [("a", "var"), ("b", "")],
            "description": "abc",
        },
    ],
)
def test_invalid_training_set(args):
    with pytest.raises((ValueError, TypeError)):
        TrainingSetVariant(**args)


def test_add_all_resources_with_schedule(all_resources_strange_order, redis_config):
    state = ResourceState()
    for resource in all_resources_strange_order:
        if hasattr(resource, "schedule"):
            resource.update_schedule("* * * * *")
        state.add(resource)
    assert state.sorted_list() == [
        User(name="Featureform", tags=[], properties={}),
        Provider(
            name="redis",
            description="desc3",
            function="fn3",
            team="team3",
            config=redis_config,
            tags=[],
            properties={},
        ),
        SourceVariant(
            created=None,
            name="primary",
            variant="abc",
            definition=PrimaryData(location=SQLTable("table")),
            owner="someone",
            description="desc",
            provider="redis-name",
            schedule="* * * * *",
            schedule_obj=Schedule(
                name="primary",
                variant="abc",
                resource_type=7,
                schedule_string="* * * * *",
            ),
            tags=[],
            properties={},
        ),
        Entity(name="user", description="A user", tags=[], properties={}),
        FeatureVariant(
            created=None,
            name="feature",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name",
            schedule="* * * * *",
            schedule_obj=Schedule(
                name="feature",
                variant="v1",
                resource_type=4,
                schedule_string="* * * * *",
            ),
            tags=[],
            properties={},
        ),
        LabelVariant(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        TrainingSetVariant(
            created=None,
            name="training-set",
            variant="v1",
            description="desc",
            owner="featureform",
            label=("label", "var"),
            features=[("f1", "var")],
            feature_lags=[],
            schedule="* * * * *",
            schedule_obj=Schedule(
                name="training-set",
                variant="v1",
                resource_type=6,
                schedule_string="* * * * *",
            ),
            tags=[],
            properties={},
        ),
        # Ordering of schedules does not matter
        Schedule(
            name="training-set",
            variant="v1",
            resource_type=6,
            schedule_string="* * * * *",
        ),
        Schedule(
            name="feature", variant="v1", resource_type=4, schedule_string="* * * * *"
        ),
        Schedule(
            name="primary", variant="abc", resource_type=7, schedule_string="* * * * *"
        ),
    ]


class TestPrimaryData(unittest.TestCase):
    def setUp(self):
        self.timestamp_column = "timestamp"

    def test_kwargs_sql_location(self):
        location = SQLTable(schema="public", database="test_db", name="test_table")
        primary_data = PrimaryData(
            location=location, timestamp_column=self.timestamp_column
        )

        expected = {
            "primaryData": pb.PrimaryData(
                table=pb.SQLTable(
                    schema="public",
                    database="test_db",
                    name="test_table",
                ),
                timestamp_column=self.timestamp_column,
            )
        }

        self.assertEqual(primary_data.kwargs(), expected)

    def test_kwargs_sql_table(self):
        location = SQLTable(name="test_table")
        primary_data = PrimaryData(
            location=location, timestamp_column=self.timestamp_column
        )

        expected = {
            "primaryData": pb.PrimaryData(
                table=pb.SQLTable(name="test_table"),
                timestamp_column=self.timestamp_column,
            )
        }

        self.assertEqual(primary_data.kwargs(), expected)

    def test_kwargs_file_store(self):
        location = FileStore(path_uri="/path/to/file")
        primary_data = PrimaryData(
            location=location, timestamp_column=self.timestamp_column
        )

        expected = {
            "primaryData": pb.PrimaryData(
                filestore=pb.FileStoreTable(path="/path/to/file"),
                timestamp_column=self.timestamp_column,
            )
        }

        self.assertEqual(primary_data.kwargs(), expected)

    def test_kwargs_glue_catalog_table(self):
        location = GlueCatalogTable(
            database="test_db", table="test_table", table_format=TableFormat.ICEBERG
        )
        primary_data = PrimaryData(
            location=location, timestamp_column=self.timestamp_column
        )

        expected = {
            "primaryData": pb.PrimaryData(
                catalog=pb.CatalogTable(
                    database="test_db",
                    table="test_table",
                    table_format="iceberg",
                ),
                timestamp_column=self.timestamp_column,
            )
        }

        self.assertEqual(primary_data.kwargs(), expected)

    def test_kwargs_kafka_topic(self):
        location = KafkaTopic(topic="test_topic")
        primary_data = PrimaryData(
            location=location, timestamp_column=self.timestamp_column
        )

        expected = {
            "primaryData": pb.PrimaryData(
                kafka=pb.Kafka(topic="test_topic"),
                timestamp_column=self.timestamp_column,
            )
        }

    def test_unsupported_location_type(self):
        class UnsupportedLocation(Location):
            pass

        location = UnsupportedLocation()
        primary_data = PrimaryData(
            location=location, timestamp_column=self.timestamp_column
        )

        with self.assertRaises(ValueError) as context:
            primary_data.kwargs()

        self.assertIn("Unsupported location type", str(context.exception))


@pytest.mark.parametrize(
    "catalog, config",
    [
        (
            SnowflakeCatalog(external_volume="ext_vol", base_location="base_loc"),
            {
                "ExternalVolume": "ext_vol",
                "BaseLocation": "base_loc",
                "TableFormat": TableFormat.ICEBERG,
                "TableConfig": {
                    "TargetLag": "DOWNSTREAM",
                    "RefreshMode": "AUTO",
                    "Initialize": "ON_CREATE",
                },
            },
        ),
        (
            SnowflakeCatalog(
                external_volume="ext_vol",
                base_location="base_loc",
                table_config=SnowflakeDynamicTableConfig(
                    target_lag="65 minutes",
                    refresh_mode=RefreshMode.AUTO,
                    initialize=Initialize.ON_SCHEDULE,
                ),
            ),
            {
                "ExternalVolume": "ext_vol",
                "BaseLocation": "base_loc",
                "TableFormat": TableFormat.ICEBERG,
                "TableConfig": {
                    "TargetLag": "65 minutes",
                    "RefreshMode": "AUTO",
                    "Initialize": "ON_SCHEDULE",
                },
            },
        ),
        (
            SnowflakeCatalog(
                external_volume="ext_vol",
                base_location="base_loc",
                table_config=SnowflakeDynamicTableConfig(
                    target_lag="DOWNSTREAM",
                    refresh_mode=RefreshMode.FULL,
                    initialize=Initialize.ON_SCHEDULE,
                ),
            ),
            {
                "ExternalVolume": "ext_vol",
                "BaseLocation": "base_loc",
                "TableFormat": TableFormat.ICEBERG,
                "TableConfig": {
                    "TargetLag": "DOWNSTREAM",
                    "RefreshMode": "FULL",
                    "Initialize": "ON_SCHEDULE",
                },
            },
        ),
        pytest.param(
            lambda: SnowflakeCatalog(
                external_volume="ext_vol",
                base_location="base_loc",
                table_config=SnowflakeDynamicTableConfig(
                    target_lag="20 seconds",
                ),
            ),
            {
                "ExternalVolume": "ext_vol",
                "BaseLocation": "base_loc",
                "TableFormat": TableFormat.ICEBERG,
                "TableConfig": {
                    "TargetLag": "20 seconds",
                    "RefreshMode": "AUTO",
                    "Initialize": "ON_CREATE",
                },
            },
            marks=pytest.mark.xfail(reason="minimum target lag is 1 minutes"),
        ),
        pytest.param(
            lambda: SnowflakeCatalog(
                external_volume="ext_vol",
                base_location="base_loc",
                table_config=SnowflakeDynamicTableConfig(
                    target_lag="1 day",
                ),
            ),
            {
                "ExternalVolume": "ext_vol",
                "BaseLocation": "base_loc",
                "TableFormat": TableFormat.ICEBERG,
                "TableConfig": {
                    "TargetLag": "1 day",
                    "RefreshMode": "AUTO",
                    "Initialize": "ON_CREATE",
                },
            },
            marks=pytest.mark.xfail(reason='units must be plural (e.g. "days")'),
        ),
    ],
)
def test_snowflake_catalog(catalog, config):
    if callable(catalog):
        catalog = catalog()

    assert catalog.config() == config


@pytest.mark.parametrize(
    "config, proto",
    [
        (SnowflakeDynamicTableConfig(), pb.SnowflakeDynamicTableConfig()),
        (
            SnowflakeDynamicTableConfig(
                target_lag="1 day",
                refresh_mode=RefreshMode.AUTO,
                initialize=Initialize.ON_CREATE,
            ),
            pb.SnowflakeDynamicTableConfig(
                target_lag="1 day",
                refresh_mode=pb.RefreshMode.REFRESH_MODE_AUTO,
                initialize=pb.Initialize.INITIALIZE_ON_CREATE,
            ),
        ),
    ],
)
def test_snowflake_dynamic_table_config(config, proto):
    assert config.to_proto() == proto


def test_sql_transformation_to_proto():
    query = "SELECT * FROM {{ X.Y }}"
    transformation = SQLTransformation(
        query=query,
        func_params_to_inputs={"X": ("df", "var")},
        partition_options=DailyPartition(column="date"),
        spark_flags=SparkFlags(
            spark_params={"spark.executor.memory": "1g"},
            write_options={"-something": "spark.executor.memory=1g"},
            table_properties={"something": "1g"},
        ),
    )

    expected = pb.Transformation(
        SQLTransformation=pb.SQLTransformation(
            query=query.encode(),
        ),
        DailyPartition=pb.DailyPartition(column="date"),
        spark_flags=pb.SparkFlags(
            spark_params=[pb.SparkParam(key="spark.executor.memory", value="1g")],
            write_options=[
                pb.WriteOption(key="-something", value="spark.executor.memory=1g")
            ],
            table_properties=[pb.TableProperty(key="something", value="1g")],
        ),
    )
    assert transformation.to_proto() == expected


def test_df_transformation_to_proto():
    query = "SELECT * FROM {{ X.Y }}"
    transformation = DFTransformation(
        query=query.encode(),
        inputs=[],
        args=K8sArgs("my/docker:image", None),
        source_text="source_text",
        canonical_func_text="SELECT * FROM {{ X.Y }}",
        partition_options=DailyPartition(column="date"),
        spark_flags=SparkFlags(
            spark_params={"spark.executor.memory": "1g"},
            write_options={"-something": "spark.executor.memory=1g"},
            table_properties={"something": "1g"},
        ),
    )

    expected = pb.Transformation(
        DFTransformation=pb.DFTransformation(
            query=query.encode(),
            source_text="source_text",
            canonical_func_text="SELECT * FROM {{ X.Y }}",
            inputs=[],
        ),
        kubernetes_args=pb.KubernetesArgs(docker_image="my/docker:image"),
        DailyPartition=pb.DailyPartition(column="date"),
        spark_flags=pb.SparkFlags(
            spark_params=[pb.SparkParam(key="spark.executor.memory", value="1g")],
            write_options=[
                pb.WriteOption(key="-something", value="spark.executor.memory=1g")
            ],
            table_properties=[pb.TableProperty(key="something", value="1g")],
        ),
    )
    assert transformation.to_proto() == expected


def test_df_source_equivalence(spark_provider):
    # fmt :off
    @spark_provider.df_transformation(inputs=[("df", "var")])
    def my_function(df):
        """doc string"""
        """doc string"""
        df = df.head(5)

        # another comment
        if df.empty:  # df df
            # do something complex that can be split it up into different lines
            some_var = [i**2 for i in range(10)]
            if df.empty:
                return df
            return df
        return df  # some more comments and stuff

    # fmt :on

    expected = """def my_function(df):
    df = df.head(5)
    if df.empty:
        some_var = [(i ** 2) for i in range(10)]
        if df.empty:
            return df
        return df
    return df"""

    assert my_function.transformation.canonical_func_text == expected
