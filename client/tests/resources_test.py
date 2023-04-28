# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import os.path
import sys

sys.path.insert(0, "client/src/")
import pytest
from featureform.resources import (
    ResourceRedefinedError,
    ResourceState,
    Provider,
    RedisConfig,
    CassandraConfig,
    FirestoreConfig,
    AzureFileStoreConfig,
    SnowflakeConfig,
    PostgresConfig,
    RedshiftConfig,
    BigQueryConfig,
    OnlineBlobConfig,
    K8sConfig,
    User,
    Provider,
    Entity,
    Feature,
    Label,
    TrainingSet,
    PrimaryData,
    SQLTable,
    Source,
    ResourceColumnMapping,
    DynamodbConfig,
    Schedule,
    SQLTransformation,
    DFTransformation,
    K8sArgs,
    K8sResourceSpecs,
    SparkCredentials,
)

from featureform.register import OfflineK8sProvider, Registrar, FileStoreProvider

from featureform.proto import metadata_pb2 as pb


@pytest.fixture
def postgres_config():
    return PostgresConfig(
        host="localhost",
        port="123",
        database="db",
        user="user",
        password="p4ssw0rd",
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
        port="5439",
        database="dev",
        user="user",
        password="p4ssw0rd",
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
        credentials_path=path,
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
def core_site_path():
    return "test_files/yarn_files/core-site.xml"


@pytest.fixture
def yarn_site_path():
    return "test_files/yarn_files/yarn-site.xml"


def test_with_paths(core_site_path, yarn_site_path):
    config = {
        "master": "yarn",
        "deploy_mode": "client",
        "python_version": "3.7.16",
        "core_site_path": core_site_path,
        "yarn_site_path": yarn_site_path,
    }

    assert SparkCredentials(**config)._verify_yarn_config() is None


def test_without_paths():
    config = {
        "master": "yarn",
        "python_version": "3.7.16",
        "deploy_mode": "client",
    }
    with pytest.raises(Exception):
        SparkCredentials(**config)._verify_yarn_config()


def test_with_missing_core_site_path(yarn_site_path):
    config = {
        "master": "yarn",
        "python_version": "3.7.16",
        "yarn_site_path": yarn_site_path,
        "deploy_mode": "client",
    }
    with pytest.raises(Exception):
        SparkCredentials(**config)._verify_yarn_config()


def test_with_missing_yarn_site_path(core_site_path):
    config = {
        "master": "yarn",
        "python_version": "3.7.16",
        "core_site_path": core_site_path,
        "deploy_mode": "client",
    }
    with pytest.raises(Exception):
        SparkCredentials(**config)._verify_yarn_config()


def test_with_non_yarn_master(core_site_path, yarn_site_path):
    config = {
        "master": "local",
        "deploy_mode": "client",
        "python_version": "3.7.16",
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
    "query,image", [("SELECT * FROM X", ""), ("SELECT * FROM X", "my/docker:image")]
)
def test_sql_k8s_image(query, image):
    transformation = SQLTransformation(query, K8sArgs(image, K8sResourceSpecs()))
    recv_query = transformation.kwargs()["transformation"].SQLTransformation.query
    recv_image = transformation.kwargs()["transformation"].kubernetes_args.docker_image
    assert recv_query == query
    assert recv_image == image


def test_sql_k8s_image_none():
    query = "SELECT * FROM X"
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
    return Registrar()


def get_transformation_config(registrar):
    provider_source = registrar.get_resources()[0].to_source()
    config = provider_source.definition.kwargs()["transformation"]
    return config


@pytest.mark.parametrize("image", ["", "docker/image:latest"])
def test_k8s_sql_provider(registrar, mock_provider, image):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.sql_transformation(owner="mock-owner", docker_image=image)
    def mock_transform():
        return "SELECT * FROM X"

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert image == docker_image


def test_k8s_sql_provider_empty(registrar, mock_provider):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.sql_transformation(owner="mock-owner")
    def mock_transform():
        return "SELECT * FROM X"

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert docker_image == ""


@pytest.mark.parametrize("image", ["", "docker/image:latest"])
def test_k8s_df_provider(registrar, mock_provider, image):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.df_transformation(owner="mock-owner", docker_image=image)
    def mock_transform(df):
        return df

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert image == docker_image


def test_k8s_df_provider_empty(registrar, mock_provider):
    k8s = OfflineK8sProvider(registrar, mock_provider)

    @k8s.df_transformation(owner="mock-owner")
    def mock_transform():
        return "SELECT * FROM X"

    config = get_transformation_config(registrar)
    docker_image = config.kubernetes_args.docker_image
    assert docker_image == ""


def init_feature(input):
    Feature(
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
def test_valid_feature_column_types(input, fail):
    if not fail:
        init_feature(input)
    if fail:
        with pytest.raises(ValueError):
            init_feature(input)


def init_label(input):
    Label(
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
        Source(
            name="primary",
            variant="abc",
            definition=PrimaryData(location=SQLTable("table")),
            owner="someone",
            description="desc",
            provider="redis-name",
            tags=[],
            properties={},
        ),
        Feature(
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
        Label(
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
        TrainingSet(
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
        TrainingSet(
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
        Label(
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
        Feature(
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
        Source(
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
):
    providers = [
        redis_provider,
        snowflake_provider,
        postgres_provider,
        redshift_provider,
        bigquery_provider,
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
        Source(
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
        Feature(
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
        Label(
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
        TrainingSet(
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
        t = resource.type()
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
        TrainingSet(**args)


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
        Source(
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
        Feature(
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
        Label(
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
        TrainingSet(
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
        Schedule(
            name="feature", variant="v1", resource_type=4, schedule_string="* * * * *"
        ),
        Schedule(
            name="primary", variant="abc", resource_type=7, schedule_string="* * * * *"
        ),
        Schedule(
            name="training-set",
            variant="v1",
            resource_type=6,
            schedule_string="* * * * *",
        ),
    ]
