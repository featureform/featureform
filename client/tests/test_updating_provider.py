import featureform as ff
from featureform.resources import ResourceRedefinedError
import json
import pytest
from types import SimpleNamespace


# TODO: Make this test pass
@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param(
            "hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted
        ),
        pytest.param(
            "hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker
        ),
    ],
)
def xtest_valid_provider_update(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    resource_client, postgres_name, redis_name = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    # Must clear global state to simulate two separate calls to apply;
    # otherwise, a ResourceRedefinedError is thrown
    ff.clear_state()

    # DESCRIPTION
    postgres_description = "This is an updated description for Provider A"
    redis_description = "This is an updated description for Provider B"
    # USERNAME
    postgres_username = "username_a_updated"
    # PASSWORD
    postgres_password = "password_a_updated"
    redis_password = "password_b_updated"

    if is_local:
        postgres_host = "0.0.0.0"
        redis_host = "0.0.0.0"
    else:
        # The host name for postgres is different between Docker and Minikube
        postgres_host = (
            "host.docker.internal"
            if "docker" in custom_marks
            else "quickstart-postgres"
        )
        redis_host = (
            "host.docker.internal" if "docker" in custom_marks else "quickstart-redis"
        )

    postgres = ff.register_postgres(
        name=postgres_name,
        host=postgres_host,
        port="5432",
        database="postgres",
        user=postgres_username,
        password=postgres_password,
        description=postgres_description,
    )

    redis = ff.register_redis(
        name=redis_name,
        host=redis_host,
        port=6379,
        password=redis_password,
        description=redis_description,
    )

    resource_client.apply()

    updated_postgres = resource_client.get_provider(postgres_name, is_local)
    updated_redis = resource_client.get_provider(redis_name, is_local)
    print(updated_postgres, updated_redis)
    postgres_config, redis_config = get_postgres_redis_configs(
        updated_postgres, updated_redis, is_local
    )

    postgres_updates = [
        # updated_postgres["description"] == postgres_description if is_local else updated_postgres.description == postgres_description,
        postgres_config.Username == postgres_username,
        postgres_config.Password == postgres_password,
    ]

    redis_updates = [
        # updated_redis["description"] == redis_description if is_local else updated_redis.description == redis_description,
        redis_config.Password
        == redis_password,
    ]

    print(postgres_updates, redis_updates)

    assert all(postgres_updates) and all(redis_updates)


# TODO: Make this test pass
@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param(
            "hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted
        ),
        pytest.param(
            "hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker
        ),
    ],
)
def xtest_invalid_provider_update(
    provider_source_fxt, is_local, is_insecure, request, capsys
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    resource_client, postgres_name, redis_name = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    # Must clear global state to simulate two separate calls to apply;
    # otherwise, a ResourceRedefinedError is thrown
    ff.clear_state()

    updated_postgres_host = "updated-quickstart-postgres"
    updated_postgres_database = "updated-postgres"

    postgres = ff.register_postgres(
        name=postgres_name,
        host=updated_postgres_host,
        database=updated_postgres_database,
        port="5432",
        user="postgres",
        password="password",
        description="A Postgres deployment we created for the Featureform quickstart",
    )

    resource_client.apply()
    out, err = capsys.readouterr()
    postgres_logs = [ln for ln in out.split("\n")]

    ff.clear_state()

    updated_redis_host = "updated-quickstart-redis"
    updated_redis_port = 6380

    redis = ff.register_redis(
        name=redis_name,
        host=updated_redis_host,
        port=updated_redis_port,
    )

    resource_client.apply()
    out, err = capsys.readouterr()
    redis_logs = [ln for ln in out.split("\n")]

    updated_postgres = resource_client.get_provider(postgres_name, is_local)
    updated_redis = resource_client.get_provider(redis_name, is_local)

    postgres_config, redis_config = get_postgres_redis_configs(
        updated_postgres, updated_redis, is_local
    )

    no_updates = [
        postgres_config.Host != updated_postgres_host,
        postgres_config.Database != updated_postgres_database,
        redis_config.Addr != f"{updated_redis_host}:{updated_redis_port}",
    ]

    assert (
        "postgres-quickstart already exists." in postgres_logs
        and "redis-quickstart already exists." in redis_logs
        and all(no_updates)
    )


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources(provider, source, online_store, is_local, is_insecure):
    if is_local:
        postgres_name = "postgres-quickstart"
        redis_name = "redis-quickstart"

        postgres = ff.register_postgres(
            name="postgres-quickstart",
            host="0.0.0.0",
            port="5432",
            user="postgres",
            password="password",
            database="postgres",
            description="A Postgres deployment we created for the Featureform quickstart",
        )

        redis = ff.register_redis(
            name="redis-quickstart",
            host="0.0.0.0",
            port=6379,
        )
    else:
        postgres_name = provider._OfflineProvider__provider.name
        redis_name = online_store._OnlineProvider__provider.name

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    return (resource_client, postgres_name, redis_name)


def get_postgres_redis_configs(postgres_resource, redis_resource, is_local):
    if is_local:
        postgres_config_json = postgres_resource["serializedConfig"]
        redis_config_json = redis_resource["serializedConfig"]
    else:
        postgres_config_json = postgres_resource.serialized_config.decode("UTF-8")
        redis_config_json = redis_resource.serialized_config.decode("UTF-8")

    postgres_config = json.loads(
        postgres_config_json, object_hook=lambda d: SimpleNamespace(**d)
    )
    redis_config = json.loads(
        redis_config_json, object_hook=lambda d: SimpleNamespace(**d)
    )

    return (postgres_config, redis_config)
