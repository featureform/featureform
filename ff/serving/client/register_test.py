import pytest

from register import Registrar
from resources import Table


@pytest.fixture
def registrar():
    return Registrar()


@pytest.mark.parametrize("args", [
    {
        "name": "snowflake",
        "username": "user",
        "password": "abc",
        "database": "db",
        "account": "db",
        "organization": "org",
    },
    {
        "name": "snowflake",
        "description": "test",
        "team": "featureform",
        "username": "user",
        "password": "abc",
        "database": "db",
        "account": "db",
        "organization": "org",
        "schema": "private",
    },
])
def test_register_snowflake(registrar, args):
    registrar.register_snowflake(**args)


minimal_postgres_args = {
    "name": "postgres",
}


@pytest.mark.parametrize("args", [
    minimal_postgres_args,
    {
        "name": "postgres",
        "description": "test",
        "team": "featureform",
        "host": "localhost",
        "port": 1234,
        "user": "Abc",
        "password": "abc",
        "database": "db",
    },
])
def test_register_postgres(registrar, args):
    registrar.register_postgres(**args)


@pytest.mark.parametrize("args", [
    {
        "name": "redis"
    },
    {
        "name": "redis",
        "description": "test",
        "team": "featureform",
        "host": "localhost",
        "port": 1234,
        "password": "abc",
        "db": 4
    },
])
def test_register_redis(registrar, args):
    registrar.register_redis(**args)


minimal_user_args = {
    "name": "user",
}


@pytest.mark.parametrize("args", [minimal_user_args])
def test_register_user(registrar, args):
    registrar.register_user(**args)


@pytest.mark.parametrize("args", [
    {
        "name": "data",
        "variant": "var",
        "location": Table("name"),
        "owner": "user",
        "provider": "snowflake",
    },
    {
        "name": "data",
        "variant": "var",
        "location": Table("name"),
        "owner": "user",
        "provider": "snowflake",
        "description": "desc",
    },
])
def test_register_primary_data(registrar, args):
    registrar.register_primary_data(**args)


def test_register_primary_data_with_registrars(registrar):
    user = registrar.register_user(**minimal_user_args)
    postgres = registrar.register_postgres(**minimal_postgres_args)
    registrar.register_primary_data(
        name="data",
        variant="var",
        location=Table("name"),
        owner=user,
        provider=postgres,
    )


def test_register_table_offline_store(registrar):
    user = registrar.register_user(**minimal_user_args)
    postgres = registrar.register_postgres(**minimal_postgres_args)
    postgres.register_table(
        name="data",
        variant="var",
        table="name",
        owner=user,
    )
