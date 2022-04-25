import pytest

from register import Registrar
from resources import SQLTable, SQLTransformation, Source, User, PrimaryData


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


@pytest.mark.parametrize("args,expected", [(minimal_user_args, User("user"))])
def test_register_user(registrar, args, expected):
    registrar.register_user(**args)
    assert registrar.state().sorted_list() == [
        expected,
    ]


@pytest.mark.parametrize("args, expected", [
    ({
        "name": "data",
        "variant": "var",
        "location": SQLTable("name"),
        "owner": "user",
        "provider": "snowflake",
    },
     Source(
         name="data",
         variant="var",
         definition=PrimaryData(location=SQLTable("name")),
         owner="user",
         provider="snowflake",
         description="",
     )),
    ({
        "name": "data",
        "variant": "var",
        "location": SQLTable("name"),
        "owner": "user",
        "provider": "snowflake",
        "description": "desc",
    },
     Source(
         name="data",
         variant="var",
         definition=PrimaryData(location=SQLTable("name")),
         owner="user",
         provider="snowflake",
         description="desc",
     )),
])
def test_register_primary_data(registrar, args, expected):
    registrar.register_primary_data(**args)
    assert registrar.state().sorted_list() == [expected]


def test_register_primary_data_with_registrars(registrar):
    user = registrar.register_user(**minimal_user_args)
    postgres = registrar.register_postgres(**minimal_postgres_args)
    registrar.register_primary_data(
        name="data",
        variant="var",
        location=SQLTable("name"),
        owner=user,
        provider=postgres,
    )


def test_register_primary_data_with_default_owner(registrar):
    user = registrar.register_user(**minimal_user_args)
    user.make_default_owner()
    postgres = registrar.register_postgres(**minimal_postgres_args)
    registrar.register_primary_data(
        name="data",
        variant="var",
        location=SQLTable("name"),
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


def name():
    """doc string"""
    return "query"


def test_sql_transformation_decorator(registrar):
    decorator = registrar.sql_transformation(
        variant="var",
        owner="owner",
        provider="provider",
    )
    decorator(name)
    assert decorator.to_source() == Source(
        name="name",
        variant="var",
        definition=SQLTransformation(query="query"),
        owner="owner",
        provider="provider",
        description="doc string",
    )


def test_sql_transformation_decorator_with_registrar(registrar):
    user = registrar.register_user(**minimal_user_args)
    postgres = registrar.register_postgres(**minimal_postgres_args)
    decorator = registrar.sql_transformation(variant="var",
                                             owner=user,
                                             provider=postgres)
    decorator(name)
    assert decorator.to_source() == Source(
        name="name",
        variant="var",
        definition=SQLTransformation(query="query"),
        owner="user",
        provider="postgres",
        description="doc string",
    )


def test_offline_store_sql_transformation_decorator(registrar):
    user = registrar.register_user(**minimal_user_args)
    user.make_default_owner()
    postgres = registrar.register_postgres(**minimal_postgres_args)
    decorator = postgres.sql_transformation(variant="var",)
    decorator(name)
    assert decorator.to_source() == Source(
        name="name",
        variant="var",
        definition=SQLTransformation(query="query"),
        owner="user",
        provider="postgres",
        description="doc string",
    )


def test_decorator_state(registrar):
    decorator = registrar.sql_transformation(
        variant="var",
        owner="owner",
        provider="provider",
    )
    decorator(name)

    assert registrar.state().sorted_list() == [
        Source(
            name="name",
            variant="var",
            definition=SQLTransformation(query="query"),
            owner="owner",
            provider="provider",
            description="doc string",
        )
    ]
    # Do it twice to verify that decorators aren't re-used.
    assert registrar.state().sorted_list() == [
        Source(
            name="name",
            variant="var",
            definition=SQLTransformation(query="query"),
            owner="owner",
            provider="provider",
            description="doc string",
        )
    ]


def empty_string():
    return ""


def return_5():
    return 5


@pytest.mark.parametrize("fn", [empty_string, return_5])
def test_sql_transformation_decorator_invalid_fn(registrar, fn):
    decorator = registrar.sql_transformation(
        variant="var",
        owner="owner",
        provider="provider",
    )
    with pytest.raises((TypeError, ValueError)):
        decorator(fn)


def test_register_sql_transformation_with_registrar(registrar):
    user = registrar.register_user(**minimal_user_args)
    postgres = registrar.register_postgres(**minimal_postgres_args)
    registrar.register_sql_transformation(
        name="name",
        variant="var",
        owner=user,
        query="query",
        provider=postgres,
        description="doc string",
    )


def test_register_sql_transformation_default_owner(registrar):
    user = registrar.register_user(**minimal_user_args)
    user.make_default_owner()
    postgres = registrar.register_postgres(**minimal_postgres_args)
    registrar.register_sql_transformation(
        name="name",
        variant="var",
        query="query",
        provider=postgres,
        description="doc string",
    )
