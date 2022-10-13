import pytest
from featureform.register import Registrar
from featureform.resources import SQLTable, SQLTransformation, Source, User, PrimaryData, Entity, Feature, Label, ResourceColumnMapping



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
        "port": "1234",
        "user": "Abc",
        "password": "abc",
        "database": "db",
    },
])
def test_register_postgres(registrar, args):
    registrar.register_postgres(**args)

@pytest.mark.parametrize("args", [
    {
        "name": "azure_blob"
    },
    {
        "name": "azure_blob",
        "description": "test",
        "team": "featureform",
        "account_name": "<account_name>",
        "account_key": "<account_key>",
        "container_name": "container",
        "root_path": "example/path",
    }
])
def test_register_blob_store(registrar, args):
    registrar.register_blob_store(**args)

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

@pytest.mark.parametrize("args", [
    {
        "name": "firestore"
    },
    {
        "name": "firestore",
        "description": "test",
        "team": "featureform",
        "collection": "abc",
        "project_id": "abc",
        "credentials_path": "abc"
    },
])
def test_register_firestore(registrar, args):
    registrar.register_firestore(**args)
   
minimal_user_args = {
    "name": "user",
    "collection": "abc",
    "project_id": "abc",
    "credentials_path": "abc"
}
    
@pytest.mark.parametrize("args,expected", [(minimal_user_args, User("user"))])
def test_register_user(registrar, args, expected):
    registrar.register_user(**args)
    assert registrar.state().sorted_list() == [
        expected,
    ]
    
@pytest.mark.parametrize("args", [
    {
        "name": "dynamodb"
    },
    {
        "name": "dynamodb",
        "description": "test",
        "team": "featureform",
        "region": "abc",
        "access_key": "abc",
        "secret_key": "abc"
    },
])
def test_register_dynamodb(registrar, args):
    registrar.register_dynamodb(**args)


@pytest.mark.parametrize("args", [
    {
        "name": "redshift"
    },
    {
        "name": "redshift",
        "description": "test",
        "team": "featureform",
        "project_id": "abc",
        "dataset_id": "abc",
    },
])
def test_register_redshift(registrar, args):
    registrar.register_redshift(**args)


@pytest.mark.parametrize("args", [
    {
        "name": "bigquery"
    },
    {
        "name": "bigquery",
        "description": "test",
        "team": "featureform",
        "project_id": "abc",
        "dataset_id": "abc",
        "credentials_path": "abc",
    },
])
def test_register_bigquery(registrar, args):
    registrar.register_bigquery(**args)

    
minimal_user_args = {
    "name": "user",
    "region": "abc",
    "access_key": "abc",
    "secret_key": "abc"
}


@pytest.mark.parametrize("args,expected", [(minimal_user_args, User("user"))])
def test_register_user(registrar, args, expected):
    registrar.register_user(**args)
    assert registrar.state().sorted_list() == [
        expected,
    ]

@pytest.mark.parametrize("args", [
    {
        "name": "cassandra"
    },
    {
        "name": "cassandra",
        "description": "test",
        "team": "featureform",
        "host": "localhost",
        "port": 123,
        "username" :"abc",
        "password": "abc",
        "keyspace": "",
        "consistency": "THREE",
        "replication": 3
    },
])
def test_register_cassandra(registrar, args):
    registrar.register_cassandra(**args)


minimal_user_args = {
    "name": "user",
    "username": "abc",
    "password": "abc",
    "keyspace": "",
    "consistency": "THREE",
    "replication": 3
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


@pytest.mark.parametrize("args, expected", [
    ({
        "name": "user",
    }, Entity(
        name="user",
        description="",
    )),
    ({
        "name": "user",
        "description": "desc",
    }, Entity(
        name="user",
        description="desc",
    )),
])
def test_register_entity(registrar, args, expected):
    registrar.register_entity(**args)
    assert registrar.state().sorted_list() == [expected]


def test_register_column_resources(registrar):
    registrar.register_column_resources(
        source=("name", "variant"),
        entity="user",
        entity_column="user_clm",
        inference_store="redis",
        features=[{
            "name": "f1",
            "variant": "v1",
            "column": "value",
            "type": "float32"
        }],
        labels=[{
            "name": "l1",
            "variant": "lv1",
            "column": "label",
            "type": "string"
        }],
        owner="user",
        timestamp_column="date",
    )
    expected = [
        Feature(name="f1",
                variant="v1",
                source=("name", "variant"),
                value_type="float32",
                entity="user",
                owner="user",
                provider="redis",
                description="",
                location=ResourceColumnMapping(entity="user_clm",
                                               value="value",
                                               timestamp="date")),
        Label(name="l1",
              variant="lv1",
              source=("name", "variant"),
              value_type="string",
              entity="user",
              owner="user",
              description="",
              location=ResourceColumnMapping(entity="user_clm",
                                             value="label",
                                             timestamp="date")),
    ]
    assert registrar.state().sorted_list() == expected


def test_register_column_features(registrar):
    registrar.register_column_resources(
        source=("name", "variant"),
        entity="user",
        entity_column="user_clm",
        inference_store="redis",
        features=[{
            "name": "f1",
            "variant": "v1",
            "column": "value",
            "type": "float32"
        }],
        owner="user",
        timestamp_column="date",
    )
    expected = [
        Feature(name="f1",
                variant="v1",
                source=("name", "variant"),
                value_type="float32",
                entity="user",
                owner="user",
                provider="redis",
                description="",
                location=ResourceColumnMapping(entity="user_clm",
                                               value="value",
                                               timestamp="date")),
    ]
    assert registrar.state().sorted_list() == expected


def test_register_column_labels(registrar):
    registrar.register_column_resources(
        source=("name", "variant"),
        entity="user",
        entity_column="user_clm",
        labels=[{
            "name": "l1",
            "variant": "lv1",
            "column": "label",
            "type": "string"
        }],
        owner="user",
        timestamp_column="date",
    )
    expected = [
        Label(name="l1",
              variant="lv1",
              source=("name", "variant"),
              value_type="string",
              entity="user",
              owner="user",
              description="",
              location=ResourceColumnMapping(entity="user_clm",
                                             value="label",
                                             timestamp="date")),
    ]
    assert registrar.state().sorted_list() == expected


def test_register_column_no_resources_error(registrar):
    with pytest.raises(ValueError):
        registrar.register_column_resources(
            source=("name", "variant"),
            entity="user",
            entity_column="user_clm",
            owner="user",
            timestamp_column="date",
        )


def test_register_column_no_inference_store_error(registrar):
    with pytest.raises(ValueError):
        registrar.register_column_resources(
            source=("name", "variant"),
            entity="user",
            entity_column="user_clm",
            owner="user",
            features=[{
                "name": "f1",
                "variant": "v1",
                "column": "value",
                "type": "float32"
            }],
            timestamp_column="date",
        )


def test_register_column_no_owner_error(registrar):
    with pytest.raises(ValueError):
        registrar.register_column_resources(
            source=("name", "variant"),
            entity="user",
            entity_column="user_clm",
            inference_store="redis",
            features=[{
                "name": "f1",
                "variant": "v1",
                "column": "value",
                "type": "float32"
            }],
            timestamp_column="date",
        )


def test_register_column_resources_with_registrar(registrar):
    entity = registrar.register_entity("user")
    redis = registrar.register_redis("redis")
    user = registrar.register_user("person")
    source = registrar.register_primary_data(
        name="name",
        variant="variant",
        location=SQLTable("name"),
        owner="user",
        provider="snowflake",
    )
    source.register_resources(
        entity=entity,
        entity_column="user_clm",
        inference_store=redis,
        features=[{
            "name": "f1",
            "variant": "v1",
            "column": "value",
            "type": "float32"
        }],
        labels=[{
            "name": "l1",
            "variant": "lv1",
            "column": "label",
            "type": "string"
        }],
        owner=user,
        timestamp_column="date",
    )
    expected = [
        Feature(name="f1",
                variant="v1",
                source=("name", "variant"),
                value_type="float32",
                entity="user",
                owner="person",
                provider="redis",
                description="",
                location=ResourceColumnMapping(entity="user_clm",
                                               value="value",
                                               timestamp="date")),
        Label(name="l1",
              variant="lv1",
              source=("name", "variant"),
              value_type="string",
              entity="user",
              owner="person",
              description="",
              location=ResourceColumnMapping(entity="user_clm",
                                             value="label",
                                             timestamp="date")),
    ]
    assert registrar.state().sorted_list()[-2:] == expected
