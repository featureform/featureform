#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import dill
import pytest

from featureform import get_random_name
from featureform.register import (
    ColumnSourceRegistrar,
    OfflineSparkProvider,
    Registrar,
)
from featureform.resources import (
    DFTransformation,
    Provider,
    SourceVariant,
    SparkConfig,
    SQLTransformation,
    SparkCredentials,
)


@pytest.mark.parametrize(
    "executor_fixture, filestore_fixture",
    [
        ("databricks", "azure_blob"),
        ("databricks", "s3"),
        ("emr", "azure_blob"),
        ("emr", "s3"),
    ],
)
def test_create_provider(executor_fixture, filestore_fixture, request):
    executor = request.getfixturevalue(executor_fixture)
    filestore = request.getfixturevalue(filestore_fixture)

    provider_name = "test_offline_spark_provider"
    r = Registrar()

    config = SparkConfig(
        executor_type=executor.type(),
        executor_config=executor.config(),
        store_type=filestore.store_type(),
        store_config=filestore.config(),
    )
    provider = Provider(
        name=provider_name,
        function="OFFLINE",
        description="",
        team="",
        config=config,
        tags=[],
        properties={},
    )

    offline_spark_provider = OfflineSparkProvider(r, provider)
    assert type(offline_spark_provider) == OfflineSparkProvider
    assert offline_spark_provider.name() == provider_name


@pytest.mark.parametrize(
    "test_name,file_path",
    [
        ("file", "abfss://test_files/input/transaction"),
    ],
)
def test_register_file(test_name, file_path, spark_provider):
    variant = get_random_name()
    s = spark_provider.register_file(
        name=test_name,
        variant=variant,
        file_path=file_path,
    )

    assert type(s) == ColumnSourceRegistrar

    src = s._SourceRegistrar__source
    assert src.owner == spark_provider._OfflineSparkProvider__registrar.default_owner()
    assert src.provider == spark_provider.name()
    assert src.variant == variant


@pytest.mark.parametrize(
    "name,variant,sql",
    [
        ("test_name", "test_variant", "SELECT * FROM {{test_name.test_variant}}"),
    ],
)
def test_sql_transformation(name, variant, sql, spark_provider):
    def transformation():
        """doc string"""
        return sql

    decorator = spark_provider.sql_transformation(name=name, variant=variant)
    decorator(transformation)

    assert decorator.to_source() == SourceVariant(
        created=None,
        name=name,
        variant=variant,
        definition=SQLTransformation(query=sql),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )


@pytest.mark.parametrize(
    "sql",
    [
        ("SELECT * FROM {{test_name.test_variant}}"),
    ],
)
def test_sql_transformation_without_variant(sql, spark_provider):
    def transformation():
        """doc string"""
        return sql

    variant = get_random_name()
    decorator = spark_provider.sql_transformation(variant=variant)
    decorator(transformation)

    assert decorator.to_source() == SourceVariant(
        created=None,
        name=transformation.__name__,
        variant=variant,
        definition=SQLTransformation(query=sql),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )


@pytest.mark.parametrize(
    "name,variant,inputs,transformation",
    [
        (
            "test_input",
            "primary_source_sql_table",
            "primary_source_sql_table",
            "avg_user_transaction",
        ),
        (
            "test_input",
            "df_transformation",
            "df_transformation_src",
            "avg_user_transaction",
        ),
        (
            "test_input",
            "sql_transformation",
            "sql_transformation_src",
            "avg_user_transaction",
        ),
        ("test_input", "tuples", "tuple_inputs", "avg_user_transaction"),
    ],
)
def test_df_transformation(
    name, variant, inputs, transformation, spark_provider, request
):
    df_transformation = request.getfixturevalue(transformation)
    inputs = request.getfixturevalue(inputs)

    decorator = spark_provider.df_transformation(
        name=name, variant=variant, inputs=inputs
    )
    decorator(df_transformation)

    query = dill.dumps(df_transformation.__code__)
    source_text = dill.source.getsource(df_transformation)

    decorator_src = decorator.to_source()
    expected_src = SourceVariant(
        created=None,
        name=name,
        variant=variant,
        definition=DFTransformation(
            query=query,
            inputs=inputs,
            source_text=source_text,
            canonical_func_text=decorator.canonical_func_text,
        ),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )

    assert decorator_src.name == expected_src.name
    assert decorator_src.owner == expected_src.owner
    assert decorator_src.provider == expected_src.provider
    assert decorator_src.description == expected_src.description
    assert decorator_src.definition.type() == expected_src.definition.type()
    assert decorator_src.definition.kwargs() == expected_src.definition.kwargs()


@pytest.mark.parametrize(
    "store_config",
    [
        "s3_config",
        "azure_file_config",
        pytest.param("s3_config_slash", marks=pytest.mark.xfail),
        pytest.param("s3_config_slash_ending", marks=pytest.mark.xfail),
    ],
)
def test_store_config(store_config, request):
    store, expected_config = request.getfixturevalue(store_config)

    assert store.config() == expected_config


@pytest.mark.parametrize(
    "executor_config",
    [
        "databricks_config",
        "emr_config",
        "spark_executor",
        pytest.param("spark_executor_incorrect_deploy_mode", marks=pytest.mark.xfail),
    ],
)
def test_executor_config(executor_config, request):
    executor, expected_config = request.getfixturevalue(executor_config)

    assert executor.config() == expected_config


@pytest.mark.parametrize(
    "version,expected_version",
    [
        ("3.7", "3.7.16"),
        ("3.8", "3.8.16"),
        ("3.9", "3.9.16"),
        ("3.10", "3.10.10"),
        ("3.7.10", "3.7.16"),
        ("3.7.1", "3.7.16"),
        ("3.8.16", "3.8.16"),
        ("3.9.11", "3.9.16"),
        ("3.10.10", "3.10.10"),
        ("3.11.2", "3.11.2"),
        pytest.param("3", "", marks=pytest.mark.xfail),
        pytest.param("3.", "", marks=pytest.mark.xfail),
        pytest.param("2.10", "", marks=pytest.mark.xfail),
        pytest.param("3.10.10.0", "", marks=pytest.mark.xfail),
    ],
)
def test__verify_python_version(version, expected_version):
    spark = SparkCredentials("local", "client", version)

    assert spark.python_version == expected_version
