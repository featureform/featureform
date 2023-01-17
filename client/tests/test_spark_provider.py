import dill

import pytest

from featureform.register import ColumnSourceRegistrar, OfflineSparkProvider, Registrar
from featureform.resources import DFTransformation, Provider, Source, SparkConfig, SQLTransformation, DatabricksCredentials, AzureFileStoreConfig


@pytest.mark.parametrize(
    "executor_fixture, filestore_fixture",
    [
        ("databricks", "azure_blob"),
        ("databricks", "s3"),
        ("emr", "azure_blob"),
        ("emr", "s3"),
    ]
)
def test_create_provider(executor_fixture, filestore_fixture, request):
    executor = request.getfixturevalue(executor_fixture)
    filestore = request.getfixturevalue(filestore_fixture)

    provider_name = "test_offline_spark_provider"
    r = Registrar() 
    
    config = SparkConfig(executor_type=executor.type(), executor_config=executor.config(), store_type=filestore.store_type(), store_config=filestore.config())
    provider = Provider(name=provider_name, function="OFFLINE", description="", team="", config=config)
    
    offline_spark_provider = OfflineSparkProvider(r, provider)
    assert type(offline_spark_provider) == OfflineSparkProvider
    assert offline_spark_provider.name() == provider_name


@pytest.mark.parametrize(
    "test_name,file_path,default_variant",
    [
        ("file", "test_files/input/transaction", False),
        ("file", "test_files/input/transaction", True),
    ]
)
def test_register_parquet_file(test_name, file_path, default_variant, spark_provider):
    if default_variant:
        variant = "default"
        s = spark_provider.register_parquet_file(
            name=test_name,
            file_path=file_path,
        )
    else:
        variant = "test_variant"
        s = spark_provider.register_parquet_file(
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
        ("test_name", "test_variant","SELECT * FROM {{test_name.test_variant}}"),
    ]
)
def test_sql_transformation(name, variant, sql, spark_provider):
    def transformation():
        """doc string"""
        return sql

    decorator = spark_provider.sql_transformation(name=name, variant=variant)
    decorator(transformation)

    assert decorator.to_source() == Source(
        name=name,
        variant=variant,
        definition=SQLTransformation(query=sql),
        owner="tester",
        provider="spark",
        description="doc string",
    )


@pytest.mark.parametrize(
    "name,variant,transformation",
    [
        ("test_name", "test_variant","avg_user_transaction"),
    ]
)
def test_df_transformation(name, variant, transformation, spark_provider, request):
    df_transformation = request.getfixturevalue(transformation)

    src_variant = f"{variant}_src"
    decorator = spark_provider.df_transformation(name=name, variant=variant, inputs=[(name, src_variant)])
    decorator(df_transformation)

    query = dill.dumps(df_transformation.__code__)

    decorator_src = decorator.to_source()
    expected_src = Source(
        name=name,
        variant=variant,
        definition=DFTransformation(query=query, inputs=[(name, src_variant)]),
        owner="tester",
        provider="spark",
        description="doc string",
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
    ]
)
def test_store_config(store_config, request):
    store, expected_config = request.getfixturevalue(store_config)

    assert store.config() == expected_config


@pytest.mark.parametrize(
    "executor_config",
    [
        "databricks_config",
        "emr_config",
    ]
)
def test_executor_config(executor_config, request):
    executor, expected_config = request.getfixturevalue(executor_config)

    assert executor.config() == expected_config
