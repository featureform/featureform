import marshal

import pytest

from featureform.register import ColumnSourceRegistrar, OfflineSparkProvider, Registrar
from featureform.resources import DFTransformation, Provider, Source, SparkAWSConfig, SQLTransformation


def test_create_provider():
    provider_name = "test_offline_spark_provider"
    r = Registrar()
    config = SparkAWSConfig(emr_cluster_id="",bucket_path="",emr_cluster_region="",bucket_region="",aws_access_key_id="",aws_secret_access_key="")
    provider = Provider(name=provider_name, function="OFFLINE", description="", team="", config=config)
    
    offline_spark_provider = OfflineSparkProvider(r, provider)
    assert type(offline_spark_provider) == OfflineSparkProvider
    assert offline_spark_provider.name() == provider_name

@pytest.mark.parametrize(
    "test_name,file_path",
    [
        ("file", "test_files/input/transaction")
    ]
)
def test_register_parquet_file(test_name, file_path, spark_provider):
    s = spark_provider.register_parquet_file(
        name=test_name,
        variant="test_variant",
        file_path=file_path,
    )

    assert type(s) == ColumnSourceRegistrar

    src = s._SourceRegistrar__source
    assert src.owner == spark_provider._OfflineSparkProvider__registrar.default_owner()
    assert src.provider == spark_provider.name()


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

    query = marshal.dumps(df_transformation.__code__)

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
