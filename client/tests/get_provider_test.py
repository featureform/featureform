import os
import pytest

from client.src.featureform.register import (
    EntityRegistrar,
    LocalProvider,
    OnlineProvider,
    FileStoreProvider,
    OfflineSQLProvider,
    OfflineSparkProvider,
    OfflineK8sProvider,
    ColumnSourceRegistrar,
    Registrar,
)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.mark.local
def test_registrar_get_source():
    reg = Registrar()
    result = reg.get_source(name="name", variant="variant")
    assert isinstance(result, ColumnSourceRegistrar)


@pytest.mark.local
def test_registrar_get_local_provider():
    reg = Registrar()
    result = reg.get_local_provider(name="unit-test")
    assert isinstance(result, LocalProvider)


@pytest.mark.local
def test_registrar_get_redis():
    reg = Registrar()
    result = reg.get_redis(name="unit-test")
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_registrar_get_mongodb():
    reg = Registrar()
    # todox: should we change this to include username, port, etc.?
    result = reg.get_mongodb(
        name="unit-test",
    )
    assert isinstance(result, OnlineProvider)


@pytest.mark.local
def test_registrar_get_blob_store():
    reg = Registrar()
    result = reg.get_blob_store(
        name="unit-test",
    )
    assert isinstance(result, FileStoreProvider)


@pytest.mark.local
def test_registrar_get_postgres():
    reg = Registrar()
    result = reg.get_postgres(
        name="unit-test",
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_registrar_get_snowflake():
    reg = Registrar()
    result = reg.get_snowflake(
        name="unit-test",
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_registrar_get_snowflake_legacy():
    reg = Registrar()
    result = reg.get_snowflake_legacy(
        name="unit-test",
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_registrar_get_redshift():
    reg = Registrar()
    result = reg.get_redshift(
        name="unit-test",
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_registrar_get_bigquery():
    reg = Registrar()
    result = reg.get_bigquery(
        name="unit-test",
    )
    assert isinstance(result, OfflineSQLProvider)


@pytest.mark.local
def test_registrar_get_spark():
    reg = Registrar()
    result = reg.get_spark(
        name="unit-test",
    )
    assert isinstance(result, OfflineSparkProvider)


@pytest.mark.local
def test_registrar_get_kubernetes():
    reg = Registrar()
    result = reg.get_kubernetes(
        name="unit-test",
    )
    assert isinstance(result, OfflineK8sProvider)


@pytest.mark.local
def test_registrar_get_s3():
    reg = Registrar()
    result = reg.get_s3(
        name="unit-test",
    )
    assert isinstance(result, FileStoreProvider)


@pytest.mark.local
def test_registrar_get_gcs():
    reg = Registrar()
    result = reg.get_gcs(
        name="unit-test",
    )
    assert isinstance(result, OfflineK8sProvider)


@pytest.mark.local
def test_registrar_get_entity():
    reg = Registrar()
    result = reg.get_entity(name="unit-test", is_local=True)
    assert isinstance(result, EntityRegistrar)
