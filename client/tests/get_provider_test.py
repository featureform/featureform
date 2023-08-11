import featureform as ff
import os
import pytest

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.mark.local
def test_registrar_get_source():
    reg = ff.Registrar()
    result = reg.get_source(name="name", variant="variant")
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_local_provider():
    reg = ff.Registrar()
    result = reg.get_local_provider(name="unit-test")
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_redis():
    reg = ff.Registrar()
    result = reg.get_redis(name="unit-test")
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_mongodb():
    reg = ff.Registrar()
    # todox: should we change this to include username, port, etc.?
    result = reg.get_mongodb(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_blob_store():
    reg = ff.Registrar()
    result = reg.get_blob_store(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_postgres():
    reg = ff.Registrar()
    result = reg.get_postgres(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_snowflake():
    reg = ff.Registrar()
    result = reg.get_snowflake(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_snowflake_legacy():
    reg = ff.Registrar()
    result = reg.get_snowflake_legacy(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_redshift():
    reg = ff.Registrar()
    result = reg.get_redshift(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_bigquery():
    reg = ff.Registrar()
    result = reg.get_bigquery(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_spark():
    reg = ff.Registrar()
    result = reg.get_spark(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_kubernetes():
    reg = ff.Registrar()
    result = reg.get_kubernetes(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_s3():
    reg = ff.Registrar()
    result = reg.get_s3(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_gcs():
    reg = ff.Registrar()
    result = reg.get_gcs(
        name="unit-test",
    )
    print(result)
    assert True == True


@pytest.mark.local
def test_registrar_get_entity():
    reg = ff.Registrar()
    result = reg.get_entity(name="unit-test", is_local=True)
    print(result)
    assert True == True
