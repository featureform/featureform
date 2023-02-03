import os
import shutil
import stat
import sys

sys.path.insert(0, 'client/src/')
import pytest
from featureform.register import LocalProvider, Provider, Registrar, LocalConfig, SQLTransformationDecorator, \
    DFTransformationDecorator, SnowflakeConfig


@pytest.mark.parametrize(
    "account,organization,account_locator,should_error",
    [
        ["", "", "", True],
        ["account", "", "", True],
        ["", "org", "", True],
        ["account", "org", "", False],
        ["", "", "account_locator", False],
        ["account", "org", "account_locator", True],
    ]
)
def test_snowflake_config_credentials(account, organization, account_locator, should_error):
    if should_error:
        with pytest.raises(ValueError):
            SnowflakeConfig(account=account, organization=organization, account_locator=account_locator, username="",
                            password="", schema="")
    else:  # Creating Obj should not error with proper credentials
        SnowflakeConfig(account=account, organization=organization, account_locator=account_locator, username="",
                        password="", schema="")


@pytest.fixture
def local():
    config = LocalConfig()
    provider = Provider(name="local-mode",
                        function="LOCAL_ONLINE",
                        description="This is local mode",
                        team="team",
                        config=config)
    return LocalProvider(Registrar(), provider)


@pytest.fixture
def registrar():
    return Registrar()


def name():
    """doc string"""
    return "query"


def empty_string():
    return ""


def return_5():
    return 5


@pytest.mark.parametrize("fn", [empty_string, return_5])
def test_sql_transformation_decorator_invalid_fn(local, fn):
    decorator = local.sql_transformation(
        variant="var",
        owner="owner"
    )
    with pytest.raises((TypeError, ValueError)):
        decorator(fn)


def test_sql_transformation_empty_description(registrar):
    def my_function():
        return "SELECT * FROM X"

    dec = SQLTransformationDecorator(registrar=registrar, owner="", provider="", variant="sql")
    dec.__call__(my_function)

    # Checks that Transformation definition does not error when converting to source
    dec.to_source()


def test_df_transformation_empty_description(registrar):
    def my_function(df):
        return df

    dec = DFTransformationDecorator(registrar=registrar, owner="", provider="", variant="df")
    dec.__call__(my_function)

    # Checks that Transformation definition does not error when converting to source
    dec.to_source()


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Remove any lingering Databases
    try:
        shutil.rmtree('.featureform', onerror=del_rw)
    except:
        print("File Already Removed")
    yield
    try:
        shutil.rmtree('.featureform', onerror=del_rw)
    except:
        print("File Already Removed")
