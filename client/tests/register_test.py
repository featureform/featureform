import os
import shutil
import stat
import sys
import featureform as ff

sys.path.insert(0, "client/src/")
import pytest
from featureform.register import (
    LocalProvider,
    Provider,
    Registrar,
    LocalConfig,
    SQLTransformationDecorator,
    DFTransformationDecorator,
    SnowflakeConfig,
    Model,
)


@pytest.mark.parametrize(
    "account,organization,account_locator,should_error",
    [
        ["", "", "", True],
        ["account", "", "", True],
        ["", "org", "", True],
        ["account", "org", "", False],
        ["", "", "account_locator", False],
        ["account", "org", "account_locator", True],
    ],
)
def test_snowflake_config_credentials(
    account, organization, account_locator, should_error
):
    if should_error:
        with pytest.raises(ValueError):
            SnowflakeConfig(
                account=account,
                organization=organization,
                account_locator=account_locator,
                username="",
                password="",
                schema="",
            )
    else:  # Creating Obj should not error with proper credentials
        SnowflakeConfig(
            account=account,
            organization=organization,
            account_locator=account_locator,
            username="",
            password="",
            schema="",
        )


@pytest.fixture
def local():
    config = LocalConfig()
    provider = Provider(
        name="local-mode",
        function="LOCAL_ONLINE",
        description="This is local mode",
        team="team",
        config=config,
        tags=[],
        properties={},
    )
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
    decorator = local.sql_transformation(variant="var", owner="owner")
    with pytest.raises((TypeError, ValueError)):
        decorator(fn)


def test_sql_transformation_empty_description(registrar):
    def my_function():
        return "SELECT * FROM X"

    dec = SQLTransformationDecorator(
        registrar=registrar,
        owner="",
        provider="",
        variant="sql",
        tags=[],
        properties={},
    )
    dec.__call__(my_function)

    # Checks that Transformation definition does not error when converting to source
    dec.to_source()


def test_df_transformation_empty_description(registrar):
    def my_function(df):
        return df

    dec = DFTransformationDecorator(
        registrar=registrar, owner="", provider="", variant="df", tags=[], properties={}
    )
    dec.__call__(my_function)

    # Checks that Transformation definition does not error when converting to source
    dec.to_source()


def test_valid_model_registration():
    model_name = "model_a"

    model = ff.register_model(model_name)

    assert isinstance(model, Model) and model.name == model_name


def test_invalid_model_registration():
    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'name'"
    ):
        model = ff.register_model()


@pytest.mark.parametrize(
    "provider_name,func",
    [("snowflake", ff.get_snowflake), ("snowflake_legacy", ff.get_snowflake_legacy)],
)
def test_get_snowflake_functions(provider_name, func):
    offlineSQLProvider = func(provider_name)
    assert offlineSQLProvider.name() == provider_name


@pytest.mark.parametrize(
    "tuple,error",
    [
        (("name", "variant"), None),
        (
            ("name", "variant", "owner"),
            TypeError("Tuple must be of length 2, got length 3"),
        ),
        (("name"), TypeError("not a tuple; received: 'str' type")),
        (
            ("name",),
            TypeError("Tuple must be of length 2, got length 1"),
        ),
        (
            ("name", [1, 2, 3]),
            TypeError("Tuple must be of type (str, str); got (str, list)"),
        ),
        (
            ([1, 2, 3], "variant"),
            TypeError("Tuple must be of type (str, str); got (list, str)"),
        ),
    ],
)
def test_local_provider_verify_inputs(tuple, error):
    try:
        r = Registrar()
        assert r._verify_tuple(tuple) is None and error is None
    except Exception as e:
        assert type(e).__name__ == type(error).__name__
        assert str(e) == str(error)


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Remove any lingering Databases
    try:
        shutil.rmtree(".featureform", onerror=del_rw)
    except:
        print("File Already Removed")
    yield
    try:
        shutil.rmtree(".featureform", onerror=del_rw)
    except:
        print("File Already Removed")


@pytest.mark.parametrize(
    "sql_query, expected_valid_sql_query",
    [
        ("SELECT * FROM X", False),
        ("SELECT * FROM", False),
        ("SELECT * FROM {{ name.variant }}", True),
        ("SELECT * FROM {{name.variant }}", True),
        ("SELECT * FROM     \n {{ name.variant }}", True),
        (
            """
        SELECT *
        FROM {{ name.variant2 }}
        WHERE x >= 5.
        """,
            True,
        ),
        ("SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id", True),
    ],
)
def test_validate_sql_query(sql_query, expected_valid_sql_query):
    dec = SQLTransformationDecorator(
        registrar=registrar,
        owner="",
        provider="",
        variant="sql",
        tags=[],
        properties={},
    )

    is_valid = dec._is_valid_sql_query(sql_query)
    assert is_valid == expected_valid_sql_query
