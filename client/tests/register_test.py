import os
import shutil
import stat
import sys

import featureform as ff
from featureform import InvalidSQLQuery

sys.path.insert(0, "client/src/")
import pytest
from featureform.register import (
    Provider,
    Registrar,
    SQLTransformationDecorator,
    DFTransformationDecorator,
    SnowflakeConfig,
    Model,
)

from featureform.resources import ScheduleTriggerResource


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
def registrar():
    return Registrar()


def name():
    """doc string"""
    return "query"


def empty_string():
    return ""


def return_5():
    return 5


@pytest.mark.parametrize(
    "sql_query, expected_query, inputs",
    [
        (
            "SELECT * FROM {{ arg1 }} JOIN {{ arg2 }}",
            "SELECT * FROM {{ df.var }} JOIN {{ df2.var2 }}",
            [("df", "var"), ("df2", "var2")],
        ),
        (
            "SELECT * FROM {{ arg1 }} JOIN {{ arg1 }} JOIN {{ arg2 }} JOIN {{ arg2 }}",
            "SELECT * FROM {{ df.var }} JOIN {{ df.var }} JOIN {{ df2.var2 }} JOIN {{ df2.var2 }}",
            [("df", "var"), ("df2", "var2")],
        ),
        (
            "SELECT * FROM {{arg1}} JOIN {{         arg1  }} JOIN {{     arg2 }} JOIN {{ arg2       }}",
            "SELECT * FROM {{ df.var }} JOIN {{ df.var }} JOIN {{ df2.var2 }} JOIN {{ df2.var2 }}",
            [("df", "var"), ("df2", "var2")],
        ),
        (
            "SELECT * FROM {{arg1}} JOIN {{         arg1  }} JOIN {{     arg2 }} JOIN {{ some_transformation.variant       }}",
            "SELECT * FROM {{ df.var }} JOIN {{ df.var }} JOIN {{ df2.var2 }} JOIN {{ some_transformation.variant       }}",
            [("df", "var"), ("df2", "var2")],
        ),
    ],
)
def test_sql_transformation_inputs_valid(registrar, sql_query, expected_query, inputs):
    def my_function(arg1, arg2):
        return sql_query

    dec = SQLTransformationDecorator(
        registrar=registrar,
        owner="",
        provider="",
        variant="sql",
        tags=[],
        properties={},
        inputs=inputs,
    )
    dec.__call__(my_function)

    # Check that Transformation definition does not error when converting to source
    source = dec.to_source()
    assert (
        source.definition.kwargs()["transformation"].SQLTransformation.query
        == expected_query
    )


@pytest.mark.parametrize(
    "fn, inputs, error_message",
    [
        (
            lambda arg1, arg2, arg3: "SELECT * FROM {{ arg1 }} JOIN {{ arg2 }}",
            [("df", "var"), ("df2", "var2")],
            "Transformation function has more parameters than inputs.",
        ),
        (
            lambda arg1, arg2: "SELECT * FROM {{ arg1 }} JOIN {{ arg2 }} JOIN {{ arg3 }}",
            [("df", "var"), ("df2", "var2")],
            "SQL placeholder '{{ arg3 }}' not found in input arguments",
        ),
        (
            lambda arg1, arg2: "SELECT * FROM {{ arg1 }} JOIN {{ arg2 }}",
            [],
            "Transformation function has more parameters than inputs.",
        ),
        (
            lambda arg1: "SELECT * FROM {{ arg1 }} JOIN {{ arg2 }}",
            [("df", "var"), ("df2", "var2")],
            "Too many inputs for transformation function.",
        ),
    ],
)
def test_sql_transformation_inputs_error(registrar, fn, inputs, error_message):
    with pytest.raises(ValueError) as e:
        dec = SQLTransformationDecorator(
            registrar=registrar,
            owner="",
            provider="",
            variant="sql",
            tags=[],
            properties={},
            inputs=inputs,
        )
        dec.__call__(fn)
        dec.to_source().definition.kwargs()
    assert error_message in str(e.value)


def test_sql_transformation_empty_description(registrar):
    def my_function():
        return "SELECT * FROM {{ name.variant }}"

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
        registrar=registrar,
        owner="",
        provider="",
        variant="df",
        tags=[],
        properties={},
        inputs=[("df", "var")],
    )
    dec.__call__(my_function)

    # Checks that Transformation definition does not error when converting to source
    dec.to_source()


@pytest.mark.parametrize(
    # fmt: off
    "func,args,should_raise",
    [
        # Same number of arguments, should not raise an error
        (
                lambda a, b: None,
                [("name1", "var1"), ("name2", "var2")],
                False,
        ),
        # 0 function arguments, 1 decorator argument, should not raise an error
        (
                lambda: None,
                [("name1", "var1")],
                False
        ),
        # 1 function argument, 0 decorator arguments, should raise an error
        (
                lambda df: None,
                [],
                True
        ),
        # 5 function arguments, 3 decorator arguments, should raise an error
        (
                lambda a, b, c, d, e: None,
                [("name1", "var1"), ("name2", "var2"), ("name3", "var3")],
                True,
        ),
        # 2 function arguments, 5 decorator arguments, should not raise an error
        (
                lambda x, y: None,
                [("name1", "var1"), ("name2", "var2"), ("name3", "var3"), ("name4", "var4")],
                False,
        ),
    ],
    # fmt: on
)
def test_transformations_invalid_args_and_inputs(registrar, func, args, should_raise):
    dec = DFTransformationDecorator(
        registrar=registrar,
        owner="",
        provider="",
        variant="df",
        inputs=args,
        tags=[],
        properties={},
    )

    if should_raise:
        with pytest.raises(ValueError) as e:
            dec(func)

        assert "Transformation function has more parameters than inputs." in str(
            e.value
        )
    else:
        dec(func)  # Should not raise an error


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
        ("SELECT * FROM     \n {{ name }}", True),
        ("SELECT * FROM     \n {{name}}", True),
        ("SELECT * FROM {{ name.variant }}", True),
        ("SELECT * FROM {{name.variant }}", True),
        ("SELECT * FROM     \n {{ name.variant }}", True),
        ("SELECT * FROM     \n {{name.variant}}", True),
        ("SELECT * FROM     \n {{name . variant}}", False),
        (
            """
                                            SELECT *
                                            FROM {{ name.variant2 }}
                                            WHERE x >= 5.
                                            """,
            True,
        ),
        (
            "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id",
            True,
        ),
        (
            (
                "SELECT CustomerID as user_id, avg(TransactionAmount) "
                "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
            ),
            True,
        ),
    ],
)
def test_assert_query_contains_at_least_one_source(sql_query, expected_valid_sql_query):
    dec = SQLTransformationDecorator(
        registrar=registrar,
        owner="",
        provider="",
        variant="sql",
        tags=[],
        properties={},
    )

    if not expected_valid_sql_query:
        with pytest.raises(InvalidSQLQuery) as ex_info:
            dec._assert_query_contains_at_least_one_source(sql_query)
        assert (
            str(ex_info.value)
            == f"Invalid SQL query. Query: ' {sql_query} ' No source specified."
        )
    else:
        dec._assert_query_contains_at_least_one_source(sql_query)


@pytest.mark.parametrize(
    "bucket_name, expected_error",
    [
        ("s3://bucket_name", None),
        ("bucket_name", None),
        ("s3a://bucket_name", None),
        (
            "bucket_name/",
            ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the AWS S3 bucket only."
            ),
        ),
        (
            "s3://bucket_name/",
            ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the AWS S3 bucket only."
            ),
        ),
        (
            "s3a://bucket_name/",
            ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the AWS S3 bucket only."
            ),
        ),
    ],
)
def test_register_s3(bucket_name, expected_error, ff_registrar, aws_credentials):
    try:
        _ = ff_registrar.register_s3(
            name="s3_bucket",
            credentials=aws_credentials,
            bucket_region="us-east-1",
            bucket_name=bucket_name,
        )
    except ValueError as ve:
        assert str(ve) == str(expected_error)
    except Exception as e:
        raise e


@pytest.mark.parametrize(
    "bucket_name, expected_error",
    [
        ("gs://bucket_name", None),
        ("bucket_name", None),
        (
            "bucket_name/",
            ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the GCS bucket only."
            ),
        ),
        (
            "gs://bucket_name/",
            ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the GCS bucket only."
            ),
        ),
    ],
)
def test_register_gcs(bucket_name, expected_error, ff_registrar, gcp_credentials):
    try:
        _ = ff_registrar.register_gcs(
            name="gcs_bucket",
            bucket_name=bucket_name,
            root_path="",
            credentials=gcp_credentials,
        )
    except ValueError as ve:
        assert str(ve) == str(expected_error)
    except Exception as e:
        raise e


@pytest.mark.parametrize(
    "container_name, expected_error",
    [
        ("abfss://container_name", None),
        ("container_name", None),
        (
            "container_name/",
            ValueError(
                "container_name cannot contain '/'. container_name should be the name of the Azure Blobstore container only."
            ),
        ),
        (
            "abfss://bucket_name/",
            ValueError(
                "container_name cannot contain '/'. container_name should be the name of the Azure Blobstore container only."
            ),
        ),
    ],
)
def test_register_blob_store(container_name, expected_error, ff_registrar):
    try:
        _ = ff_registrar.register_blob_store(
            name="blob_store_container",
            container_name=container_name,
            root_path="custom/path/in/container",
            account_name="account_name",
            account_key="azure_account_key",
        )
    except ValueError as ve:
        assert str(ve) == str(expected_error)
    except Exception as e:
        raise e


@pytest.mark.local
def test_register_schedule_trigger():
    result = ff.ScheduleTrigger("name", "* * * * *")
    assert isinstance(result, ScheduleTriggerResource)
