#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import ast
import os
import shutil
import stat
import sys
import textwrap

import featureform as ff
from featureform import InvalidSQLQuery

sys.path.insert(0, "client/src/")
import pytest
from featureform.register import (
    Registrar,
    SQLTransformationDecorator,
    DFTransformationDecorator,
    SnowflakeConfig,
    Model,
    get_name_variant,
    Variants,
    FeatureColumnResource,
    LabelColumnResource,
)
from featureform.resources import Provider, TrainingSetVariant, PrimaryData, SQLTable
from featureform.proto import metadata_pb2 as pb
from featureform.enums import ResourceType


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
    "sql_query, expected_error",
    [
        ("SELECT * FROM X", InvalidSQLQuery("SELECT * FROM X", "No source specified.")),
        ("SELECT * FROM", InvalidSQLQuery("SELECT * FROM", "No source specified.")),
        ("SELECT * FROM     \n {{ name }}", None),
        ("SELECT * FROM     \n {{name}}", None),
        ("SELECT * FROM {{ name.variant }}", None),
        ("SELECT * FROM {{name.variant }}", None),
        ("SELECT * FROM     \n {{ name.variant }}", None),
        ("SELECT * FROM     \n {{name.variant}}", None),
        (
            "SELECT * FROM     \n {{name . variant}}",
            InvalidSQLQuery(
                "SELECT * FROM     \n {{name . variant}}",
                "Source name cannot start or end with a space.",
            ),
        ),
        (
            """
                                                        SELECT *
                                                        FROM {{ name.variant2 }}
                                                        WHERE x >= 5.
                                                        """,
            None,
        ),
        (
            "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id",
            None,
        ),
        (
            (
                "SELECT CustomerID as user_id, avg(TransactionAmount) "
                "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
            ),
            None,
        ),
        (
            "SELECT * FROM {{ transactions.2024-04-18t16-13-22 }}",
            None,
        ),
        (
            "SELECT * FROM {{ _transactions.2024-04-18t16-13-22 }}",
            InvalidSQLQuery(
                "SELECT * FROM {{ _transactions.2024-04-18t16-13-22 }}",
                "Source name cannot start or end with an underscore.",
            ),
        ),
        (
            "SELECT * FROM {{ transactions_.2024-04-18t16-13-22 }}",
            InvalidSQLQuery(
                "SELECT * FROM {{ transactions_.2024-04-18t16-13-22 }}",
                "Source name cannot start or end with an underscore.",
            ),
        ),
        (
            "SELECT * FROM {{ transactions._2024-04-18t16-13-22 }}",
            InvalidSQLQuery(
                "SELECT * FROM {{ transactions._2024-04-18t16-13-22 }}",
                "Source variant cannot start or end with an underscore.",
            ),
        ),
        (
            "SELECT * FROM {{ transactions.2024-04-18t16-13-22_ }}",
            InvalidSQLQuery(
                "SELECT * FROM {{ transactions.2024-04-18t16-13-22_ }}",
                "Source variant cannot start or end with an underscore.",
            ),
        ),
        (
            "SELECT * FROM {{ trans__actions.2024-04-18t16-13-22 }}",
            InvalidSQLQuery(
                "SELECT * FROM {{ trans__actions.2024-04-18t16-13-22 }}",
                "Source name and variant cannot contain consecutive underscores.",
            ),
        ),
        (
            "SELECT * FROM {{ transactions.var__iant }}",
            InvalidSQLQuery(
                "SELECT * FROM {{ transactions.var__iant }}",
                "Source name and variant cannot contain consecutive underscores.",
            ),
        ),
        (
            "SELECT * FROM {{ trans.actions.variant}}",
            InvalidSQLQuery(
                "SELECT * FROM {{ trans.actions.variant}}",
                "Source name and variant cannot contain more than one period.",
            ),
        ),
    ],
)
def test_assert_query_contains_at_least_one_source(sql_query, expected_error):
    dec = SQLTransformationDecorator(
        registrar=registrar,
        owner="",
        provider="",
        variant="sql",
        tags=[],
        properties={},
    )

    if expected_error:
        with pytest.raises(InvalidSQLQuery) as ex_info:
            dec._assert_query_contains_at_least_one_source(sql_query)
        assert str(ex_info.value) == str(expected_error)
    else:
        dec._assert_query_contains_at_least_one_source(sql_query)


@pytest.mark.parametrize(
    "sql_query, source_str, expected_name, expected_variant, expected_error",
    [
        ("SELECT * FROM {{ name.variant }}", "name.variant", "name", "variant", None),
        ("SELECT * FROM {{ name }}", "name", "name", "", None),
        (
            "SELECT * FROM {{ name.vari.ant }}",
            "name.vari.ant",
            "name",
            "variant",
            InvalidSQLQuery(
                "SELECT * FROM {{ name.vari.ant }}",
                "Source name and variant cannot contain more than one period.",
            ),
        ),
    ],
)
def test_get_name_variant(
    sql_query, source_str, expected_name, expected_variant, expected_error
):
    if expected_error:
        with pytest.raises(InvalidSQLQuery) as ex_info:
            name, variant = get_name_variant(sql_query, source_str)
        assert str(ex_info.value) == str(expected_error)
    else:
        name, variant = get_name_variant(sql_query, source_str)

        assert name == expected_name
        assert variant == expected_variant


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


feature_dict = {
    "name": "feature_name",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
    "default_variant": "default",
    "variants": ["not_default", "default"],
}
label_dict = {
    "name": "label_name",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
    "default_variant": "default",
    "variants": ["default"],
}
source_dict = {
    "name": "source_name",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
    "default_variant": "variant3",
    "variants": ["variant1, variant2, variant3"],
}
ts_dict = {
    "name": "ts_name",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
    "default_variant": "2024-07-26t10-42-25",
    "variants": ["2024-07-24t10-28-25", "2024-07-26t10-42-25"],
}

ts2_dict = {
    "name": "ts2",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
    "default_variant": "var",
    "variants": ["var"],
}

test_model = ff.Model("model_name")

test_variant = Variants(
    {
        "res1": FeatureColumnResource(
            transformation_args=(
                Registrar(),
                "source_variant",
                ["col1", "col2"],
            ),
            type="string",
            name="feature_name",
        ),
        "res2": LabelColumnResource(
            transformation_args=(
                Registrar(),
                "source_variant",
                ["col1", "col2"],
            ),
            type="string",
            name="label_name",
        ),
    }
)

source_variant_dict = {
    "name": "source_name",
    "variant": "variant1",
    "primaryData": pb.PrimaryData(
        table=pb.SQLTable(name="name", database="database", schema="schema"),
        timestamp_column="T23-40-23",
    ),
    "owner": "User",
    "provider": "Provider",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
    "table": "transactions",
}

feature_variant_dict = {
    "name": "feature_name",
    "variant": "variant1",
    "source": pb.NameVariant(name="source_name", variant="source_variant"),
    "entity": "User",
    "description": "its a feature",
    "provider": "Provider",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
}

label_variant_dict = {
    "name": "label_name",
    "variant": "variant1",
    "source": pb.NameVariant(name="source_name", variant="source_variant"),
    "entity": "User",
    "owner": "Me",
    "description": "its a label",
    "provider": "Provider",
    "status": pb.ResourceStatus(status=pb.ResourceStatus.Status.CREATED),
}


class MockStub:
    def GetFeatures(self, req):
        feature_name = next(req).name.name
        if feature_name == "feature_name":
            yield pb.Feature(**feature_dict)

    def GetLabels(self, req):
        label_name = next(req).name.name
        if label_name == "label_name":
            yield pb.Label(**label_dict)

    def GetSources(self, req):
        source_name = next(req).name.name
        if source_name == "source_name":
            yield pb.Source(**source_dict)

    def GetTrainingSets(self, req):
        training_set_name = next(req).name.name
        if training_set_name == "ts_name":
            yield pb.TrainingSet(**ts_dict)
        elif training_set_name == "ts2":
            yield pb.TrainingSet(**ts2_dict)

    def GetSourceVariants(self, req):
        source_name = next(req).name_variant.name
        if source_name == "source_name":
            yield pb.SourceVariant(**source_variant_dict)

    def GetFeatureVariants(self, req):
        feature_name = next(req).name_variant.name
        if feature_name == "feature_name":
            yield pb.FeatureVariant(**feature_variant_dict)

    def GetLabelVariants(self, req):
        label_name = next(req).name_variant.name
        if label_name == "label_name":
            yield pb.LabelVariant(**label_variant_dict)


@pytest.mark.parametrize(
    "resource_name, resource_type, expected_output, expected_error, expected_error_msg",
    [
        (
            "feature_name",
            ResourceType.FEATURE_VARIANT,
            feature_dict["variants"],
            None,
            "",
        ),
        ("label_name", ResourceType.LABEL_VARIANT, label_dict["variants"], None, ""),
        ("source_name", ResourceType.SOURCE_VARIANT, source_dict["variants"], None, ""),
        ("ts_name", ResourceType.TRAININGSET_VARIANT, ts_dict["variants"], None, ""),
        (
            TrainingSetVariant(
                name="ts2",
                owner="User",
                label=("label", "variant"),
                features=[("feature", "variant")],
                description="description",
                variant="var",
            ),
            None,
            ["var"],
            None,
            "",
        ),
        (
            "feature_without_type",
            None,
            None,
            ValueError,
            "A resource type param must be provided if the resource name param is of type string.",
        ),
        (
            test_model,
            None,
            None,
            ValueError,
            f"Expected input: string, Feature, Label, Source or Training Set object, actual input: {test_model}.",
        ),
        (
            "provider_name",
            ResourceType.PROVIDER,
            None,
            ValueError,
            "Resource type Provider doesnt have variants.",
        ),
    ],
)
def test_get_variants(
    resource_name, resource_type, expected_output, expected_error, expected_error_msg
):
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()
    if expected_error is None:
        all_variants = client.get_variants(resource_name, resource_type)
        assert all_variants == expected_output
    else:
        with pytest.raises(expected_error) as e:
            all_variants = client.get_variants(resource_name, resource_type)
        assert str(e.value) == expected_error_msg


@pytest.mark.parametrize(
    "resource_name, resource_type, expected_output, expected_error, expected_error_msg",
    [
        (
            "feature_name",
            ResourceType.FEATURE_VARIANT,
            feature_dict["default_variant"],
            None,
            "",
        ),
        (
            "label_name",
            ResourceType.LABEL_VARIANT,
            label_dict["default_variant"],
            None,
            "",
        ),
        (
            "source_name",
            ResourceType.SOURCE_VARIANT,
            source_dict["default_variant"],
            None,
            "",
        ),
        (
            "ts_name",
            ResourceType.TRAININGSET_VARIANT,
            ts_dict["default_variant"],
            None,
            "",
        ),
        (
            TrainingSetVariant(
                name="ts2",
                owner="User",
                label=("label", "variant"),
                features=[("feature", "variant")],
                description="description",
                variant="var",
            ),
            None,
            "var",
            None,
            "",
        ),
        (
            "feature_without_type",
            None,
            None,
            ValueError,
            "A resource type param must be provided if the resource name param is of type string.",
        ),
        (
            test_model,
            None,
            None,
            ValueError,
            (
                f"Expected input: string, Feature, Label, Source or Training Set object, actual input: {test_model}."
            ),
        ),
        (
            "provider_name",
            ResourceType.PROVIDER,
            None,
            ValueError,
            "Resource type Provider doesnt have variants.",
        ),
        (
            test_variant,
            None,
            "",
            ValueError,
            f"{test_variant} is of type Variants. Please provide a resource type.",
        ),
    ],
)
def test_get_latest_variant(
    resource_name, resource_type, expected_output, expected_error, expected_error_msg
):
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()
    if expected_error is None:
        all_variants = client.latest_variant(resource_name, resource_type)
        assert all_variants == expected_output
    else:
        with pytest.raises(expected_error) as e:
            all_variants = client.latest_variant(resource_name, resource_type)
        assert str(e.value) == expected_error_msg


def test_get_source():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()
    source_variant_column_registrar = client.get_source("source_name", "variant1")
    source_variant = source_variant_column_registrar.source()
    assert source_variant.name == source_variant_dict["name"]
    assert source_variant.definition == PrimaryData(
        SQLTable(schema="schema", name="name", database="database"), "T23-40-23"
    )
    assert source_variant.owner == source_variant_dict["owner"]
    assert source_variant.provider == source_variant_dict["provider"]
    assert source_variant.created == None
    assert source_variant.status == "CREATED"


def test_get_feature():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()
    feature_variant = client.get_feature("feature_name", "variant1")
    assert feature_variant.name == feature_variant_dict["name"]
    assert feature_variant.status == "CREATED"
    assert feature_variant.entity == feature_variant_dict["entity"]
    assert feature_variant.variant == feature_variant_dict["variant"]
    assert feature_variant.description == feature_variant_dict["description"]
    assert feature_variant.provider == feature_variant_dict["provider"]
    assert feature_variant.source[0] == "source_name"
    assert feature_variant.source[1] == "source_variant"


def test_get_label():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()
    label_variant = client.get_label("label_name", "variant1")
    assert label_variant.name == label_variant_dict["name"]
    assert label_variant.status == "CREATED"
    assert label_variant.entity == label_variant_dict["entity"]
    assert label_variant.owner == label_variant_dict["owner"]
    assert label_variant.variant == label_variant_dict["variant"]
    assert label_variant.description == label_variant_dict["description"]
    assert label_variant.provider == label_variant_dict["provider"]
    assert label_variant.source[0] == "source_name"
    assert label_variant.source[1] == "source_variant"
