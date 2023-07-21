import os
import shutil
import stat
import sys
import pytest
import dill

sys.path.insert(0, "client/src/")
from featureform.register import (
    ResourceClient,
    DFTransformation,
    SQLTransformation,
    PrimaryData,
    Location,
    SQLTable,
)
from featureform.resources import (
    Feature,
    Label,
    TrainingSet,
    Source,
    Transformation,
    ResourceColumnMapping,
    ResourceStatus,
)
from featureform.proto import metadata_pb2 as pb


def my_func(df):
    return df


pb_no_status = pb.ResourceStatus.Status.NO_STATUS
pb_created = pb.ResourceStatus.Status.CREATED
pb_pending = pb.ResourceStatus.Status.PENDING
pb_ready = pb.ResourceStatus.Status.READY
pb_failed = pb.ResourceStatus.Status.FAILED
sql_query = "SELECT * FROM NONE"
df_query = dill.dumps(my_func)
df_source_text = dill.source.getsource(my_func)
df_name_variants = [("name", "variant")]
primary_table_name = "my_table"

sql_definition_proto = pb.SourceVariant(
    name="",
    transformation=pb.Transformation(
        SQLTransformation=pb.SQLTransformation(query=sql_query)
    ),
)

df_definition_proto = pb.SourceVariant(
    name="",
    transformation=pb.Transformation(
        DFTransformation=pb.DFTransformation(
            query=df_query,
            inputs=[
                pb.NameVariant(name=nv[0], variant=nv[1]) for nv in df_name_variants
            ],
            source_text=df_source_text,
        )
    ),
)

primary_definition_proto = pb.SourceVariant(
    name="",
    primaryData=pb.PrimaryData(table=pb.PrimarySQLTable(name=primary_table_name)),
)

sql_definition_obj = SQLTransformation(sql_query)

df_definition_obj = DFTransformation(
    query=df_query, inputs=df_name_variants, source_text=df_source_text
)

primary_definition_obj = PrimaryData(SQLTable(primary_table_name))


# Fetches status from proto for same response that client would give
def get_pb_status(status):
    return pb.ResourceStatus.Status._enum_type.values[status].name


@pytest.mark.parametrize(
    "status,expected,ready",
    [
        (pb_no_status, ResourceStatus.NO_STATUS, False),
        (pb_created, ResourceStatus.CREATED, False),
        (pb_pending, ResourceStatus.PENDING, False),
        (pb_ready, ResourceStatus.READY, True),
        (pb_failed, ResourceStatus.FAILED, False),
    ],
)
def test_feature_status(mocker, status, expected, ready):
    # Environment variable is getting set somewhere when using Makefile
    # Needs further investigation
    os.environ.pop("FEATUREFORM_CERT", None)
    mocker.patch.object(
        ResourceClient,
        "get_feature",
        return_value=Feature(
            name="name",
            variant="",
            source=("some", "source"),
            value_type="float32",
            entity="entity",
            owner="me",
            provider="provider",
            location=ResourceColumnMapping("", "", ""),
            description="",
            status=get_pb_status(status),
            tags=[],
            properties={},
        ),
    )
    client = ResourceClient("host")
    feature = client.get_feature("", "")
    assert feature.get_status() == expected
    assert feature.status == expected.name
    assert feature.is_ready() == ready


@pytest.mark.parametrize(
    "status,expected,ready",
    [
        (pb_no_status, ResourceStatus.NO_STATUS, False),
        (pb_created, ResourceStatus.CREATED, False),
        (pb_pending, ResourceStatus.PENDING, False),
        (pb_ready, ResourceStatus.READY, True),
        (pb_failed, ResourceStatus.FAILED, False),
    ],
)
def test_label_status(mocker, status, expected, ready):
    mocker.patch.object(
        ResourceClient,
        "get_label",
        return_value=Label(
            name="name",
            variant="",
            source=("some", "source"),
            value_type="float32",
            entity="entity",
            owner="me",
            provider="provider",
            location=ResourceColumnMapping("", "", ""),
            description="",
            status=get_pb_status(status),
            tags=[],
            properties={},
        ),
    )
    client = ResourceClient("host")
    label = client.get_label("", "")
    assert label.get_status() == expected
    assert label.status == expected.name
    assert label.is_ready() == ready


@pytest.mark.parametrize(
    "status,expected,ready",
    [
        (pb_no_status, ResourceStatus.NO_STATUS, False),
        (pb_created, ResourceStatus.CREATED, False),
        (pb_pending, ResourceStatus.PENDING, False),
        (pb_ready, ResourceStatus.READY, True),
        (pb_failed, ResourceStatus.FAILED, False),
    ],
)
def test_training_set_status(mocker, status, expected, ready):
    mocker.patch.object(
        ResourceClient,
        "get_training_set",
        return_value=TrainingSet(
            name="",
            variant="",
            owner="",
            label=("something", "something"),
            features=[("some", "feature")],
            feature_lags=[],
            description="",
            status=get_pb_status(status),
            tags=[],
            properties={},
        ),
    )
    client = ResourceClient("host")
    ts = client.get_training_set("", "")
    assert ts.get_status() == expected
    assert ts.status == expected.name
    assert ts.is_ready() == ready


@pytest.mark.parametrize(
    "status,expected,ready",
    [
        (pb_no_status, ResourceStatus.NO_STATUS, False),
        (pb_created, ResourceStatus.CREATED, False),
        (pb_pending, ResourceStatus.PENDING, False),
        (pb_ready, ResourceStatus.READY, True),
        (pb_failed, ResourceStatus.FAILED, False),
    ],
)
def test_source_status(mocker, status, expected, ready):
    mocker.patch.object(
        ResourceClient,
        "get_source",
        return_value=Source(
            name="",
            variant="",
            definition=Transformation(),
            owner="me",
            provider="provider",
            description="",
            status=get_pb_status(status),
            tags=[],
            properties={},
        ),
    )
    client = ResourceClient("host")
    source = client.get_source("", "")
    assert source.get_status() == expected
    assert source.status == expected.name
    assert source.is_ready() == ready


@pytest.mark.parametrize(
    "source,expected",
    [
        (sql_definition_proto, sql_definition_obj),
    ],
)
def test_sql_source_definition_parse(source, expected):
    client = ResourceClient("host")
    source_obj = client._get_source_definition(source)
    assert source_obj.query == expected.query


@pytest.mark.parametrize(
    "source,expected",
    [
        (df_definition_proto, df_definition_obj),
    ],
)
def test_df_source_definition_parse(source, expected):
    client = ResourceClient("host")
    source_obj = client._get_source_definition(source)
    assert source_obj.query == expected.query
    assert source_obj.inputs == expected.inputs


@pytest.mark.parametrize(
    "source,expected",
    [
        (primary_definition_proto, primary_definition_obj),
    ],
)
def test_primary_source_definition_parse(source, expected):
    client = ResourceClient("host")
    source_obj = client._get_source_definition(source)
    assert source_obj.location.name == expected.location.name
