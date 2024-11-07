#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import sys
import pytest
import dill
import random
from unittest.mock import MagicMock


sys.path.insert(0, "client/src/")
from featureform.register import (
    ResourceClient,
    DFTransformation,
    SQLTransformation,
    PrimaryData,
    SQLTable,
)
from featureform.resources import (
    FeatureVariant,
    LabelVariant,
    TrainingSetVariant,
    SourceVariant,
    Transformation,
    ResourceColumnMapping,
    ResourceState,
)
from featureform.proto import metadata_pb2 as pb
from featureform.enums import ResourceType, ResourceStatus
from featureform.status_display import (
    DisplayStatus,
    ResourceTableRow,
    StatusDisplayer,
    NUM_DISPLAY_ROWS,
)


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
    primaryData=pb.PrimaryData(table=pb.SQLTable(name=primary_table_name)),
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
        return_value=FeatureVariant(
            created=None,
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
        return_value=LabelVariant(
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
        return_value=TrainingSetVariant(
            created=None,
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
        return_value=SourceVariant(
            created=None,
            name="",
            variant="",
            definition=SQLTransformation(query=""),
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
    source_obj = SourceVariant._get_source_definition(source)
    assert source_obj.query == expected.query


@pytest.mark.parametrize(
    "source,expected",
    [
        (df_definition_proto, df_definition_obj),
    ],
)
def test_df_source_definition_parse(source, expected):
    source_obj = SourceVariant._get_source_definition(source)
    assert source_obj.query == expected.query
    assert source_obj.inputs == expected.inputs


@pytest.mark.parametrize(
    "source,expected",
    [
        (primary_definition_proto, primary_definition_obj),
    ],
)
def test_primary_source_definition_parse(source, expected):
    source_obj = SourceVariant._get_source_definition(source)
    assert source_obj.location.name == expected.location.name


@pytest.mark.parametrize(
    "host, resource_type, resource_name, variant, expected_url",
    [
        (
            "localhost:7878",
            ResourceType.FEATURE_VARIANT,
            "featurename",
            "variant",
            "http://localhost/features/featurename?variant=variant",
        ),
        (
            "gcp.featureform.com",
            ResourceType.SOURCE_VARIANT,
            "sourcename",
            "variant",
            "https://gcp.featureform.com/sources/sourcename?variant=variant",
        ),
        (
            "localhost:7878",
            ResourceType.LABEL_VARIANT,
            "labelname",
            "",
            "http://localhost/labels/labelname",
        ),
        (
            "localhost:7878",
            ResourceType.PROVIDER,
            "providername",
            "",
            "http://localhost/providers/providername",
        ),
        (
            "localhost:7878",
            ResourceType.TRANSFORMATION,
            "transformationname",
            "variant",
            "http://localhost/sources/transformationname?variant=variant",
        ),
    ],
)
def test_build_dashboard_url(host, resource_type, resource_name, variant, expected_url):
    url = ResourceState().build_dashboard_url(
        host, resource_type, resource_name, variant
    )
    assert url == expected_url


def test_failed_dashboard_url():
    with pytest.raises(KeyError):
        ResourceState().build_dashboard_url(
            "localhost:7878", "Server", "name", "variant"
        )


displaystatus_feature_pending = DisplayStatus(
    ResourceType.FEATURE_VARIANT, "displaystatus", "feature", "PENDING", ""
)
displaystatus_feature_ready = DisplayStatus(
    ResourceType.FEATURE_VARIANT, "displaystatus", "feature", "READY", ""
)
displaystatus_label_pending = DisplayStatus(
    ResourceType.LABEL_VARIANT, "displaystatus", "label", "PENDING", ""
)
training_set = TrainingSetVariant(
    name="ts2",
    owner="User",
    label=("label", "variant"),
    features=[("feature", "variant")],
    description="description",
    variant="var",
)

feature_variants = []
for i in range(1, 51):
    feature_variant = FeatureVariant(
        created=None,
        name="feature_name",
        variant=f"variant_{i}",
        source=("some", "source"),
        value_type="float32",
        entity="entity",
        owner="me",
        provider="provider",
        location=ResourceColumnMapping("", "", ""),
        description="",
        status="PENDING",
        tags=[],
        properties={},
    )
    feature_variants.append(feature_variant)

label_variants = []
for i in range(50):
    label_variant = LabelVariant(
        name="label_name",
        variant=f"{i}",
        source=("some", "source"),
        value_type="float32",
        entity="entity",
        owner="me",
        provider="provider",
        location=ResourceColumnMapping("", "", ""),
        description="",
        status="PENDING",
        tags=[],
        properties={},
    )
    label_variants.append(label_variant)


@pytest.mark.parametrize(
    "initial_status, resource, new_status, new_error, expected_error",
    [
        (
            displaystatus_feature_pending,
            feature_variants[0],
            ResourceStatus.READY,
            None,
            None,
        ),
        (
            displaystatus_label_pending,
            label_variants[0],
            None,
            "NetworkError",
            None,
        ),
        (
            displaystatus_feature_pending,
            feature_variants[0],
            None,
            None,
            f"No updates provided to status for feature_name",
        ),
        (
            displaystatus_label_pending,
            None,
            None,
            "NetworkError",
            "Resource cannot be empty",
        ),
    ],
)
def test_update_status(initial_status, resource, new_status, new_error, expected_error):
    if not resource:
        with pytest.raises(ValueError, match=expected_error):
            ResourceTableRow(resource, initial_status)
        return
    row = ResourceTableRow(resource, initial_status)
    assert row.get_status() == initial_status
    assert row.name == resource.name
    assert row.get_time_ticks() == 0
    assert row.resource == resource

    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            row.update_status(new_status=new_status, error=new_error)
    else:
        row.update_status(new_status=new_status, error=new_error)
        updated_status = row.get_status()
        if new_status:
            assert updated_status.status == new_status
        if new_error:
            assert updated_status.error == new_error


def test_update_display_data():
    status_displayer = StatusDisplayer(None, feature_variants)
    expected_success_list = []
    expected_failed_list = []
    status_displayer.setup_display_resources()

    def update_resource_status(resource_table_row):
        random_num = random.randint(0, 2)
        if random_num == 0:
            resource_table_row.update_status(ResourceStatus.READY, None)
            if resource_table_row not in expected_success_list:
                expected_success_list.append(resource_table_row)
        elif random_num == 1:
            resource_table_row.update_status(ResourceStatus.FAILED, "NetworkError")
            if resource_table_row not in expected_failed_list:
                expected_failed_list.append(resource_table_row)
        else:
            resource_table_row.update_status(ResourceStatus.PENDING)

    status_displayer.update_resource_status = MagicMock(
        side_effect=update_resource_status
    )
    for _ in range(10):
        status_displayer.update_display_data()
        assert set(status_displayer.success_list) == set(expected_success_list)
        assert set(status_displayer.failed_list) == set(expected_failed_list)


def test_display_table():
    status_displayer = StatusDisplayer(None, label_variants)
    len_all_resources = len(label_variants)
    expected_display_table = status_displayer.unprocessed_resources[:NUM_DISPLAY_ROWS]

    def mock_update_all_resource_statuses():
        return

    status_displayer.update_all_resource_statuses = MagicMock(
        side_effect=mock_update_all_resource_statuses
    )
    status_displayer.setup_display_resources()
    len_all_resources = len_all_resources - NUM_DISPLAY_ROWS
    assert set(status_displayer.display_table_list) == set(expected_display_table)
    assert len(status_displayer.unprocessed_resources) == len_all_resources

    def mock_update_display_status(resource_table_row: ResourceTableRow):
        pass

    status_displayer.update_resource_status = MagicMock(
        side_effect=mock_update_display_status
    )

    batch = 5
    for index, resource_table_row in enumerate(status_displayer.display_table_list):
        if index >= batch:
            break
        resource_table_row.update_status(ResourceStatus.READY, None)
        len_all_resources = len_all_resources - 1

    status_displayer._process_existing_display_rows()
    assert len(status_displayer.display_table_list) == NUM_DISPLAY_ROWS
    status_displayer._process_existing_display_rows()
    assert len(status_displayer.display_table_list) == NUM_DISPLAY_ROWS - batch
    status_displayer._fill_display_rows_from_unprocessed_list()
    assert len(status_displayer.display_table_list) == NUM_DISPLAY_ROWS
    assert len(status_displayer.unprocessed_resources) == len_all_resources

    batch = 15
    for index, resource_table_row in enumerate(status_displayer.display_table_list):
        if index >= batch:
            break
        resource_table_row.update_status(ResourceStatus.READY, None)

    for index, resource_table_row in enumerate(status_displayer.unprocessed_resources):
        if index >= batch:
            break
        resource_table_row.update_status(ResourceStatus.FAILED, "NetworkError")

    status_displayer._process_existing_display_rows()
    assert len(status_displayer.display_table_list) == NUM_DISPLAY_ROWS
    status_displayer._process_existing_display_rows()
    assert len(status_displayer.display_table_list) == NUM_DISPLAY_ROWS - batch
    status_displayer._fill_display_rows_from_unprocessed_list()
    if (len_all_resources - batch) < batch:
        assert len(
            status_displayer.display_table_list
        ) == NUM_DISPLAY_ROWS + len_all_resources - (2 * batch)
    else:
        assert len(status_displayer.display_table_list) == NUM_DISPLAY_ROWS
    len_all_resources = len_all_resources - (2 * batch)
    assert len(status_displayer.unprocessed_resources) == max(len_all_resources, 0)
