import os
import shutil
import stat
import sys
import pytest

sys.path.insert(0, 'client/src/')
from featureform.register import ResourceClient
from featureform.resources import Feature, Label, TrainingSet, Source, Transformation, ResourceColumnMapping, ResourceStatus
from featureform.proto import metadata_pb2 as pb


def pb_no_status():
    return pb.ResourceStatus.Status.NO_STATUS


def pb_created():
    return pb.ResourceStatus.Status.CREATED


def pb_pending():
    return pb.ResourceStatus.Status.PENDING


def pb_ready():
    return pb.ResourceStatus.Status.READY


def pb_failed():
    return pb.ResourceStatus.Status.FAILED

# Fetches status from proto for same response that client would give
def get_pb_status(status):
    return pb.ResourceStatus.Status._enum_type.values[status].name


@pytest.mark.parametrize("status,expected,ready", [
    (pb_no_status(), ResourceStatus.NO_STATUS, False),
    (pb_created(), ResourceStatus.CREATED, False),
    (pb_pending(), ResourceStatus.PENDING, False),
    (pb_ready(), ResourceStatus.READY, True),
    (pb_failed(), ResourceStatus.FAILED, False),
])
def test_feature_status(mocker, status, expected, ready):
    # Environment variable is getting set somewhere when using Makefile
    # Needs further investigation
    os.environ.pop('FEATUREFORM_CERT', None)
    mocker.patch.object(
        ResourceClient,
        "get_feature",
        return_value=Feature(
            name="name",
            source=("some", "source"),
            value_type="float32",
            entity="entity",
            owner="me",
            provider="provider",
            location=ResourceColumnMapping("", "", ""),
            description="",
            status=get_pb_status(status),
        ))
    client = ResourceClient("host")
    feature = client.get_feature("", "")
    assert feature.get_status() == expected
    assert feature.status == expected.name
    assert feature.is_ready() == ready


@pytest.mark.parametrize("status,expected,ready", [
    (pb_no_status(), ResourceStatus.NO_STATUS, False),
    (pb_created(), ResourceStatus.CREATED, False),
    (pb_pending(), ResourceStatus.PENDING, False),
    (pb_ready(), ResourceStatus.READY, True),
    (pb_failed(), ResourceStatus.FAILED, False),
])
def test_label_status(mocker, status, expected, ready):
    mocker.patch.object(
        ResourceClient,
        "get_label",
        return_value=Label(
            name="name",
            source=("some", "source"),
            value_type="float32",
            entity="entity",
            owner="me",
            provider="provider",
            location=ResourceColumnMapping("", "", ""),
            description="",
            status=get_pb_status(status),
        ))
    client = ResourceClient("host")
    label = client.get_label("", "")
    assert label.get_status() == expected
    assert label.status == expected.name
    assert label.is_ready() == ready


@pytest.mark.parametrize("status,expected,ready", [
    (pb_no_status(), ResourceStatus.NO_STATUS, False),
    (pb_created(), ResourceStatus.CREATED, False),
    (pb_pending(), ResourceStatus.PENDING, False),
    (pb_ready(), ResourceStatus.READY, True),
    (pb_failed(), ResourceStatus.FAILED, False),
])
def test_training_set_status(mocker, status, expected, ready):
    mocker.patch.object(
        ResourceClient,
        "get_training_set",
        return_value=TrainingSet(
            name="",
            owner="",
            label=("something", "something"),
            features=[("some", "feature")],
            feature_lags=[],
            description="",
            status=get_pb_status(status),
        ))
    client = ResourceClient("host")
    ts = client.get_training_set("", "")
    assert ts.get_status() == expected
    assert ts.status == expected.name
    assert ts.is_ready() == ready


@pytest.mark.parametrize("status,expected,ready", [
    (pb_no_status(), ResourceStatus.NO_STATUS, False),
    (pb_created(), ResourceStatus.CREATED, False),
    (pb_pending(), ResourceStatus.PENDING, False),
    (pb_ready(), ResourceStatus.READY, True),
    (pb_failed(), ResourceStatus.FAILED, False),
])
def test_source_status(mocker, status, expected, ready):
    mocker.patch.object(
        ResourceClient,
        "get_source",
        return_value=Source(
            name="",
            definition=Transformation(),
            owner="me",
            provider="provider",
            description="",
            status=get_pb_status(status),
        ))
    client = ResourceClient("host")
    source = client.get_source("", "")
    assert source.get_status() == expected
    assert source.status == expected.name
    assert source.is_ready() == ready
