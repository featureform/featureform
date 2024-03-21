import csv
import os
import shutil
import stat
import sys
import time
from tempfile import NamedTemporaryFile
from unittest import TestCase
from unittest import mock

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, "client/src/")
from featureform import Client
import serving_cases as cases
import featureform as ff
from featureform.serving import check_feature_type, Row, Dataset
from featureform.enums import ResourceType


class MockStub:
    req = None

    def AddTrigger(self, req):
        req = req

    def RemoveTrigger(self, req):
        req = req

    def UpdateTrigger(self, req):
        req = req

    def DeleteTrigger(self, req):
        req = req

    def CreateTrigger(self, req):
        req = req


class MockRegistrar:
    def __init__(self, *args, **kwargs):
        pass


def test_trigger_operation():
    client = ff.Client(host="localhost:7878", insecure=True)
    client._stub = MockStub()
    trigger = ff.ScheduleTrigger("trigger_name", "1 1 * * *")
    f = ("name", "variant", "FEATURE_VARIANT")

    client.add_trigger(trigger, f)
    client.update_trigger(trigger, "1 * * * *")
    client.remove_trigger(trigger, f)


@pytest.mark.parametrize(
    "trigger_input, expected",
    [
        (ff.ScheduleTrigger("trigger", "1 1 * * *"), "trigger"),
        ("trigger_name", "trigger_name"),
        (ff.Feature(("name", "variant", "other_info"), ff.String), None),
    ],
)
def test_create_trigger_proto(trigger_input, expected):
    client = ff.Client(host="localhost:7878", insecure=True)

    if expected:
        proto = client._create_trigger_proto(trigger_input)
        assert type(proto) == ff.metadata_pb2.Trigger
        assert proto.name == expected
    else:
        with pytest.raises(ValueError) as e:
            client._create_trigger_proto(trigger_input)
        assert (
            str(e.value)
            == f"Invalid trigger type: {type(trigger_input)}. Please use the trigger name or TriggerResource"
        )


@mock.patch("featureform.register.Registrar", mock.MagicMock(side_effect=MockRegistrar))
def test_create_resource_proto_error():
    client = ff.Client(host="localhost:7878", insecure=True)

    class User:
        label_obj = ff.Label(
            (
                MockRegistrar(),
                ("source_name", "source_variant"),
                ["column1", "column2"],
            ),
            variant="variant",
            type=ff.String,
        )

    with pytest.raises(ValueError) as e:
        client._create_resource_proto(User)
    assert (
        str(e.value)
        == f"Invalid resource type: {type(User)}. Resource must be a Feature, Training Set or Source."
    )


@pytest.mark.parametrize(
    "resource_input, expected",
    [
        ("feature_obj", ("feature_obj", ResourceType.FEATURE.value)),
        ("label_obj", ("label_obj", ResourceType.LABEL.value)),
        ("ts_obj", ("ts_obj", ResourceType.TRAINING_SET.value)),
        ("feature", ("feature_obj", ResourceType.FEATURE.value)),
        ("label", ("label_obj", ResourceType.LABEL.value)),
        ("ts", ("ts_obj", ResourceType.TRAINING_SET.value)),
    ],
)
@mock.patch("featureform.register.Registrar", mock.MagicMock(side_effect=MockRegistrar))
def test_create_resource_proto(resource_input, expected):
    client = ff.Client(host="localhost:7878", insecure=True)

    # Arguments
    class User:
        label_obj = ff.Label(
            (
                MockRegistrar(),
                ("source_name", "source_variant"),
                ["column1", "column2"],
            ),
            variant="variant",
            type=ff.String,
        )
        feature_obj = ff.Feature(
            (
                MockRegistrar(),
                ("source_name", "source_variant"),
                ["column1", "column2"],
            ),
            variant="variant",
            type=ff.String,
        )

    User.feature_obj.name = "feature_obj"
    User.label_obj.name = "label_obj"
    ts_obj = ff.register_training_set(
        "ts_obj",
        "variant",
        features=[("feature_obj", "variant")],
        label=("label_obj", "variant"),
    )

    if resource_input == "feature":
        proto = client._create_resource_proto(User.feature_obj)
    elif resource_input == "label":
        proto = client._create_resource_proto(User.label_obj)
    elif resource_input == "ts":
        proto = client._create_resource_proto(ts_obj)
    else:
        proto = client._create_resource_proto((resource_input, "variant", expected[1]))

    # Expected output
    assert type(proto) == ff.metadata_pb2.ResourceID
    assert proto.resource.name == expected[0]
    assert proto.resource.variant == "variant"
    assert proto.resource_type == expected[1]
