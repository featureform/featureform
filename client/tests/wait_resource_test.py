import pytest
import time
import featureform as ff
from featureform.resources import TrainingSet, Feature, Label, Source, ResourceColumnMapping, PrimaryData, SQLTable, Location
from featureform.serving import ServingClient

from datetime import timedelta

def wait_function_success(name, variant, resource_type, timeout=None):
    time.sleep(1)

def wait_function_failure(name, variant, resource_type, timeout=None):
    raise ValueError("Resource took to long to return")

test_resources = {
    "feature": Feature(
        name="test",
        variant="test",
        value_type="int",
        source=("test","test"),
        entity="test",
        owner="test",
        provider="test",
        location=ResourceColumnMapping(entity="",value="",timestamp=""),
        description="test"
    ),
    "label": Label(
        name="test",
        variant="test",
        value_type="int",
        source=("test","test"),
        entity="test",
        owner="test",
        provider="test",
        location=ResourceColumnMapping(entity="",value="",timestamp=""),
        description="test"
    ),
    "training_set": TrainingSet(
        name="test",
        variant="test",
        label=("test","test"),
        owner="test",
        provider="test",
        description="test",
        features=[("test_feature","variant")],
        feature_lags=[],
    ),
    "source": Source(
        name="test",
        definition=PrimaryData(location=SQLTable(name="test_table")),
        variant="test",
        owner="test",
        provider="test",
        description="test"
    ),
}

wait_functions = {
    "success": wait_function_success,
    "failure": wait_function_failure,
}

@pytest.mark.parametrize("resource", [resource for resource in test_resources.values()])
def test_wait_success(resource):
    resource.wait_function = wait_function_success
    copy_resource = resource.wait()
    assert copy_resource == resource


@pytest.mark.parametrize("resource", [resource for resource in test_resources.values()])
def test_wait_failure(resource):
    resource.wait_function = wait_function_failure
    try:
        resource.wait()
    except ValueError as actual_error:
        assert actual_error.args[0] == f'Resource took to long to return'