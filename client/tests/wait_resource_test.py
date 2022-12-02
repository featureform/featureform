import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from datetime import timedelta

def wait_function_success(timeout=None):
    time.sleep(1)

def wait_function_failure(timeout=None):
    raise ValueError("Resource took to long to return")
    
test_resources = {
    "feature": Feature(),
    "label": Label(),
    "training_set": TrainingSet(),
    "source": Source(),
}

wait_functions = {
    "success": wait_function_success,
    "failure": wait_function_failure,
}

@pytest.mark.parametrize("resource", [resource for resource in test_resources.values()])
def test_wait_success(resource):
    resource.wait_function = wait_function_success
    try:
        resource.wait()
    

@pytest.mark.parametrize("resource", [resource for resource in test_resources.values()])
def test_wait_failure(resource):
    resource.wait_function = wait_function_failure
    try:
        resource.wait()
    except ValueError as actual_error:
        assert actual_error.args[0] == f'Resource took to long to return'


