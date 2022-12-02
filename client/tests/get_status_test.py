import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping, ResourceStatus
from featureform.serving import ServingClient

from datetime import timedelta

from .proto import metadata_pb2_grpc, metadata_pb2

def resource_with_status(resource, status):
    resource.status = status
    return resource

proto_get_functions = {
    "training_set": {
        "proto_stub": 'metadata_pb2_grpc.ApiStub.GetTrainingSetVariants',
        "proto_resource", metadata_pb2.TrainingSet(name="name", variant="variant", description="description")
        "mock_return": resource_with_status,
        "client_function": ResourceClient(host="mock_host", local=False, insecure=True, cert_path="").get_training_set("",""),
    },
    "feature": {
        "proto_stub": 'metadata_pb2_grpc.ApiStub.GetFeatureVariants',
        "proto_resource", metadata_pb2.Feature(name="name", variant="variant", description="description")
        "mock_return": resource_with_status,
        "client_function": ResourceClient(host="mock_host", local=False, insecure=True, cert_path="").get_feature("",""),
    },
    "source": {
        "proto_stub": 'metadata_pb2_grpc.ApiStub.GetSourceVariants',
        "proto_resource", metadata_pb2.Source(name="name", variant="variant", description="description")
        "mock_return": resource_with_status,
        "client_function": ResourceClient(host="mock_host", local=False, insecure=True, cert_path="").get_source("",""),
    },
    "label": {
        "proto_stub": 'metadata_pb2_grpc.ApiStub.GetLabelVariants',
        "proto_resource", metadata_pb2.Label(name="name", variant="variant", description="description")
        "mock_return": resource_with_status,
        "client_function": ResourceClient(host="mock_host", local=False, insecure=True, cert_path="").get_label("",""),
    }
}

status_map = {
    ResourceStatus.CREATED: metadata_pb2.ResourceStatus(status=metadata_pb2.CREATED),
    ResourceStatus.PENDING: metadata_pb2.ResourceStatus(status=metadata_pb2.PENDING),
    ResourceStatus.READY: metadata_pb2.ResourceStatus(status=metadata_pb2.READY),
    ResourceStatus.FAILED: metadata_pb2.ResourceStatus(status=metadata_pb2.FAILED)
}

@pytest.mark.parametrize("mocker,functions"
    [(mocker, value) for value in proto_get_functions.values()])
@pytest.mark.parametrize("status", [(key, status_map[key]) for key in status_map.key()])
def test_expected_status(mocker, functions, status):
    mocker.patch(
        functions["proto_stub"],
        return_value=[functions["mock_return"](functions["proto_resource"],status[1])]
    )
    assert status[0] == functions["client_function"].get_status()