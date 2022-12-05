import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping, ResourceStatus
from featureform.serving import ServingClient

from datetime import timedelta

from .proto import metadata_pb2_grpc, metadata_pb2

def resource_with_status(resource, status):
    resource.status = status
    return resource

expected_list = [("PENDING", ff.ResourceStatus.PENDING), ("READY", ff.ResourceStatus.READY), ("CREATED", ff.ResourceStatus.CREATED), ("FAILED", ff.ResourceStatus.FAILED)]

@pytest.mark.paramaterize("input,expected" expected_list)
def test_feature(mocker, input, expected):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetFeatures'
        return_value=metadata_pb2.FeatureVariant(
            name="",
            status=input
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_feature("", "").get_status()
    assert status == expected

@pytest.mark.paramaterize("input,expected" expected_list)
def test_training_set(mocker, input, expected):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetTrainingSets'
        return_value=metadata_pb2.TrainingSetVariant(
            name="",
            status=input
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_feature("", "").get_status()
    assert status == expected


@pytest.mark.paramaterize("input,expected" expected_list)
def test_label(mocker, input, expected):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetLabels'
        return_value=metadata_pb2.LabelVariant(
            name="",
            status=input
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_label("", "").get_status()
    assert status == expected

@pytest.mark.paramaterize("input,expected" expected_list)
def test_source(mocker, input, expected):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetSources'
        return_value=metadata_pb2.SourceVariant(
            name="",
            status=input
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_source("", "").get_status()
    assert status == expected