import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from datetime import timedelta

from .proto import metadata_pb2_grpc, metadata_pb2

def test_pending_training_set_hosted(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetTrainingSetVariants'
        return_value=metadata_pb2.TrainingSetVariant(
            name="",
            status="PENDING"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_training_set("", "").get_status()
    assert status == ff.ResourceStatus.PENDING
    

def test_pending_feature(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetFeatures'
        return_value=metadata_pb2.FeatureVariant(
            name="",
            status="PENDING"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_feature("", "").get_status()
    assert status == ff.ResourceStatus.PENDING

def test_pending_source(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetSources'
        return_value=metadata_pb2.SourceVariant(
            name="",
            status="PENDING"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_source("", "").get_status()
    assert status == ff.ResourceStatus.PENDING

def test_pending_label(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetLabels'
        return_value=metadata_pb2.LabelVariant(
            name="",
            status="PENDING"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_label("", "").get_status()
    assert status == ff.ResourceStatus.PENDING
Hosted Ready
def test_ready_training_set_hosted(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetTrainingSetVariants'
        return_value=metadata_pb2.TrainingSetVariant(
            name="",
            status="READY"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_training_set("", "").get_status()
    assert status == ff.ResourceStatus.READY
    

def test_ready_feature(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetFeatures'
        return_value=metadata_pb2.FeatureVariant(
            name="",
            status="READY"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_feature("", "").get_status()
    assert status == ff.ResourceStatus.READY

def test_ready_source(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetSources'
        return_value=metadata_pb2.SourceVariant(
            name="",
            status="READY"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_source("", "").get_status()
    assert status == ff.ResourceStatus.READY

def test_ready_label(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetLabels'
        return_value=metadata_pb2.LabelVariant(
            name="",
            status="READY"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_label("", "").get_status()
    assert status == ff.ResourceStatus.READY
Hosted Failed
def test_failed_training_set_hosted(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetTrainingSetVariants'
        return_value=metadata_pb2.TrainingSetVariant(
            name="",
            status="FAILED"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_training_set("", "").get_status()
    assert status == ff.ResourceStatus.FAILED
    

def test_failed_feature(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetFeatures'
        return_value=metadata_pb2.FeatureVariant(
            name="",
            status="FAILED"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_feature("", "").get_status()
    assert status == ff.ResourceStatus.FAILED

def test_failed_source(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetSources'
        return_value=metadata_pb2.SourceVariant(
            name="",
            status="FAILED"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_source("", "").get_status()
    assert status == ff.ResourceStatus.FAILED

def test_failed_label(mocker):
    mocker.patch(
        'metadata_pb2_grpc.ApiStub.GetLabels'
        return_value=metadata_pb2.LabelVariant(
            name="",
            status="FAILED"
        )
    )
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    status = client.get_label("", "").get_status()
    assert status == ff.ResourceStatus.FAILED
