import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping, ResourceStatus
from featureform.serving import ServingClient
from featureform.register import ResourceClient

from datetime import timedelta

from featureform.proto import metadata_pb2_grpc, metadata_pb2

expected_list = [(metadata_pb2.ResourceStatus.PENDING, ff.ResourceStatus.PENDING), (metadata_pb2.ResourceStatus.READY, ff.ResourceStatus.READY), (metadata_pb2.ResourceStatus.CREATED, ff.ResourceStatus.CREATED), (metadata_pb2.ResourceStatus.FAILED, ff.ResourceStatus.FAILED)]

class MockFeatureVariantStub:
        def __init__(self, status):
            self.status = status
        def GetFeatureVariants(self, input):
            return [metadata_pb2.FeatureVariant(
            name="",
            status=metadata_pb2.ResourceStatus(status=self.status, error_message=""))]

class MockTrainingSetVariantStub:
        def __init__(self, status):
            self.status = status
        def GetTrainingSetVariants(self, input):
            return [metadata_pb2.TrainingSetVariant(
            name="",
            label=metadata_pb2.NameVariant(name="name",variant="variant"),
            features=[metadata_pb2.NameVariant(name="name",variant="variant")],
            status=metadata_pb2.ResourceStatus(status=self.status, error_message=""))]
            

class MockLabelVariantStub:
        def __init__(self, status):
            self.status = status
        def GetLabelVariants(self, input):
            return [metadata_pb2.LabelVariant(
            name="",
            status=metadata_pb2.ResourceStatus(status=self.status, error_message=""))]

class MockSourceVariantStub:
        def __init__(self, status):
            self.status = status
        def GetSourceVariants(self, input):
            return [metadata_pb2.SourceVariant(
            name="",
            primaryData=metadata_pb2.PrimaryData(table=metadata_pb2.PrimarySQLTable(name="table")),
            status=metadata_pb2.ResourceStatus(status=self.status, error_message=""))]
          

@pytest.mark.parametrize("input,expected", expected_list)
def test_feature(input, expected):
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    client._stub = MockFeatureVariantStub(input)
    status = client.get_feature("", "").get_status()
    assert status == expected


@pytest.mark.parametrize("input,expected", expected_list)
def test_training_set(input, expected):
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    client._stub = MockTrainingSetVariantStub(input)
    status = client.get_training_set("", "").get_status()
    assert status == expected


@pytest.mark.parametrize("input,expected", expected_list)
def test_label(input, expected):
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    client._stub = MockLabelVariantStub(input)
    status = client.get_label("", "").get_status()
    assert status == expected

@pytest.mark.parametrize("input,expected", expected_list)
def test_source(input, expected):
    client=ResourceClient(host="mock_host", local=False, insecure=True, cert_path="")
    client._stub = MockSourceVariantStub(input)
    status = client.get_source("", "").get_status()
    assert status == expected