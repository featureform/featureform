from featureform.enums import ComputationMode
from featureform.proto import metadata_pb2 as pb


def test_computation_from_proto():
    precomputed_proto = pb.ComputationMode.PRECOMPUTED
    client_computed_proto = pb.ComputationMode.CLIENT_COMPUTED

    precomputed = ComputationMode.from_proto(precomputed_proto)
    client_computed = ComputationMode.from_proto(client_computed_proto)

    assert precomputed == ComputationMode.PRECOMPUTED
    assert client_computed == ComputationMode.CLIENT_COMPUTED