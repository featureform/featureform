import pytest
from featureform import ServingClient, ResourceClient
import grpc

def test_metadata_connection():
    client = ResourceClient("localhost:7878", False)
    client.register_user("test")
    try:
        client.apply()
    except grpc.RpcError as e:
        assert(e.details() == "connection error: desc = \"transport: Error while dialing dial tcp: lookup "
                              "sandbox-metadata-server: no such host\"")



def test_serving_connection():
    client = ServingClient("localhost:7878", False)
    print(client.features([("f1", "v1")], {"user": "a"}))

