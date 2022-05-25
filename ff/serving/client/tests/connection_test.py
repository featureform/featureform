from featureform import ServingClient, ResourceClient
import grpc
import os


def test_metadata_connection():
    host = os.getenv('API_ADDRESS', "localhost:7878")
    client = ResourceClient(host, False)
    client.register_user("test")
    try:
        client.apply()
    except grpc.RpcError as e:
        assert (e.details() == "connection error: desc = \"transport: Error while dialing dial tcp: lookup "
                               "sandbox-metadata-server: no such host\"")


def test_serving_connection():
    host = os.getenv('API_ADDRESS', "localhost:7878")
    client = ServingClient(host, False)
    try:
        client.features([("f1", "v1")], {"user": "a"})
    except grpc.RpcError as e:
        assert (e.details() == "connection error: desc = \"transport: Error while dialing dial tcp: lookup "
                               "sandbox-serving-server: no such host\"")
