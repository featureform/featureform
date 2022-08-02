from featureform import ServingClient, ResourceClient
import grpc
import os

# Tests to make sure client can successfully connect to metadata endpoints
def test_metadata_connection():
    host = os.getenv('API_ADDRESS', "localhost:7878")
    metadata_host = os.getenv('METADATA_HOST')

    try:
        client = ResourceClient(host=host, insecure=True)
        client.register_user("test")
        client.apply()
    # Expect error since metadata server behind api server is not running
    # Checks that the metadata server hostname failed to resolve
    except grpc.RpcError as e:
        assert (metadata_host in e.details())

# Tests to make sure client can successfully connect to serving endpoints
def test_serving_connection():
    host = os.getenv('API_ADDRESS', "localhost:7878")
    serving_host = os.getenv('SERVING_HOST')
    try:
        client = ServingClient(host=host, insecure=True)
        client.features([("f1", "v1")], {"user": "a"})
    # Expect error since feature server behind api server is not running
    # Checks that the feature server hostname failed to resolve
    except grpc.RpcError as e:
        assert (serving_host in e.details())


if __name__ == "__main__":
    test_serving_connection()
