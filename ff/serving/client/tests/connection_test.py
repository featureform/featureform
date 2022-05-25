import subprocess
from time import sleep

import pytest
from featureform import ServingClient, ResourceClient
import grpc
from subprocess import Popen

def pytest_configure(config):
    def cleanup(proc: Popen):
        proc.kill()
    p = subprocess.Popen(["nohup", "go", "run", "../../api/api.go"])
    sleep(3)
    config.add_cleanup(p.kill())


def test_metadata_connection():
    client = ResourceClient("localhost:7878", False)
    client.register_user("test")
    try:
        client.apply()
    except grpc.RpcError as e:
        assert (e.details() == "connection error: desc = \"transport: Error while dialing dial tcp: lookup "
                               "sandbox-metadata-server: no such host\"")


def test_serving_connection():
    client = ServingClient("localhost:7878", False)
    try:
        client.features([("f1", "v1")], {"user": "a"})
    except grpc.RpcError as e:
        assert (e.details() == "connection error: desc = \"transport: Error while dialing dial tcp: lookup "
                               "sandbox-serving-server: no such host\"")


