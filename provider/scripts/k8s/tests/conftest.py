import os 

import pytest

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.fixture(scope="module")
def local_variables_success():
    return {
        "MODE": "local", 
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test/", 
        "SOURCES": f"{dir_path}/test_files/inputs/transaction",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }

@pytest.fixture(scope="module")
def local_variables_failure():
    return {}


@pytest.fixture(scope="module")
def k8s_variables_success():
    return {
        "MODE": "k8s", 
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test", 
        "SOURCES": f"{dir_path}/test_files/output/local_test",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }

@pytest.fixture(scope="module")
def k8s_variables_failure():
    return {
        "MODE": "k8s", 
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test", 
        "SOURCES": f"{dir_path}/test_files/output/local_test",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }
