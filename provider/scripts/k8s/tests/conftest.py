import os 

import dill
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
def local_df_variables_success():
    return {
        "MODE": "local", 
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test/", 
        "SOURCES": f"{dir_path}/test_files/inputs/transaction",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": f"{dir_path}/test_files/transformations/same_df.pkl",
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


@pytest.fixture(scope="module")
def df_transformation():
    file_path = f"{dir_path}/test_files/transformations/same_df.pkl"

    def transformation(transaction):
        return transaction

    with open(file_path, "wb") as f:
        dill.dump(transformation.__code__, f)
    return file_path
