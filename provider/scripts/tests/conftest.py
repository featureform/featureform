import os 
from argparse import Namespace

import dill
import pytest
from pyspark.sql import SparkSession


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

@pytest.fixture(scope="module")
def sql_all_arguments():
    input_args = ["sql", "--job_type", "Transformation", "--output_uri", "s3://featureform-testing/fake-path", "--sql_query", "SELECT * FROM source_0", "--source_list", "s3://path", "s3://path"]
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri="s3://featureform-testing/fake-path", sql_query="SELECT * FROM source_0", source_list=["s3://path", "s3://path"])
    return (input_args, expected_args)

@pytest.fixture(scope="module")
def sql_local_all_arguments():
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri=f"{dir_path}/test_files/output/test_transformation", sql_query="SELECT * FROM source_0", source_list=[f"{dir_path}/test_files/input/transaction"])
    return expected_args

@pytest.fixture(scope="module")
def sql_partial_arguments():
    input_args = ["sql", "--job_type", "Transformation", "--output_uri", "s3://featureform-testing/fake-path"]
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri="s3://featureform-testing/fake-path", sql_query=None, source_list=None)
    return (input_args, expected_args)

@pytest.fixture(scope="module")
def sql_invaild_arguments():
    input_args = ["sql", "--job_type", "Transformation", "--hi"]
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri="s3://featureform-testing/fake-path", sql_query="SELECT * FROM source_0", source_list=["s3://path s3://path"], )
    return (input_args, expected_args)

@pytest.fixture(scope="module")
def sql_invalid_local_arguments():
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri="s3://featureform-testing/fake-path", sql_query="SELECT * FROM source_0", source_list=["NONE"])
    return expected_args

@pytest.fixture(scope="module")
def df_all_arguments():
    input_args = ["df", "--output_uri", "s3://featureform-testing/fake-path", "--code", "code", "--source", "s3://featureform/transaction", "s3://featureform/account", "--aws_region", "us-east-1"]
    expected_args = Namespace(transformation_type="df", output_uri="s3://featureform-testing/fake-path", code="code", source=["s3://featureform/transaction", "s3://featureform/account"], aws_region="us-east-1")
    return (input_args, expected_args)

@pytest.fixture(scope="module")
def df_partial_arguments():
    input_args = ["df", "--job_type", "Transformation"]
    expected_args = Namespace(transformation_type="df", output_uri=None)
    return (input_args, expected_args)

@pytest.fixture(scope="module")
def df_invaild_arguments():
    input_args = ["df", "--job_type", "Transformation", "--hi"]
    expected_args = Namespace(transformation_type="df", output_uri="s3://featureform-testing/fake-path")
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def invalid_arguments():
    input_args = ["invalid_arg"]
    expected_args = Namespace()
    return (input_args, expected_args)


@pytest.fixture(scope="module")
def df_local_all_arguments(df_transformation):
    expected_args = Namespace(transformation_type="df", output_uri=f"{dir_path}/test_files/output/test_transformation", code=df_transformation, source=[f"{dir_path}/test_files/input/transaction"], aws_region=None)
    return expected_args


@pytest.fixture(scope="module")
def df_local_pass_none_code_failure():
    expected_args = Namespace(transformation_type="df", output_uri=f"{dir_path}/test_files/output/test_transformation", code="s3://featureform-testing/fake-path/code", source=[f"{dir_path}/test_files/input/transaction"])
    return expected_args


@pytest.fixture(scope="module")
def df_transformation():
    file_path = f"{dir_path}/test_files/transformations/same_df.pkl"

    def transformation(transaction):
        return transaction

    with open(file_path, "wb") as f:
        dill.dump(transformation.__code__, f)
    return file_path


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Testing App").getOrCreate()
