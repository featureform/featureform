from argparse import Namespace

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def sql_all_arguments():
    input_args = ["sql", "--job_type", "Transformation", "--output_uri", "s3://featureform-testing/fake-path", "--sql_query", "SELECT * FROM source_0", "--source_list", "s3://path", "s3://path"]
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri="s3://featureform-testing/fake-path", sql_query="SELECT * FROM source_0", source_list=["s3://path", "s3://path"])
    return (input_args, expected_args)

@pytest.fixture(scope="module")
def sql_local_all_arguments():
    expected_args = Namespace(transformation_type="sql", job_type="Transformation", output_uri="tests/test_files/output/test_transformation", sql_query="SELECT * FROM source_0", source_list=["tests/test_files/input/transaction"])
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
    input_args = ["df", "--output_uri", "s3://featureform-testing/fake-path", "--code", "code", "--source", "transaction=s3://featureform/transaction", "account=s3://featureform/account"]
    expected_args = Namespace(transformation_type="df", output_uri="s3://featureform-testing/fake-path", code="code", source={"transaction": "s3://featureform/transaction", "account": "s3://featureform/account"})
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
    expected_args = Namespace(transformation_type="df", output_uri="tests/test_files/output/test_transformation", code=df_transformation, source={"transactions": "tests/test_files/input/transaction"})
    return expected_args


@pytest.fixture(scope="module")
def df_local_pass_none_code_failure():
    expected_args = Namespace(transformation_type="df", output_uri="tests/test_files/output/test_transformation", code=None, source={"transactions": "tests/test_files/input/transaction"})
    return expected_args


@pytest.fixture(scope="module")
def df_transformation():
    def sum_transaction(transactions):
        return transactions.groupBy(["account_id"]).agg({'transaction_amount': 'sum'})
    return sum_transaction.__code__


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Testing App").getOrCreate()
