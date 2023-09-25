import os
import sys

sys.path.insert(0, "provider/scripts/spark")

import pytest

from offline_store_spark_runner import (
    main,
    parse_args,
    execute_sql_query,
    execute_df_job,
    set_spark_configs,
    download_blobs_to_local,
    split_key_value,
    get_credentials_dict,
    delete_file,
    dill_exception,
)


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.mark.parametrize(
    "arguments",
    [
        "sql_local_all_arguments",
        "df_local_all_arguments",
        pytest.param("invalid_arguments", marks=pytest.mark.xfail),
        pytest.param("sql_invalid_local_arguments", marks=pytest.mark.xfail),
    ],
)
def test_main(arguments, request):
    expected_args = request.getfixturevalue(arguments)
    main(expected_args)


@pytest.mark.parametrize(
    "arguments",
    [
        "sql_all_arguments",
        "sql_partial_arguments",
        "df_all_arguments",
        "sql_databricks_all_arguments",
        pytest.param("df_partial_arguments", marks=pytest.mark.xfail),
        pytest.param("sql_invalid_arguments", marks=pytest.mark.xfail),
        pytest.param("df_invalid_arguments", marks=pytest.mark.xfail),
        pytest.param("invalid_arguments", marks=pytest.mark.xfail),
    ],
)
def test_parse_args(arguments, request):
    input_args, expected_args = request.getfixturevalue(arguments)
    args = parse_args(input_args)

    assert args == expected_args


@pytest.mark.parametrize(
    "arguments,expected_output",
    [
        ("sql_local_all_arguments", f"{dir_path}/test_files/input/transaction.parquet"),
        pytest.param(
            "sql_invalid_local_arguments",
            f"{dir_path}/test_files/expected/test_execute_sql_job_success",
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_execute_sql_query(arguments, expected_output, spark, request):
    args = request.getfixturevalue(arguments)
    output_file = execute_sql_query(
        args.job_type,
        args.output_uri,
        args.sql_query,
        args.spark_config,
        args.source_list,
    )

    expected_df = spark.read.parquet(expected_output)
    output_df = spark.read.parquet(output_file)

    assert expected_df.count() == output_df.count()
    assert expected_df.schema == output_df.schema


@pytest.mark.parametrize(
    "arguments,expected_output",
    [
        ("df_local_all_arguments", f"{dir_path}/test_files/input/transaction.parquet"),
        pytest.param(
            "df_local_pass_none_code_failure",
            f"{dir_path}/test_files/expected/test_execute_df_job_success",
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_execute_df_job(arguments, expected_output, spark, request):
    args = request.getfixturevalue(arguments)
    output_file = execute_df_job(
        args.output_uri,
        args.code,
        args.store_type,
        args.spark_config,
        args.credential,
        args.source,
    )

    expected_df = spark.read.parquet(expected_output)
    output_df = spark.read.parquet(output_file)

    assert expected_df.count() == output_df.count()
    assert expected_df.schema == output_df.schema


def test_set_spark_config(spark):
    config = {
        "fs.azure.account.key.account_name.dfs.core.windows.net": "adfjaidfasdklciadsj=="
    }

    set_spark_configs(spark, config)

    for key, value in config.items():
        assert spark.conf.get(key) == value


def test_download_blobs_to_local(container_client):
    blob = "scripts/spark/python_packages.sh"
    local_filename = "python_packages.sh"
    output_file = download_blobs_to_local(container_client, blob, local_filename)

    assert os.path.exists(output_file)


def test_get_credentials_dict():
    input_base64_creds = "eyJ0ZXN0aW5nIjogImZpbGUifQ=="
    expected_output = {"testing": "file"}

    creds = get_credentials_dict(input_base64_creds)

    assert creds == expected_output


def test_delete_file(tmp_path):
    file_path = f"{tmp_path}/test.txt"
    print(tmp_path, file_path)
    with open(file_path, "w") as f:
        f.write("hi.world\n")

    assert os.path.isfile(file_path)
    delete_file(file_path)
    assert not os.path.isfile(file_path)
    delete_file(file_path)
    assert not os.path.isfile(file_path)


def test_split_key_value():
    key_values = ["a=b", "b=c", "c=b", "d=e=="]
    expected_output = {"a": "b", "b": "c", "c": "b", "d": "e=="}

    output = split_key_value(key_values)
    assert output == expected_output


@pytest.mark.parametrize(
    "exception_message, error",
    [
        (
            Exception("TypeError: code() takes at most 16 arguments (19 given)"),
            "dill_python_version_error",
        ),
        (Exception("generic error"), "generic_error"),
    ],
)
def test_dill_exception(exception_message, error, request):
    expected_error = request.getfixturevalue(error)
    error = dill_exception(exception_message)
    assert str(error) == str(expected_error)
