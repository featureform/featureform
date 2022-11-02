import os
import sys
sys.path.insert(0, 'provider/scripts')

import pytest

from offline_store_spark_runner import main, parse_args, execute_df_job


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

@pytest.mark.parametrize(
    "arguments",
    [   
        "sql_local_all_arguments",
        "df_local_all_arguments", 
        pytest.param("invalid_arguments", marks=pytest.mark.xfail),
        pytest.param("sql_invalid_local_arguments", marks=pytest.mark.xfail),
    ]
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
        pytest.param("df_partial_arguments", marks=pytest.mark.xfail),
        pytest.param("sql_invalid_arguments", marks=pytest.mark.xfail),
        pytest.param("df_invalid_arguments", marks=pytest.mark.xfail),
        pytest.param("invalid_arguments", marks=pytest.mark.xfail),
    ])
def test_parse_args(arguments, request):
    input_args, expected_args = request.getfixturevalue(arguments)
    args = parse_args(input_args)

    assert args == expected_args

@pytest.mark.parametrize(
    "arguments,expected_output",
    [
        ("df_local_all_arguments", f"{dir_path}/test_files/input/transaction"),
        pytest.param("df_local_pass_none_code_failure", f"{dir_path}/test_files/expected/test_execute_df_job_success", marks=pytest.mark.xfail),
    ]
)
def test_execute_df_job(arguments, expected_output, spark, request):
    args = request.getfixturevalue(arguments)
    output_file = execute_df_job(args.output_uri, args.code, args.aws_region, args.source)

    expected_df = spark.read.parquet(expected_output)
    output_df = spark.read.parquet(output_file)

    assert expected_df.count() == output_df.count()
    assert expected_df.schema == output_df.schema
