import os
import sys
sys.path.insert(0, 'provider/scripts/k8s')

import pandas
import pytest

from offline_store_pandas_runner import main, get_args, execute_df_job, execute_sql_job


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

@pytest.mark.parametrize(
    "variables",
    [   
        "local_variables_success",
        pytest.param("local_variables_failure", marks=pytest.mark.xfail),
        "local_df_variables_success",
    ]
)
def test_main(variables, df_transformation, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    
    args = get_args()
    main(args)

    set_environment_variables(environment_variables, delete=True)


@pytest.mark.parametrize(
    "variables,expected_output",
    [
        ("local_variables_success", f"{dir_path}/test_files/inputs/transaction"),
        pytest.param("df_local_pass_none_code_failure", f"{dir_path}/test_files/expected/test_execute_df_job_success", marks=pytest.mark.xfail),
    ]
)
def test_execute_sql_job(variables, expected_output, request):
    env = request.getfixturevalue(variables)
    set_environment_variables(env)
    args = get_args()
    output_file = execute_sql_job(args.mode, args.output_uri, args.transformation, args.sources)

    expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)

    assert len(expected_df) == len(output_df)

    set_environment_variables(env, delete=True)


@pytest.mark.parametrize(
    "variables,expected_output",
    [
        ("local_df_variables_success", f"{dir_path}/test_files/inputs/transaction"),
    ]
)
def test_execute_df_job(df_transformation, variables, expected_output, request):
    env = request.getfixturevalue(variables)
    set_environment_variables(env)
    args = get_args()

    output_file = execute_df_job(args.mode, args.output_uri, df_transformation, args.sources)

    expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)

    set_environment_variables(env, delete=True)
    assert len(expected_df) == len(output_df)


@pytest.mark.parametrize(
    "variables",
    [   
        "local_variables_success",
        pytest.param("local_variables_failure", marks=pytest.mark.xfail),
        "k8s_variables_success",
        pytest.param("k8s_variables_failure", marks=pytest.mark.xfail),
        pytest.param("k8s_variables_port_not_provided_failure", marks=pytest.mark.xfail),
    ]
)
def test_get_args(variables, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    _ = get_args()
    set_environment_variables(environment_variables, delete=True)


def set_environment_variables(variables, delete=False):
    for key, value in variables.items():
        if delete:
            os.environ.pop(key)
        else:
            os.environ[key] = value