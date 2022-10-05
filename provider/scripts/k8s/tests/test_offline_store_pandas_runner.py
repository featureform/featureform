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
        "k8s_variables_success",
        pytest.param("k8s_variables_failure", marks=pytest.mark.xfail),
    ]
)
def test_get_args(variables, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    args = get_args()
    set_environment_variables(environment_variables, delete=True)


@pytest.mark.parametrize(
    "variables",
    [   
        "local_variables_success",
        pytest.param("local_variables_failure", marks=pytest.mark.xfail),
    ]
)
def test_main(variables, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    args = get_args()
    main(args)


@pytest.mark.parametrize(
    "variables,expected_output",
    [
        ("local_variables_success", f"{dir_path}/test_files/inputs/transaction"),
        pytest.param("df_local_pass_none_code_failure", f"{dir_path}/test_files/expected/test_execute_df_job_success", marks=pytest.mark.xfail),
    ]
)
def test_execute_sql_job(variables, expected_output, request):
    set_environment_variables(request.getfixturevalue(variables))
    args = get_args()
    output_file = execute_sql_job(args.mode, args.output_uri, args.transformation, args.sources)

    expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)

    assert len(expected_df) == len(output_df)


def set_environment_variables(variables, delete=False):
    for key, value in variables.items():
        if delete:
            os.environ.pop(key)
        else:
            os.environ[key] = value