import os
import sys
import uuid
from argparse import Namespace

sys.path.insert(0, "provider/scripts/k8s")

import pandas
import pytest
from dotenv import load_dotenv

from offline_store_pandas_runner import K8S_MODE, LOCAL, AZURE, S3, GCS, LOCAL_DATA_PATH
from offline_store_pandas_runner import (
    main,
    get_args,
    get_blob_store,
    execute_df_job,
    execute_sql_job,
    get_blob_credentials,
    check_dill_exception,
)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.mark.parametrize(
    "variables",
    [
        "local_variables_success",
        pytest.param("local_variables_failure", marks=pytest.mark.xfail),
        "local_df_variables_success",
    ],
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
        (
            "local_variables_success",
            f"{dir_path}/test_files/inputs/transactions_short.csv",
        ),
        (
            "local_variables_parquet_success",
            f"{dir_path}/test_files/inputs/transaction_short",
        ),
        pytest.param(
            "df_local_pass_none_code_failure",
            f"{dir_path}/test_files/expected/test_execute_df_job_success",
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_execute_sql_job(variables, expected_output, request):
    env = request.getfixturevalue(variables)
    set_environment_variables(env)
    args = get_args()
    blob_store = get_blob_store(args.blob_credentials)

    output_file = execute_sql_job(
        args.mode,
        args.output_uri,
        args.transformation,
        args.sources,
        blob_store,
    )

    if expected_output.endswith(".csv"):
        expected_df = pandas.read_csv(expected_output)
    else:
        expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)
    pandas.testing.assert_frame_equal(expected_df, output_df)

    set_environment_variables(env, delete=True)


@pytest.mark.parametrize(
    "variables,expected_output",
    [
        (
            "local_df_variables_success",
            f"{dir_path}/test_files/inputs/transactions_short.csv",
        ),
        (
            "local_df_parquet_variables_success",
            f"{dir_path}/test_files/inputs/transaction_short",
        ),
    ],
)
def test_execute_df_job(df_transformation, variables, expected_output, request):
    env = request.getfixturevalue(variables)
    set_environment_variables(env)
    args = get_args()

    blob_store = get_blob_store(args.blob_credentials)

    output_file = execute_df_job(
        args.mode,
        args.output_uri,
        df_transformation,
        args.sources,
        blob_store,
    )

    if expected_output.endswith(".csv"):
        expected_df = pandas.read_csv(expected_output)
    else:
        expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)

    set_environment_variables(env, delete=True)
    assert len(expected_df) == len(output_df)


@pytest.mark.parametrize(
    "variables",
    [
        "local_variables_success",
        pytest.param("local_variables_failure", marks=pytest.mark.xfail),
        "k8s_sql_variables_success",
        "k8s_df_variables_success",
        pytest.param("k8s_variables_failure", marks=pytest.mark.xfail),
        pytest.param(
            "k8s_variables_port_not_provided_failure", marks=pytest.mark.xfail
        ),
    ],
)


def test_get_args(variables, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    _ = get_args()
    set_environment_variables(environment_variables, delete=True)


@pytest.mark.parametrize(
    "variables,type",
    [
        ("local_variables_success", LOCAL),
        ("k8s_df_variables_success", AZURE),
        ("k8s_s3_df_variables_success", S3),
        pytest.param("k8s_df_variables_failure", AZURE, marks=pytest.mark.xfail),
        pytest.param("k8s_s3_df_variables_failure", S3, marks=pytest.mark.xfail),
        pytest.param("k8s_gs_df_variables_success", GCS, marks=pytest.mark.xfail),
    ],
)
def test_get_blob_credentials(variables, type, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    args = get_args()
    credentials = get_blob_credentials(args.mode, args.blob_credentials.type)

    if type == AZURE:
        expected_output = Namespace(
            type=AZURE,
            connection_string=args.blob_credentials.connection_string,
            container=args.blob_credentials.container,
        )
    elif type == LOCAL:
        expected_output = Namespace(type=LOCAL)
    elif type == S3:
        expected_output = Namespace(
            type=S3,
            aws_access_key_id=args.blob_credentials.aws_access_key_id,
            aws_secret_key=args.blob_credentials.aws_secret_key,
            bucket_name=args.blob_credentials.bucket_name,
            bucket_region=args.blob_credentials.bucket_region,
        )

    set_environment_variables(environment_variables, delete=True)
    assert credentials == expected_output


@pytest.mark.skip("Requires actual connection strings")
@pytest.mark.parametrize(
    "variables,",
    [
        "local_variables_success",
        "k8s_df_variables_success",
        "k8s_s3_df_variables_success",
        pytest.param("not_supported_blob_store", marks=pytest.mark.xfail),
    ],
)
def test_blob_stores(variables, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    args = get_args()
    blob_store = get_blob_store(args.blob_credentials)
    set_environment_variables(environment_variables, delete=True)

    assert blob_store != None
    assert blob_store.type == args.blob_credentials.type
    assert blob_store.get_client() != None

    if blob_store.type != LOCAL:
        unique_id = uuid.uuid4()
        source_file = f"{dir_path}/test_files/inputs/transactions_short.csv"
        upload_file = f"{unique_id}/upload_transactions_short.csv"
        download_file = f"{unique_id}_download_transactions_short.csv"

        _ = blob_store.upload(source_file, upload_file)
        _ = blob_store.download(upload_file, download_file)

        assert os.path.isfile(f"{LOCAL_DATA_PATH}/{download_file}")

        source_directory = f"{dir_path}/test_files/inputs/transaction_short"
        upload_directory = f"{unique_id}/upload_transaction_short"
        download_directory = f"{unique_id}_download_transaction_short"

        _ = blob_store.upload(source_directory, upload_directory)
        _ = blob_store.download(upload_directory, download_directory)

        assert os.path.isdir(f"{LOCAL_DATA_PATH}/{download_directory}")


def set_environment_variables(variables, delete=False):
    for key, value in variables.items():
        if delete:
            os.environ.pop(key)
        else:
            os.environ[key] = value


@pytest.fixture(scope="session", autouse=True)
def load_env_file():
    # get the path to .env in root directory
    env_directory = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(real_path))))
    )
    env_file = os.path.join(env_directory, ".env")
    load_dotenv(env_file)


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
def test_check_dill_exception(exception_message, error, request):
    expected_error = request.getfixturevalue(error)
    error = check_dill_exception(exception_message)
    assert str(error) == str(expected_error)
