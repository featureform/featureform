import os
import sys
import uuid
from argparse import Namespace
sys.path.insert(0, 'provider/scripts/k8s')

import pandas
import pytest
from dotenv import load_dotenv

from offline_store_pandas_runner import K8S_MODE, LOCAL, AZURE
from offline_store_pandas_runner import main, get_etcd_host, get_args, execute_df_job, execute_sql_job, get_blob_credentials, download_blobs_to_local, upload_blob_to_blob_store

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
    blob_credentials = get_blob_credentials(args) 
    output_file = execute_sql_job(args.mode, args.output_uri, args.transformation, args.sources, blob_credentials)

    expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)

    assert len(expected_df) == len(output_df)

    set_environment_variables(env, delete=True)


@pytest.mark.parametrize(
    "variables,expected_output",
    [
        ("local_df_variables_success", f"{dir_path}/test_files/inputs/transaction"),
        # ("k8s_df_variables_single_port_success", f"{dir_path}/test_files/inputs/transaction"),
    ]
)
def test_execute_df_job(df_transformation, variables, expected_output, request):
    env = request.getfixturevalue(variables)
    set_environment_variables(env)
    args = get_args()

    etcd_creds = None
    if args.mode == K8S_MODE:
        etcd_creds = {"host": args.etcd_host, "ports": args.etcd_ports, "username": args.etcd_user, "password": args.etcd_password}
    blob_credentials = get_blob_credentials(args)

    output_file = execute_df_job(args.mode, args.output_uri, df_transformation, args.sources, etcd_creds, blob_credentials)

    expected_df = pandas.read_parquet(expected_output)
    output_df = pandas.read_parquet(output_file)

    set_environment_variables(env, delete=True)
    assert len(expected_df) == len(output_df)


@pytest.mark.parametrize(
    "host,ports,expected_output",
    [   
        ("127.0.0.1", ["2379"], (("127.0.0.1", 2379),)),
        ("127.0.0.1", ["2379", "2380"], (("127.0.0.1", 2379), ("127.0.0.1", 2380))),
    ]
)
def test_get_etcd_host(host, ports, expected_output):
    etcd_host = get_etcd_host(host, ports)
    assert etcd_host == expected_output


@pytest.mark.parametrize(
    "variables",
    [   
        "local_variables_success",
        pytest.param("local_variables_failure", marks=pytest.mark.xfail),
        "k8s_sql_variables_success",
        "k8s_df_variables_success",
        pytest.param("k8s_variables_failure", marks=pytest.mark.xfail),
        pytest.param("k8s_variables_port_not_provided_failure", marks=pytest.mark.xfail),
    ]
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
    ]
)
def test_get_blob_credentials(variables, type, request):
    environment_variables = request.getfixturevalue(variables)
    set_environment_variables(environment_variables)
    args = get_args()
    credentials = get_blob_credentials(args)

    if type == AZURE:
        expected_output = Namespace(type=AZURE, connection_string=args.azure_blob_credentials, container=args.azure_container_name)
    elif type == LOCAL:
        expected_output = Namespace(type=LOCAL)
    

    set_environment_variables(environment_variables, delete=True)
    assert credentials == expected_output


def test_download_blobs_to_local(container_client):
    blob = "featureform/testing/primary/name/variant/transactions_short.csv"
    local_filename = "transactions_short.csv"
    output_file = download_blobs_to_local(container_client, blob, local_filename)

    set_environment_variables({"AZURE_CONNECTION_STRING": "", "AZURE_CONTAINER_NAME":""}, delete=True)
    assert os.path.exists(output_file)


def test_upload_blob_to_blob_store(container_client):
    blob = f"featureform/testing/primary/name/variant/transactions_short_{uuid.uuid4()}.csv"
    local_filename = f"{dir_path}/test_files/inputs/transactions_short.csv"
    output_file = upload_blob_to_blob_store(container_client, local_filename, blob)

    blob_list = container_client.list_blobs(name_starts_with=blob)
    for blob in blob_list:
        if output_file == blob.name:
            return
    
    set_environment_variables({"AZURE_CONNECTION_STRING": "", "AZURE_CONTAINER_NAME":""}, delete=True)
    assert False, "blob wasn't uploaded successfully"

def set_environment_variables(variables, delete=False):
    for key, value in variables.items():
        if delete:
            os.environ.pop(key)
        else:
            os.environ[key] = value