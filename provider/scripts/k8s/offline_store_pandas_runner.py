import io
import os
import types

from typing import List
from datetime import datetime
from argparse import Namespace

import dill
import etcd
import pandas as pd
from pandasql import sqldf
from azure.storage.blob import BlobServiceClient


LOCAL_MODE = "local"
K8S_MODE = "k8s"

# Blob Store Types
LOCAL = "local"
AZURE = "azure"

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

LOCAL_DATA_PATH = f"{dir_path}/.featureform/data"

def main(args):
    blob_credentials = get_blob_credentials(args)
    if args.transformation_type == "sql":
        print(f"starting execution for SQL Transformation in {args.mode} mode")
        output_location = execute_sql_job(args.mode, args.output_uri, args.transformation, args.sources, blob_credentials)
    elif args.transformation_type == "df":
        print(f"starting execution for DF Transformation in {args.mode} mode") 
        etcd_credentials = {"host": args.etcd_host, "ports": args.etcd_ports, "username": args.etcd_user, "password": args.etcd_password}
        output_location = execute_df_job(args.mode, args.output_uri, args.transformation, args.sources, etcd_credentials, blob_credentials)
   
    return output_location


def execute_sql_job(mode, output_uri, transformation, source_list, blob_credentials):
    """
    Executes the SQL Queries:
    Parameters:
        mode:           string ("local", "k8s")
        output_uri:     string (path to blob store)
        transformation: string (eg. "SELECT * FROM source_0)
        source_list:    List(string) (a list of input sources)
    Return:
        output_uri_with_timestamp: string (output path of blob storage)
    """

    try:
        if blob_credentials.type == AZURE:
            blob_service_client = BlobServiceClient.from_connection_string(blob_credentials.connection_string)
            container_client = blob_service_client.get_container_client(blob_credentials.container)

        for i, source in enumerate(source_list):
            if blob_credentials.type == AZURE:
                # download blob to local & set source to local path
                output_path = download_blobs_to_local(container_client, source, f"source_{i}")
            else:
                output_path = source 
            globals()[f"source_{i}"]= pd.read_csv(output_path)
        
        mysql = lambda q: sqldf(q, globals())
        output_dataframe = mysql(transformation)

        dt = datetime.now()
        output_uri_with_timestamp = f'{output_uri}{dt}'

        if blob_credentials.type == AZURE:
            local_output = f"{LOCAL_DATA_PATH}/output.csv"
            output_dataframe.to_csv(local_output)
            # upload blob to blob store
            output_uri = upload_blob_to_blob_store(container_client, local_output, output_uri_with_timestamp) 
        
        elif blob_credentials.type == LOCAL:
            output_dataframe.to_csv(output_uri_with_timestamp)
    
        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(e)
        raise e


def execute_df_job(mode, output_uri, code, sources, etcd_credentials, blob_credentials):
    """
    Executes the DF transformation:
    Parameters:
        mode:             string ("local", "k8s")
        output_uri:       string (blob store path)
        code:             code (python code)
        sources:          List(string) (a list of input sources)
        etcd_credentials: {"username": "", "password": ""} (used to pull the code
    Return:
        output_uri_with_timestamp: string (output s3 path)
    """
    
    if blob_credentials.type == AZURE:
        blob_service_client = BlobServiceClient.from_connection_string(blob_credentials.connection_string)
        container_client = blob_service_client.get_container_client(blob_credentials.container)

    func_parameters = []
    for i, location in enumerate(sources):
        if blob_credentials.type == AZURE:
            # download blob to local & set source to local path
            output_path = download_blobs_to_local(container_client, location, f"source_{i}")
        else:
            output_path = location
        func_parameters.append(pd.read_csv(output_path))
    
    try:
        code = get_code_from_file(mode, code, etcd_credentials)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = pd.DataFrame(func(*func_parameters))

        dt = datetime.now()
        output_uri_with_timestamp = f"{output_uri}{dt}"

        if blob_credentials.type == AZURE:
            local_output = f"{LOCAL_DATA_PATH}/output.csv"
            output_df.to_csv(local_output)
            # upload blob to blob store
            output_uri = upload_blob_to_blob_store(container_client, local_output, output_uri_with_timestamp) 
        
        elif blob_credentials.type == LOCAL:
            output_df.to_csv(output_uri_with_timestamp)

        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e


def download_blobs_to_local(container_client, blob, local_filename):
    """
    Downloads a blob to local to be used by pandas.

    Parameters:
        client:         ContainerClient (used to interact with Azure container)
        blob:           str (path to blob store)
        local_filename: str (path to local file)
    
    Output:
        full_path:      str (path to local file that will be used to read by pandas)
    """
    
    if not os.path.isdir(LOCAL_DATA_PATH):
        os.makedirs(LOCAL_DATA_PATH)

    blob_client = container_client.get_blob_client(blob)
    
    full_path = f"{LOCAL_DATA_PATH}/{local_filename}"
    with open(full_path, "wb") as my_blob:
        download_stream = blob_client.download_blob()
        my_blob.write(download_stream.readall())
    
    return full_path


def upload_blob_to_blob_store(client, local_filename, blob_path):
    """
    Uploads a local file to azure blob store.

    Parameters:
        client:         ContainerClient (used to interact with Azure container)
        local_filename: str (path to local file)
        blob_path:      str (path to blob store)
    
    Output:
        blob_path:      str (path to blob store)
    """
    
    blob_upload = client.get_blob_client(blob_path)
    with open(local_filename, "rb") as data:
        blob_upload.upload_blob(data, blob_type="BlockBlob")
    return blob_path


def get_code_from_file(mode, file_path, etcd_credentials):
    """
    Reads the code from a pkl file into a python code object.
    Then this object will be used to execute the transformation. 
    
    Parameters:
        mode:             string ("local", "k8s")
        file_path:        string (path to file)
        etcd_credentials: {"host": "", "port": [""], "username": "", "password": ""} (used to pull the code)
    Return:
        code: code object that could be executed
    """
    
    print(f"Retrieving transformation code from '{file_path}' file in {mode} mode.")
    code = None
    if mode == "k8s":
        """
        When executing on kubernetes, we will need to pull the transformation
        from etcd.
        """
        if len(etcd_credentials["ports"]) == 1:
            etcd_port = int(etcd_credentials["ports"][0])
            etcd_client = etcd.Client(host=etcd_credentials["host"], port=etcd_port)
        else:
            etcd_host = get_etcd_host(etcd_credentials["host"], etcd_credentials["ports"])
            etcd_client = etcd.Client(host=etcd_host)

        code_data = etcd_client.read(file_path).value
        code = dill.loads(code_data)
    else:
        with open(file_path, "rb") as f:
            code = dill.load(f)
    
    return code


def get_blob_credentials(args):
    """
    Retrieve credentials from the blob store. 
    Parameters:
        args: Namespace (input arguments that passed via environment variables)

    Output:
        credentials: Namespace(type="", ...) (includes credentials needed for each blob store.) 
    """
    
    if args.azure_blob_credentials:
        return Namespace(
            type=AZURE,
            connection_string=args.azure_blob_credentials,
            container=args.azure_container_name,
        )
    else:
        return Namespace(
            type=LOCAL,
        )


def get_etcd_host(host, ports):
    """
    Converts the host and ports list into the expected for need for etcd client.
    Parameters:
        host: str   ("127.0.0.1")
        port: [str] (["2379"])
    Output:
        etcd_host: [(str, str)] ([("127.0.0.1", "2379")])
    """
    
    etcd_host = []
    for port in ports:
        etcd_host.append((host, int(port)))
    return tuple(etcd_host)


def get_args():
    """
    Gets input arguments from environment variables.

    Parameters:
        None
    
    Output:
        Namespace
    """

    mode = os.getenv("MODE")
    output_uri = os.getenv("OUTPUT_URI")
    sources = os.getenv("SOURCES", "").split(",")
    transformation_type = os.getenv("TRANSFORMATION_TYPE")
    transformation = os.getenv("TRANSFORMATION")
    etcd_host = os.getenv("ETCD_HOST")
    etcd_ports = os.getenv("ETCD_PORT", "").split(",")
    etcd_user = os.getenv("ETCD_USERNAME")
    etcd_password = os.getenv("ETCD_PASSWORD")
    azure_connection_string = os.getenv("AZURE_CONNECTION_STRING")
    azure_container_name = os.getenv("AZURE_CONTAINER_NAME")

    assert mode in (LOCAL_MODE, K8S_MODE), f"the {mode} mode is not supported. supported modes are '{LOCAL_MODE}' and '{K8S_MODE}'."
    assert transformation_type in ("sql", "df"), f"the {transformation_type} transformation type is not supported. supported types are 'sql', and 'df'."
    assert output_uri and sources != [""] and transformation, "the environment variables are not set properly"

    if mode == K8S_MODE and transformation_type == "df":
        assert etcd_host and etcd_ports != [""] and etcd_user and etcd_password, "for k8s mode, df transformations require etcd host, port, and credentials."
    

    args = Namespace(
        mode=mode, 
        transformation_type=transformation_type, 
        transformation=transformation, 
        output_uri=output_uri, 
        sources=sources,
        etcd_host=etcd_host,
        etcd_ports=etcd_ports,
        etcd_user=etcd_user,
        etcd_password=etcd_password,
        azure_blob_credentials=azure_connection_string,
        azure_container_name=azure_container_name,
        )
    return args


if __name__ == "__main__":
    main(get_args())
