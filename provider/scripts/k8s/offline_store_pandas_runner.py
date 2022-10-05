import io
import os
import types

from typing import List
from datetime import datetime
from argparse import Namespace

import dill
import boto3
import pandas as pd
from pandasql import sqldf


LOCAL_MODE = "local"
K8S_MODE = "k8s"

def main(args):
    if args.transformation_type == "sql": 
        output_location = execute_sql_job(args.mode, args.output_uri, args.transformation, args.sources)
    elif args.transformation_type == "df":
        etcd_credentials = {"username": args.etcd_user, "password": args.etcd_password}
        output_location = execute_df_job(args.mode, args.output_uri, args.transformation, args.sources, etcd_credentials)
    return output_location


def execute_sql_job(mode, output_uri, transformation, source_list):
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
        for i, source in enumerate(source_list):
            if mode == "k8s":
                # download blob to local & set source to local path
                pass
            globals()[f"source_{i}"]= pd.read_parquet(source)
        
        mysql = lambda q: sqldf(q, globals())
        output_dataframe = mysql(transformation)

        dt = datetime.now()
        output_uri_with_timestamp = f'{output_uri}{dt}'

        output_dataframe.to_parquet(output_uri_with_timestamp)

        if mode == K8S_MODE:
            # upload blob to blob store
            pass 

        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(e)
        raise e


def execute_df_job(mode, output_uri, code, sources, etcd_credentials):
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
    
    func_parameters = []
    for location in sources:
        func_parameters.append(pd.read_parquet(location))
    
    try:
        if mode == LOCAL_MODE:
            code = get_code_from_file(mode, code)
        elif mode == K8S_MODE:
            code = get_code_from_file(mode, code, etcd_credentials)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = func(*func_parameters)

        dt = datetime.now()
        output_uri_with_timestamp = f"{output_uri}{dt}"
        output_df.to_parquet(output_uri_with_timestamp)
        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e


def get_code_from_file(mode, file_path, etcd_credentials=None):
    """
    Reads the code from a pkl file into a python code object.
    Then this object will be used to execute the transformation. 
    
    Parameters:
        mode:             string ("local", "k8s")
        file_path:        string (path to file)
        etcd_credentials: {"username": "", "password": ""} (used to pull the code)
    Return:
        code: code object that could be executed
    """
    
    code = None
    if mode == "k8s":
        """
        When executing on kubernetes, we will need to pull the transformation
        from etcd.
        """
        pass
    else:
        with open(file_path, "rb") as f:
            code = dill.load(f)
    
    return code


def get_args():
    mode = os.getenv("MODE")
    output_uri = os.getenv("OUTPUT_URI")
    sources = os.getenv("SOURCES", "").split(",")
    transformation_type = os.getenv("TRANSFORMATION_TYPE")
    transformation = os.getenv("TRANSFORMATION")
    etcd_user = os.getenv("ETCD_USERNAME")
    etcd_password = os.getenv("ETCD_PASSWORD")

    assert mode and output_uri and sources != [""] and transformation_type and transformation, "the environment variables are not set properly"

    if mode == "k8s":
        assert etcd_user and etcd_password, "for k8s mode, etcd credentials are required"
    

    args = Namespace(
        mode=mode, 
        transformation_type=transformation_type, 
        transformation=transformation, 
        output_uri=output_uri, 
        sources=sources,
        etcd_user=etcd_user,
        etcd_password=etcd_password,
        )
    return args


if __name__ == "__main__":
    main(get_args())
