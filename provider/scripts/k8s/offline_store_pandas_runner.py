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


def main(args):
    if args.transformation_type == "sql": 
        output_location = execute_sql_job(args.mode, args.output_uri, args.transformation, args.sources)
    elif args.transformation_type == "df":
        output_location = execute_df_job(args.mode, args.output_uri, args.transformation, args.sources)
    return output_location


def execute_sql_job(mode, output_uri, transformation, source_list):
    """
    Executes the SQL Queries:
    Parameters:
        output_uri: string (s3 paths)
        transformation: string (eg. "SELECT * FROM source_0)
        source_list: List(string) (a list of s3 paths)
    Return:
        output_uri_with_timestamp: string (output s3 path)
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

        if mode == "k8s":
            # upload blob to blob store
            pass 

        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(e)
        raise e


def execute_df_job(output_uri, code, sources):
    """
    Executes the DF transformation:
    Parameters:
        output_uri: string (s3 paths)
        code: code (python code)
        sources: {parameter: s3_path} (used for passing dataframe parameters)
    Return:
        output_uri_with_timestamp: string (output s3 path)
    """

    spark = SparkSession.builder.appName("Dataframe Transformation").getOrCreate()
    
    func_parameters = []
    for location in sources:
        func_parameters.append(spark.read.option("recursiveFileLookup", "true").parquet(location))
    
    try:
        code = get_code_from_file(code, aws_region)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = func(*func_parameters)

        dt = datetime.now()
        output_uri_with_timestamp = f"{output_uri}{dt}"
        output_df.write.mode("overwrite").parquet(output_uri_with_timestamp)
        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e


def get_code_from_file(file_path, aws_region=None):
    """
    Reads the code from a pkl file into a python code object.
    Then this object will be used to execute the transformation. 
    
    Parameters:
        file_path: string (path to file)
        aws_region: string (aws region where s3 bucket is located)
    Return:
        code: code object that could be executed
    """
    
    prefix_len = len("s3://")
    code = None
    if "s3://" == file_path[:prefix_len]:
        """
        S3 paths are the following path: 's3://{bucket}/key/to/file'.
        the split below separates the bucket name and the key that is 
        used to read the object in the bucket. 
        """
        split_path = file_path[prefix_len:].split("/")
        bucket = split_path[0]
        key = '/'.join(split_path[1:])

        s3_resource = boto3.resource("s3", region_name=aws_region)
        s3_object = s3_resource.Object(bucket, key)

        with io.BytesIO() as f:
            s3_object.download_fileobj(f)

            f.seek(0)
            code = dill.loads(f.read())
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
