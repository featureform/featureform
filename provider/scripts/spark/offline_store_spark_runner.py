import io
import os 
import json
import types
import base64
import argparse
from typing import List
from datetime import datetime


import dill
import boto3
from google.cloud import storage
from pyspark.sql import SparkSession
from google.oauth2 import service_account
from azure.storage.blob import BlobServiceClient


FILESTORES = ["local", "s3", "azure_blob_store", "google_cloud_storage", "hdfs"]

if os.getenv("FEATUREFORM_LOCAL_MODE"):
    real_path = os.path.realpath(__file__)
    dir_path = os.path.dirname(real_path)
    LOCAL_DATA_PATH = f"{dir_path}/.featureform/data"
else:
    LOCAL_DATA_PATH = "dbfs:/transformation"

def main(args):
    print(f"Starting execution of {args.transformation_type}")
    
    if args.transformation_type == "sql":
        output_location = execute_sql_query(args.job_type, args.output_uri, args.sql_query, args.spark_config, args.source_list)
    elif args.transformation_type == "df":
        output_location = execute_df_job(args.output_uri, args.code, args.store_type, args.spark_config, args.credential, args.source)
    
    print(f"Finished execution of {args.transformation_type}. Please check {output_location} for output file.")
    return output_location


def execute_sql_query(job_type, output_uri, sql_query, spark_configs, source_list):
    # Executes the SQL Queries:
    # Parameters:
    #     job_type: string ("Transformation", "Materialization", "Training Set")
    #     output_uri: string (s3 paths)
    #     sql_query: string (eg. "SELECT * FROM source_0)
    #     spark_configs: dict (eg. {"fs.azure.account.key.account_name.dfs.core.windows.net": "aksdfkai=="})
    #     source_list: List(string) (a list of s3 paths)
    # Return:
    #     output_uri_with_timestamp: string (output s3 path)

    try:
        spark = SparkSession.builder.appName("Execute SQL Query").getOrCreate()
        set_spark_configs(spark, spark_configs)
        spark.conf.set("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        spark.sparkContext._jsc.hadoopConfiguration().set('my.mapreduce.setting', 'someVal')

        if job_type == "Transformation" or job_type == "Materialization" or job_type == "Training Set":
            for i, source in enumerate(source_list):
                if source.split(".")[-1] == "csv":
                    source_df = spark.read.option("header","true").option("recursiveFileLookup", "true").csv(source) 
                    source_df.createOrReplaceTempView(f'source_{i}')
                else:
                    source_df = spark.read.option("header","true").option("recursiveFileLookup", "true").parquet(source) 
                    source_df.createOrReplaceTempView(f'source_{i}')
        else:
            raise Exception(f"the '{job_type}' is not supported. Supported types: 'Transformation', 'Materialization', 'Training Set'")
        
        output_dataframe = spark.sql(sql_query)

        dt = datetime.now()
        safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")
        output_uri_with_timestamp = f'{output_uri}{safe_datetime}/'

        output_dataframe.write.option("header", "true").mode("overwrite").parquet(output_uri_with_timestamp)
        return output_uri_with_timestamp
    except Exception as e:
        print(e)
        raise e


def execute_df_job(output_uri, code, store_type, spark_configs, credentials, sources):
    # Executes the DF transformation:
    # Parameters:
    #     output_uri: string (s3 paths)
    #     code: code (python code)
    #     sources: {parameter: s3_path} (used for passing dataframe parameters)
    # Return:
    #     output_uri_with_timestamp: string (output s3 path)

    spark = SparkSession.builder.appName("Dataframe Transformation").getOrCreate()
    set_spark_configs(spark, spark_configs)

    print(f"reading {len(sources)} source files")
    func_parameters = []
    for location in sources:
        func_parameters.append(spark.read.option("recursiveFileLookup", "true").parquet(location))
    
    try:
        code = get_code_from_file(code, store_type, credentials)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = func(*func_parameters)

        dt = datetime.now()
        safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")
        output_uri_with_timestamp = f"{output_uri}{safe_datetime}" if output_uri[-1] == "/" else f"{output_uri}/{safe_datetime}"
        output_df.write.mode("overwrite").parquet(output_uri_with_timestamp)
        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e


def get_code_from_file(file_path, store_type=None, credentials=None):
    # Reads the code from a pkl file into a python code object.
    # Then this object will be used to execute the transformation. 
    
    # Parameters:
    #     file_path: string (path to file)
    #     store_type: string ("s3" | "azure_blob_store" | "google_cloud_storage")
    #     credentials: dict 
    # Return:
    #     code: code object that could be executed
    
    print(f"Retrieving transformation code from {file_path} in {store_type}.")

    code = None
    if store_type == "s3":
        # S3 paths are the following path: 's3a://{bucket}/key/to/file'.
        # the split below separates the bucket name and the key that is 
        # used to read the object in the bucket. 

        aws_region = credentials.get("aws_region")
        aws_access_key_id = credentials.get("aws_access_key_id")
        aws_secret_access_key = credentials.get("aws_secret_access_key")
        bucket_name = credentials.get("aws_bucket_name")
        if not (aws_region and aws_access_key_id and aws_secret_access_key and bucket_name):
            raise Exception("the values for 'aws_region', 'aws_access_key_id', 'aws_secret_access_key', 'aws_bucket_name' need to be set as credential")

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3_resource = session.resource("s3", region_name=aws_region)
        s3_object = s3_resource.Object(bucket_name, file_path)

        with io.BytesIO() as f:
            s3_object.download_fileobj(f)

            f.seek(0)
            code = dill.loads(f.read())

    elif store_type == "hdfs":
        # S3 paths are the following path: 's3://{bucket}/key/to/file'.
        # the split below separates the bucket name and the key that is
        # used to read the object in the bucket.

        import subprocess
        output = subprocess.check_output(f"hdfs dfs -cat {file_path}", shell=True)
        code = dill.loads(bytes(output))


    elif store_type == "azure_blob_store":
        connection_string = credentials.get("azure_connection_string")
        container = credentials.get("azure_container_name")

        if connection_string == None or container == None:
            raise Exception("both 'azure_connection_string' and 'azure_container_name' need to be passed in as credential")

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container)

        transformation_path = download_blobs_to_local(container_client, file_path, "transformation.pkl") 
        with open(transformation_path, "rb") as f:
            code = dill.load(f)

    elif store_type == "google_cloud_storage":
        transformation_path = "transformation.pkl"
        bucket_name = credentials.get("gcp_bucket_name")
        project_id = credentials.get("gcp_project_id")
        gcp_credentials = get_credentials_dict(credentials.get("gcp_credentials"))

        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = storage.Client(project=project_id, credentials=credentials)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.download_to_filename(transformation_path)

        with open(transformation_path, "rb") as f:
            code = dill.load(f)

    else:
        with open(file_path, "rb") as f:
            code = dill.load(f)
    
    print("Retrieved code.")
    return code


def download_blobs_to_local(container_client, blob, local_filename):
    # Downloads a blob to local to be used by pandas.

    # Parameters:
    #     client:         ContainerClient (used to interact with Azure container)
    #     blob:           str (path to blob store)
    #     local_filename: str (path to local file)
    
    # Output:
    #     full_path:      str (path to local file that will be used to read by pandas)
    
    if not os.path.isdir(LOCAL_DATA_PATH):
        os.makedirs(LOCAL_DATA_PATH)

    full_path = f"{LOCAL_DATA_PATH}/{local_filename}"
    print(f"downloading {blob} to {full_path}")

    blob_client = container_client.get_blob_client(blob)

    with open(full_path, "wb") as my_blob:
        download_stream = blob_client.download_blob()
        my_blob.write(download_stream.readall())

    return full_path


def set_spark_configs(spark, configs):
    # This method is used to set configs for Spark. It will be mostly
    # used to set access credentials for Spark to the store. 

    print("setting spark configs")
    for key, value in configs.items():
        spark.conf.set(key, value)


def get_credentials_dict(base64_creds):
    # Takes a base64 creds and converts it into 
    # Python dictionary.
    # Input: 
    #   - base64_creds: string
    # Output:
    #   - creds: dict

    base64_bytes = base64_creds.encode("ascii")
    creds_bytes = base64.b64decode(base64_bytes)
    creds_string = creds_bytes.decode("ascii")
    creds = json.loads(creds_string)
    return creds


def split_key_value(values):
    arguments = {}
    for value in values:
        # split it into key and value; parse the first value
        value = value.replace('"', '')
        value = value.replace('\\', '')
        key, value = value.split('=', 1)
        # assign into dictionary
        arguments[key] = value
    return arguments


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest="transformation_type", required=True)
    sql_parser = subparser.add_parser("sql")
    sql_parser.add_argument(
        "--job_type", choices=["Transformation", "Materialization", "Training Set"], help="type of job being run on spark") 
    sql_parser.add_argument(
        '--output_uri', help="output file location; eg. s3a://featureform/{type}/{name}/{variant}")
    sql_parser.add_argument(
        '--sql_query', help="The SQL query you would like to run on the data source. eg. SELECT * FROM source_1 INNER JOIN source_2 ON source_1.id = source_2.id")
    sql_parser.add_argument(
        "--source_list", nargs="+", help="list of sources in the transformation string")
    sql_parser.add_argument("--store_type", choices=FILESTORES)
    sql_parser.add_argument("--spark_config", "-sc", action="append", default=[], help="spark config thats will be set by default")
    sql_parser.add_argument("--credential", "-c", action="append", default=[], help="any credentials that would be need to used")

    df_parser = subparser.add_parser("df")
    df_parser.add_argument(
        '--output_uri', required=True, help="output file location; eg. s3a://featureform/{type}/{name}/{variant}")
    df_parser.add_argument(
        "--code", required=True, help="the path to transformation code file"
    )
    df_parser.add_argument(
        "--source", required=True, nargs='*', help="""Add a number of sources"""
    )
    df_parser.add_argument("--store_type", choices=FILESTORES)
    df_parser.add_argument("--spark_config", "-sc", action="append", default=[], help="spark config thats will be set by default")
    df_parser.add_argument("--credential", "-c", action="append", default=[], help="any credentials that would be need to used")
    
    arguments = parser.parse_args(args)

    # converts the key=value into a dictionary
    arguments.spark_config = split_key_value(arguments.spark_config)
    arguments.credential = split_key_value(arguments.credential)

    return arguments


if __name__ == "__main__":
    main(parse_args())
