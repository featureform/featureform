import io
import types
import argparse
from typing import List
from datetime import datetime


import dill
import boto3
from pyspark.sql import SparkSession


def main(args):
    if args.transformation_type == "sql": 
        output_location = execute_sql_query(args.job_type, args.output_uri, args.sql_query, args.source_list)
    elif args.transformation_type == "df":
        output_location = execute_df_job(args.output_uri, args.code, args.aws_region, args.source)
    return output_location


def execute_sql_query(job_type, output_uri, sql_query, source_list):
    """
    Executes the SQL Queries:
    Parameters:
        job_type: string ("Transformation", "Materialization", "Training Set")
        output_uri: string (s3 paths)
        sql_query: string (eg. "SELECT * FROM source_0)
        source_list: List(string) (a list of s3 paths)
    Return:
        output_uri_with_timestamp: string (output s3 path)
    """

    try:
        with SparkSession.builder.appName("Execute SQL Query").getOrCreate() as spark:
            if job_type == "Transformation" or job_type == "Materialization" or job_type == "Training Set":
                for i, source in enumerate(source_list):
                    source_df = spark.read.option("header","true").option("recursiveFileLookup", "true").parquet(source) 
                    source_df.createOrReplaceTempView(f'source_{i}')
            output_dataframe = spark.sql(sql_query)

            dt = datetime.now()
            output_uri_with_timestamp = f'{output_uri}{dt}'

            output_dataframe.write.option("header", "true").mode("overwrite").parquet(output_uri_with_timestamp)
            return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(e)
        raise e


def execute_df_job(output_uri, code, aws_region, sources):
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


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest="transformation_type", required=True)
    sql_parser = subparser.add_parser("sql")
    sql_parser.add_argument(
        "--job_type", choices=["Transformation", "Materialization", "Training Set"], help="type of job being run on spark") 
    sql_parser.add_argument(
        '--output_uri', help="output S3 file location; eg. s3://featureform/{type}/{name}/{variant}")
    sql_parser.add_argument(
        '--sql_query', help="The SQL query you would like to run on the data source. eg. SELECT * FROM source_1 INNER JOIN source_2 ON source_1.id = source_2.id")
    sql_parser.add_argument(
        "--source_list", nargs="+", help="list of sources in the transformation string")
    
    df_parser = subparser.add_parser("df")
    df_parser.add_argument(
        '--output_uri', required=True, help="output S3 file location")
    df_parser.add_argument(
        "--code", required=True, help="the path to transformation code file"
    )
    df_parser.add_argument(
        "--source", required=True, nargs='*', help="""Add a number of sources"""
    )
    df_parser.add_argument(
        "--aws_region", help="the aws s3 region were the code file is stored"
    )
    
    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args())
