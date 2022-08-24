import types
import argparse
from typing import List
from datetime import datetime

from pyspark.sql import SparkSession


def main(args):
    if args.transformation_type == "sql": 
        output_location = execute_sql_query(args.job_type, args.output_uri, args.sql_query, args.source_list)
    elif args.transformation_type == "df":
        output_location = execute_df_job(args.output_uri, args.code, args.source)
    return output_location

def execute_sql_query(job_type, output_uri, sql_query, source_list):
    try:
        with SparkSession.builder.appName("Execute SQL Query").getOrCreate() as spark:
            if job_type == "Transformation" or job_type == "Materialization" or job_type == "Training Set":
                for i, source in enumerate(source_list):          
                    source_df = spark.read.option("header","true").parquet(source)  
                    source_df.createOrReplaceTempView(f'source_{i}')
            output_dataframe = spark.sql(sql_query)

            dt = datetime.now()
            output_uri_with_timestamp = f'{output_uri}{dt}'

            output_dataframe.coalesce(1).write.option("header", "true").mode("overwrite").parquet(output_uri_with_timestamp)
            return output_uri_with_timestamp
    except Exception as e:
        print(e)
        raise e

def execute_df_job(output_uri, code, sources):
    spark = SparkSession.builder.appName("Dataframe Transformation").getOrCreate()
    
    func_parameters = {}
    for name, location in sources.items():
        func_parameters[name] = spark.read.parquet(location)
    
    try:
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = func(**func_parameters)

        dt = datetime.now()
        output_uri_with_timestamp = f"{output_uri}{dt}"
        output_df.coalesce(1).write.mode("overwrite").parquet(output_uri_with_timestamp)
        return output_uri_with_timestamp
    except Exception as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e


class KeyValue(argparse.Action):
    def __call__( self , parser, namespace,
                 values, option_string = None):
        setattr(namespace, self.dest, dict())
          
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value

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
        "--code", required=True, help="the df transformation code"
    )
    df_parser.add_argument(
        "--source", required=True, nargs='*', action=KeyValue, help="""Add a number of source mapping key=value. 
        Do not put spaces before or after the '=' sign."""
    )
    
    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args())
