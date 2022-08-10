import argparse
import os
from datetime import datetime


from pyspark.sql import SparkSession

def execute_sql_query(job_type, data_source, output_uri, sql_query, source_list):
    try:
        with SparkSession.builder.appName("Execute SQL Query").getOrCreate() as spark:

            if job_type == "Primary":
                query = "SELECT * FROM data_source"
                split_source_path = data_source.split(".")
                file_extension = split_source_path[-1]
                if file_extension == "csv":
                    df = spark.read.option("header", "true").csv(data_source)
                elif file_extension == "parquet":
                    df = spark.read.option("header", "true").parquet(data_source)
                else:
                    raise ValueError("source type not implemented")
                df.createOrReplaceTempView("data_source ")
            else:
                source_df_list = []
                for i, source in enumerate(source_list):
                    source_df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(source)
                    source_df.createOrReplaceTempView(f'source_{i}')
                    source_df_list.append(source_df)

            output_dataframe = spark.sql(sql_query)

            dt = datetime.now()
            output_uri_with_timestamp = f'{output_uri}/{dt}'

            # Write the results to the specified output URI
            output_dataframe.coalesce(1).write.option("header", "true").mode("overwrite").parquet(output_uri_with_timestamp)

    except Exception as ident:
        print(ident)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job_type", help="type of job being run on spark")
    parser.add_argument(
        '--data_source', help="input S3 file location")
    parser.add_argument(
        '--output_uri', help="output S3 file location")
    parser.add_argument(
        '--sql_query', help="The SQL query you would like to run on the data source")
    parser.add_argument(
        "--source_list", nargs="+", help="list of sources in the transformation string")
    
    args = parser.parse_args()

    execute_sql_query(args.job_type, args.data_source, args.output_uri, args.sql_query, args.source_list)
			
			