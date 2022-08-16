import argparse
from datetime import datetime

from pyspark.sql import SparkSession

# Expected format

# job_type: "Transformation", "Materialization", or "Training Set",
# output_uri: s3://featureform/{type}/{name}/{variant}
# sql_query: eg. SELECT * FROM source_1 INNER JOIN source_2 ON source_1.id = source_2.id
# source_list: [s3://featureform/{type}/{name}/{variant}, s3://featureform/{type}/{name}/{variant}...] (implicitly mapped to source_1, source_2, etc)

def execute_sql_query(job_type, output_uri, sql_query, source_list):
    try:
        with SparkSession.builder.appName("Execute SQL Query").getOrCreate() as spark:
            if job_type == "Transformation" or job_type == "Materialization":
                for i, source in enumerate(source_list):
                    source_df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(source)
                    source_df.createOrReplaceTempView(f'source_{i}')

            output_dataframe = spark.sql(sql_query)

            dt = datetime.now()
            output_uri_with_timestamp = f'{output_uri}{dt}'

            output_dataframe.write.option("header", "true").mode("overwrite").parquet(output_uri_with_timestamp)
    except Exception as ident:
        print(ident)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job_type", help="type of job being run on spark") 
    parser.add_argument(
        '--output_uri', help="output S3 file location")
    parser.add_argument(
        '--sql_query', help="The SQL query you would like to run on the data source")
    parser.add_argument(
        "--source_list", nargs="+", help="list of sources in the transformation string")
    args = parser.parse_args()

    execute_sql_query(args.job_type, args.data_source, args.output_uri, args.sql_query, args.source_list)