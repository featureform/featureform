import argparse
import base64
from enum import Enum
import io
import json
import os
import sys
import types
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import dill
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
from google.oauth2 import service_account
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.functions import when, col, asc

FILESTORES = ["local", "s3", "azure_blob_store", "google_cloud_storage", "hdfs"]


class OutputFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"


class Headers(str, Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


class JobType(str, Enum):
    TRANSFORMATION = "Transformation"
    MATERIALIZATION = "Materialization"
    TRAINING_SET = "Training Set"
    BATCH_FEATURES = "Batch Features"


if os.getenv("FEATUREFORM_LOCAL_MODE"):
    real_path = os.path.realpath(__file__)
    dir_path = os.path.dirname(real_path)
    LOCAL_DATA_PATH = f"{dir_path}/.featureform/data"
else:
    LOCAL_DATA_PATH = "dbfs:/transformation"


def display_data_metrics(df, spark):
    data = {}
    if "row_number" in df.columns:
        df = df.drop("row_number")
    if "rn2" in df.columns:
        df = df.drop("rn2")

    data_type_dict = dict(df.dtypes)
    columns = []
    rows = []
    stats = []

    rows_as_list = df.collect()
    if len(rows_as_list) > 100:
        rows_as_list = rows_as_list[:100]
    for row in rows_as_list:
        row_list = row.asDict().values()
        rows.append(list(row_list))

    for column_name in data_type_dict:
        columns.append(column_name)
        column_type = data_type_dict[column_name]
        if (
            column_type == "double"
            or column_type == "float"
            or column_type == "int"
            or column_type == "long"
            or column_type == "bigint"
        ):
            bar_chart_stats = numerical_bar_chart(df, column_name)
            stats.append(bar_chart_stats)
            categories_schema = ArrayType(ArrayType(DoubleType()), True)
        elif column_type == "string" or column_type == "timestamp":
            category_stats = unique_categories(df, column_name)
            stats.append(category_stats)
            categories_schema = ArrayType(StringType())
        elif column_type == "boolean":
            boolean_stats = boolean_count(df, column_name)
            stats.append(boolean_stats)
            categories_schema = ArrayType(StringType())

    data["columns"] = columns
    data["rows"] = rows
    data["stats"] = stats

    stats_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("string_categories", ArrayType(StringType(), True), True),
            StructField(
                "numeric_categories", ArrayType(ArrayType(DoubleType()), True), True
            ),
            StructField("categoryCounts", ArrayType(IntegerType()), True),
        ]
    )

    schema = StructType(
        [
            StructField("columns", ArrayType(StringType()), True),
            StructField("rows", ArrayType(ArrayType(StringType(), True)), True),
            StructField("stats", ArrayType(stats_schema, True)),
        ]
    )
    return spark.createDataFrame([(data)], schema=schema)


# Bar graphs (frequency of that number)
def numerical_bar_chart(df, column_name):
    max_value = df.agg({column_name: "max"}).collect()[0][0]
    min_value = df.agg({column_name: "min"}).collect()[0][0]

    range_size = (max_value - min_value) / 10
    range_list = []
    for i in range(1, 11):
        i_range_list = [
            min_value + ((i - 1) * range_size),
            min_value + (i * range_size),
        ]
        range_list.append(i_range_list)

    df = df.withColumn(
        "groups",
        when(
            (col(column_name) >= range_list[0][0])
            & (col(column_name) < range_list[0][1]),
            1,
        ).otherwise(
            when(
                (col(column_name) >= range_list[1][0])
                & (col(column_name) < range_list[1][1]),
                2,
            ).otherwise(
                when(
                    (col(column_name) >= range_list[2][0])
                    & (col(column_name) < range_list[2][1]),
                    3,
                ).otherwise(
                    when(
                        (col(column_name) >= range_list[3][0])
                        & (col(column_name) < range_list[3][1]),
                        4,
                    ).otherwise(
                        when(
                            (col(column_name) >= range_list[4][0])
                            & (col(column_name) < range_list[4][1]),
                            5,
                        ).otherwise(
                            when(
                                (col(column_name) >= range_list[5][0])
                                & (col(column_name) < range_list[5][1]),
                                6,
                            ).otherwise(
                                when(
                                    (col(column_name) >= range_list[6][0])
                                    & (col(column_name) < range_list[6][1]),
                                    7,
                                ).otherwise(
                                    when(
                                        (col(column_name) >= range_list[7][0])
                                        & (col(column_name) < range_list[7][1]),
                                        8,
                                    ).otherwise(
                                        when(
                                            (col(column_name) >= range_list[8][0])
                                            & (col(column_name) < range_list[8][1]),
                                            9,
                                        ).otherwise(
                                            when(
                                                (col(column_name) >= range_list[9][0])
                                                & (col(column_name) < range_list[9][1]),
                                                10,
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ),
    )

    df = df.na.drop(subset=["groups"])
    df = df.groupBy("groups").count()
    df = df.select("groups", "count").toPandas()
    df = df.sort_values("groups")
    value_group_list = list(df["groups"])
    count_list = iter(list(df["count"]))
    categoryCounts = []

    if len(value_group_list) < 10:
        for i in range(1, 11):
            if i not in value_group_list:
                categoryCounts.append(0)
            else:
                categoryCounts.append(next(count_list))
    else:
        categoryCounts = list(df["count"])

    return {
        "name": column_name,
        "type": "numeric",
        "string_categories": [],
        "numeric_categories": range_list,
        "categoryCounts": categoryCounts,
    }


def unique_categories(df, column_name):
    category_count = df.groupBy(column_name).count()
    num_distinct_categories = category_count.count()
    return {
        "name": column_name,
        "type": "string",
        "string_categories": ["unique"],
        "numeric_categories": [],
        "categoryCounts": [num_distinct_categories],
    }


def boolean_count(df, column_name):
    df = df.groupBy(column_name).count()
    df = df.orderBy(asc(column_name))
    df = df.select("count").toPandas()
    false_true_count = list(df["count"])

    return {
        "name": column_name,
        "type": "boolean",
        "string_categories": ["false", "true"],
        "numeric_categories": [],
        "categoryCounts": false_true_count,
    }


def main(args):
    print(f"Starting execution of {args.transformation_type}")
    file_path = set_gcp_credential_file_path(
        args.store_type, args.spark_config, args.credential
    )

    output_location = ""
    try:
        if args.transformation_type == "sql":
            output_location = execute_sql_query(
                args.job_type,
                args.output_uri,
                args.sql_query,
                args.spark_config,
                args.source_list,
                args.output_format,
                args.headers,
            )
        elif args.transformation_type == "df":
            output_location = execute_df_job(
                args.output_uri,
                args.code,
                args.store_type,
                args.spark_config,
                args.credential,
                args.source,
            )

        print(
            f"Finished execution of {args.transformation_type}. Please check {output_location} for output file."
        )
    except Exception as e:
        error_message = f"the {args.transformation_type} job failed. Error: {e}"
        print(error_message)
        raise Exception(error_message)
    finally:
        delete_file(file_path)

    return output_location


def execute_sql_query(
    job_type,
    output_uri,
    sql_query,
    spark_configs,
    source_list,
    output_format,
    headers,
):
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

        if (
            job_type == JobType.TRANSFORMATION
            or job_type == JobType.MATERIALIZATION
            or job_type == JobType.TRAINING_SET
            or job_type == JobType.BATCH_FEATURES
        ):
            for i, source in enumerate(source_list):
                file_extension = Path(source).suffix
                is_directory = file_extension == ""

                if file_extension == ".csv":
                    source_df = (
                        spark.read.option("header", "true")
                        .option("ignoreCorruptFiles", "true")
                        .option("recursiveFileLookup", "true")
                        .csv(source)
                    )
                    source_df.createOrReplaceTempView(f"source_{i}")
                elif file_extension == ".parquet" or is_directory:
                    source_df = (
                        spark.read.option("header", "true")
                        .option("ignoreCorruptFiles", "true")
                        .option("recursiveFileLookup", "true")
                        .parquet(source)
                    )
                    source_df.createOrReplaceTempView(f"source_{i}")
                else:
                    raise Exception(
                        f"the file type for '{source}' file is not supported."
                    )
        else:
            raise Exception(
                f"the '{job_type}' is not supported. Supported types: 'Transformation', 'Materialization', 'Training Set'"
            )

        output_dataframe = spark.sql(sql_query)
        _validate_output_df(output_dataframe)

        dt = datetime.now()
        safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

        # remove the '/' at the end of output_uri in order to avoid double slashes in the output file path.
        output_uri_with_timestamp = f"{output_uri.rstrip('/')}/{safe_datetime}"

        if output_format == OutputFormat.PARQUET:
            if headers == Headers.EXCLUDE:
                raise Exception(
                    f"the output format '{output_format}' does not support excluding headers. Supported types: 'csv'"
                )
            output_dataframe.write.option("header", "true").mode("overwrite").parquet(
                output_uri_with_timestamp
            )
        elif output_format == OutputFormat.CSV:
            if headers == Headers.EXCLUDE:
                output_dataframe.write.mode("overwrite").csv(output_uri_with_timestamp)
            else:
                output_dataframe.write.option("header", "true").mode("overwrite").csv(
                    output_uri_with_timestamp
                )
        else:
            raise Exception(
                f"the output format '{output_format}' is not supported. Supported types: 'parquet', 'csv'"
            )

        if job_type == JobType.MATERIALIZATION:
            try:
                stats_directory = f"{output_uri.rstrip('/')}/stats"
                stats_df = display_data_metrics(output_dataframe, spark)
                stats_df.write.json(stats_directory, mode="overwrite")
            except Exception as e:
                print(e)
                print("Failed to display data metrics")
        else:
            print(f"Skipping data metrics for {job_type} job")
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
        file_extension = Path(location).suffix
        is_directory = file_extension == ""

        if file_extension == ".csv":
            func_parameters.append(
                spark.read.option("header", "true")
                .option("ignoreCorruptFiles", "true")
                .option("recursiveFileLookup", "true")
                .csv(location)
            )
        elif file_extension == ".parquet" or is_directory:
            func_parameters.append(
                spark.read.option("header", "true")
                .option("ignoreCorruptFiles", "true")
                .option("recursiveFileLookup", "true")
                .parquet(location)
            )
        else:
            raise Exception(f"the file type for '{location}' file is not supported.")

    try:
        code = get_code_from_file(code, store_type, credentials)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = func(*func_parameters)
        _validate_output_df(output_df)

        dt = datetime.now()
        safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

        # remove the '/' at the end of output_uri in order to avoid double slashes in the output file path.
        output_uri_with_timestamp = f"{output_uri.rstrip('/')}/{safe_datetime}"

        output_df.write.mode("overwrite").option("header", "true").parquet(
            output_uri_with_timestamp
        )
        try:
            stats_directory = f"{output_uri.rstrip('/')}/stats"
            stats_df = display_data_metrics(output_df, spark)
            stats_df.write.json(stats_directory, mode="overwrite")
        except Exception as e:
            print(e)
            print("Failed to display data metrics")
        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e
    except Exception as e:
        error = check_dill_exception(e)
        raise error


def _validate_output_df(output_df):
    if output_df is None:
        raise Exception("the transformation code returned None.")
    if not isinstance(output_df, DataFrame):
        raise TypeError(
            f"Expected output to be of type 'pyspark.sql.dataframe.DataFrame', "
            f"got '{type(output_df).__name__}' instead.\n"
            f"Please make sure that the transformation code returns a dataframe."
        )


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
    transformation_pkl = None
    if store_type == "s3":
        # S3 paths are the following path: 's3a://{bucket}/key/to/file'.
        # the split below separates the bucket name and the key that is
        # used to read the object in the bucket.

        s3_object = get_s3_object(file_path, credentials)

        with io.BytesIO() as f:
            s3_object.download_fileobj(f)

            f.seek(0)
            transformation_pkl = f.read()

    elif store_type == "hdfs":
        # S3 paths are the following path: 's3://{bucket}/key/to/file'.
        # the split below separates the bucket name and the key that is
        # used to read the object in the bucket.

        import subprocess

        output = subprocess.check_output(f"hdfs dfs -cat {file_path}", shell=True)
        transformation_pkl = bytes(output)

    elif store_type == "azure_blob_store":
        connection_string = credentials.get("azure_connection_string")
        container = credentials.get("azure_container_name")

        if connection_string is None or container is None:
            raise Exception(
                "both 'azure_connection_string' and 'azure_container_name' need to be passed in as credential"
            )

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        container_client = blob_service_client.get_container_client(container)

        transformation_path = download_blobs_to_local(
            container_client, file_path, "transformation.pkl"
        )
        with open(transformation_path, "rb") as f:
            transformation_pkl = f.read()

    elif store_type == "google_cloud_storage":
        transformation_path = "transformation.pkl"
        bucket_name = credentials.get("gcp_bucket_name")
        project_id = credentials.get("gcp_project_id")
        gcp_credentials = get_credentials_dict(credentials.get("gcp_credentials"))

        credentials = service_account.Credentials.from_service_account_info(
            gcp_credentials
        )
        client = storage.Client(project=project_id, credentials=credentials)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.download_to_filename(transformation_path)

        with open(transformation_path, "rb") as f:
            transformation_pkl = f.read()

    else:
        with open(file_path, "rb") as f:
            transformation_pkl = f.read()

    print("Retrieved code.")
    code = dill.loads(transformation_pkl)
    return code


def get_s3_object(file_path, credentials):
    aws_region = credentials.get("aws_region")
    aws_access_key_id = credentials.get("aws_access_key_id")
    aws_secret_access_key = credentials.get("aws_secret_access_key")
    bucket_name = credentials.get("aws_bucket_name")
    if not (aws_region and aws_access_key_id and aws_secret_access_key and bucket_name):
        raise Exception(
            "the values for 'aws_region', 'aws_access_key_id', 'aws_secret_access_key', 'aws_bucket_name' need to be set as credential"
        )

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3_resource = session.resource("s3", region_name=aws_region)
    s3_object = s3_resource.Object(bucket_name, file_path)

    return s3_object


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
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    # TODO: research implications/tradeoffs of setting these to CORRECTED, especially
    # in combination with `outputTimestampType` above.
    # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#configuration
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    for key, value in configs.items():
        spark.conf.set(key, value)


def set_gcp_credential_file_path(store_type, spark_args, creds):
    file_path = f"/tmp/{uuid.uuid4()}.json"
    if store_type == "google_cloud_storage":
        base64_creds = creds.get("gcp_credentials", "")
        creds = get_credentials_dict(base64_creds)

        with open(file_path, "w") as f:
            json.dump(creds, f)

        spark_args["google.cloud.auth.service.account.json.keyfile"] = file_path

    return file_path


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


def delete_file(file_path):
    print(f"deleting {file_path} file")
    if os.path.isfile(file_path):
        os.remove(file_path)
    else:
        print(f"{file_path} does not exist.")


def check_dill_exception(exception):
    error_message = str(exception).lower()
    is_op_code_error = "opcode" in error_message
    is_op_code_expected_arguments_error = (
        "typeerror: code() takes at most" in error_message
    )
    if is_op_code_error or is_op_code_expected_arguments_error:
        version = sys.version_info
        python_version = f"{version.major}.{version.minor}.{version.micro}"
        error_message = f"""This error is most likely caused by different Python versions between the client and Spark provider. Check to see if you are running Python version '{python_version}' on the client."""
        return Exception(error_message)
    return exception


def split_key_value(values):
    arguments = {}
    for value in values:
        # split it into key and value; parse the first value
        value = value.replace('"', "")
        value = value.replace("\\", "")
        key, value = value.split("=", 1)
        # assign into dictionary
        arguments[key] = value
    return arguments


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest="transformation_type", required=True)
    sql_parser = subparser.add_parser("sql")
    sql_parser.add_argument(
        "--job_type",
        choices=[
            JobType.TRANSFORMATION,
            JobType.MATERIALIZATION,
            JobType.TRAINING_SET,
            JobType.BATCH_FEATURES,
        ],
        help="type of job being run on spark",
    )
    sql_parser.add_argument(
        "--output_uri",
        help="output file location; eg. s3a://featureform/{type}/{name}/{variant}",
    )
    sql_parser.add_argument(
        "--sql_query",
        help="The SQL query you would like to run on the data source. eg. SELECT * FROM source_1 INNER JOIN source_2 ON source_1.id = source_2.id",
    )
    sql_parser.add_argument(
        "--source_list",
        nargs="*",  # 0 or more arguments
        help="list of sources in the transformation string",
    )
    sql_parser.add_argument("--store_type", choices=FILESTORES)
    sql_parser.add_argument(
        "--spark_config",
        "-sc",
        action="append",
        default=[],
        help="spark config thats will be set by default",
    )
    sql_parser.add_argument(
        "--credential",
        "-c",
        action="append",
        default=[],
        help="any credentials that would be need to used",
    )
    sql_parser.add_argument(
        "--output_format",
        default=OutputFormat.PARQUET,
        choices=[OutputFormat.PARQUET, OutputFormat.CSV],
        help="output file format",
    )
    sql_parser.add_argument(
        "--headers",
        default=Headers.INCLUDE,
        choices=[Headers.INCLUDE, Headers.EXCLUDE],
        help="include/exclude headers in the output file",
    )
    sql_parser.add_argument(
        "--submit_params_uri",
        help="the path to the submit params file",
    )

    df_parser = subparser.add_parser("df")
    df_parser.add_argument(
        "--output_uri",
        required=True,
        help="output file location; eg. s3a://featureform/{type}/{name}/{variant}",
    )
    df_parser.add_argument(
        "--code", required=True, help="the path to transformation code file"
    )
    df_parser.add_argument(
        "--source", required=True, nargs="*", help="""Add a number of sources"""
    )
    df_parser.add_argument("--store_type", choices=FILESTORES)
    df_parser.add_argument(
        "--spark_config",
        "-sc",
        action="append",
        default=[],
        help="spark config thats will be set by default",
    )
    df_parser.add_argument(
        "--credential",
        "-c",
        action="append",
        default=[],
        help="any credentials that would be need to used",
    )

    arguments = parser.parse_args(args)

    # converts the key=value into a dictionary
    arguments.spark_config = split_key_value(arguments.spark_config)
    arguments.credential = split_key_value(arguments.credential)

    if (
        arguments.transformation_type == "sql"
        and arguments.job_type == JobType.BATCH_FEATURES
        and arguments.submit_params_uri is not None
    ):
        if arguments.store_type == "s3":
            s3_object = get_s3_object(arguments.submit_params_uri, arguments.credential)
            with io.BytesIO() as f:
                s3_object.download_fileobj(f)
                f.seek(0)
                submit_params = json.load(f)
                arguments.sql_query = submit_params["sql_query"]
                arguments.source_list = submit_params["source_list"]
        else:
            raise Exception(
                f"the '{arguments.store_type}' is not supported. Supported types: 's3'"
            )

    return arguments


if __name__ == "__main__":
    main(parse_args())
