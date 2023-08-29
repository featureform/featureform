from behave import *
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import urllib.request
import pandas as pd


def get_filename_and_uri(filesize, filetype):
    if filesize == "small" and filetype == "csv":
        file_uri = (
            "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv"
        )
        filename = "transactions_short.csv"
    elif filesize == "small" and filetype == "parquet":
        file_uri = (
            "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv"
        )
        filename = "transactions_short.parquet"
    elif filesize == "large" and filetype == "csv":
        file_uri = "https://featureform-demo-files.s3.amazonaws.com/transactions.csv"
        filename = "transactions.csv"
    elif filesize == "large" and filetype == "parquet":
        file_uri = "https://featureform-demo-files.s3.amazonaws.com/transactions.csv"
        filename = "transactions.parquet"
    else:
        raise Exception("Invalid file size or type")
    return filename, file_uri


def download_file(file_uri, local_file_name, filetype):
    if not os.path.exists(local_file_name):
        if filetype == "csv":
            urllib.request.urlretrieve(
                file_uri,
                local_file_name,
            )
        if filetype == "parquet":
            df = pd.read_csv(file_uri)
            df.to_parquet(local_file_name)


def get_file_rows(local_file_name, filetype):
    if filetype == "csv":
        return len(pd.read_csv(local_file_name))
    elif filetype == "parquet":
        return len(pd.read_parquet(local_file_name))


def create_local_path(local_path):
    if not os.path.exists(local_path):
        os.mkdir(local_path)


def upload_to_azure(
    connection_string, container_name, local_file_name, upload_file_path
):
    blob = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=local_file_name,
    )
    if blob.exists():
        return

    print("\nUploading to Azure Storage as blob:\n\t" + upload_file_path)
    # Upload the created file
    with open(file=upload_file_path, mode="rb") as data:
        blob.upload_blob(data)


@when('I upload a "{filesize}" "{filetype}" file to "{storage_provider}"')
def step_impl(context, filesize, filetype, storage_provider):
    filename, file_uri = get_filename_and_uri(filesize, filetype)

    local_path = "data"
    create_local_path(local_path)

    local_file_name = f"{local_path}/{filename}"

    remote_path = "data"

    upload_file_path = os.path.join(remote_path, filename)
    context.filename = upload_file_path

    # Create a file in the local data directory to upload and download
    download_file(file_uri, local_file_name, filetype)

    context.file_length = get_file_rows(local_file_name, filetype)

    if storage_provider == "azure":
        upload_to_azure(
            os.getenv("CONN_STRING"), "test", local_file_name, upload_file_path
        )

    else:
        raise Exception("Invalid storage provider")
