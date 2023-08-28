from behave import *
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import urllib.request


@when("I upload a file to blob store")
def step_impl(context):
    account_url = "https://testingstoragegen.blob.core.windows.net"
    default_credential = AzureCliCredential()

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)
    # Create a local directory to hold blob data
    local_path = "data"
    # os.mkdir(local_path)

    blob = BlobClient.from_connection_string(
        conn_str=os.getenv("CONN_STRING"),
        container_name="test",
        blob_name="data/transactions.csv",
    )
    if blob.exists():
        return

    # Create a file in the local data directory to upload and download
    if not os.path.exists("data/transactions.csv"):
        urllib.request.urlretrieve(
            "https://featureform-demo-files.s3.amazonaws.com/transactions.csv",
            "data/transactions.csv",
        )
    local_file_name = "transactions.csv"
    upload_file_path = os.path.join(local_path, local_file_name)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container="test", blob="transactions.csv"
    )

    print("\nUploading to Azure Storage as blob:\n\t" + upload_file_path)

    # Upload the created file
    with open(file=upload_file_path, mode="rb") as data:
        blob.upload_blob(data)
