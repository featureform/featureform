from ..credentials import AWSCredentials, GCPCredentials

from typeguard import typechecked
from dataclasses import dataclass
from typing import Union
import json


@typechecked
@dataclass
class GCSFileStoreConfig:
    credentials: GCPCredentials
    bucket_name: str
    path: str = ""

    def software(self) -> str:
        return "gcs"

    def type(self) -> str:
        return "GCS"

    def serialize(self) -> bytes:
        config = self.to_json()
        return bytes(json.dumps(config), "utf-8")

    def to_json(self):
        return {
            "BucketName": self.bucket_name,
            "BucketPath": self.path,
            "Credentials": self.credentials.config(),
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class AzureFileStoreConfig:
    account_name: str
    account_key: str
    container_name: str
    path: str

    def software(self) -> str:
        return "azure"

    def type(self) -> str:
        return "AZURE"

    def serialize(self) -> bytes:
        config = self.to_json()
        return bytes(json.dumps(config), "utf-8")

    def to_json(self):
        return {
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "ContainerName": self.container_name,
            "Path": self.path,
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class S3StoreConfig:
    def __init__(
        self,
        bucket_name: str,
        bucket_region: str,
        credentials: AWSCredentials,
        path: str = "",
        bucket_path: str = "",  # Deprecated
    ):
        bucket_path = bucket_name
        bucket_path_ends_with_slash = len(bucket_path) != 0 and bucket_path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.bucket_name = bucket_name
        self.bucket_path = bucket_path
        self.bucket_region = bucket_region
        self.credentials = credentials
        self.path = path

    def software(self) -> str:
        return "S3"

    def type(self) -> str:
        return "S3"

    def serialize(self) -> bytes:
        config = self.to_json()
        return bytes(json.dumps(config), "utf-8")

    def to_json(self):
        return {
            "Credentials": self.credentials.config(),
            "BucketRegion": self.bucket_region,
            "BucketPath": self.bucket_path,
            "Path": self.path,
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class HDFSConfig:
    def __init__(self, host: str, port: str, path: str, username: str):
        bucket_path_ends_with_slash = len(path) != 0 and path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.path = path
        self.host = host
        self.port = port
        self.username = username

    def software(self) -> str:
        return "HDFS"

    def type(self) -> str:
        return "HDFS"

    def serialize(self) -> bytes:
        config = self.to_json()
        return bytes(json.dumps(config), "utf-8")

    def to_json(self):
        return {
            "Host": self.host,
            "Port": self.port,
            "Path": self.path,
            "Username": self.username,
        }

    def store_type(self):
        return self.type()


FilestoreConfig = Union[
    S3StoreConfig, AzureFileStoreConfig, GCSFileStoreConfig, HDFSConfig
]
