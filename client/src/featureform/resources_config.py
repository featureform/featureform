import json
from abc import abstractmethod, ABC
from typing import Dict, Any

from dataclasses import dataclass
from typeguard import typechecked


class Config(ABC):
    @staticmethod
    @abstractmethod
    def type() -> str:
        pass

    @abstractmethod
    def config(self) -> Dict[str, Any]:
        return self.__dict__

    def serialize(self) -> bytes:
        return bytes(json.dumps(self.config()), "utf-8")


class FileStoreConfig(Config, ABC):
    def store_type(self) -> str:
        return self.type()


@typechecked
@dataclass
class RedisConfig(Config):
    host: str
    port: int
    password: str
    db: int

    def type(self) -> str:
        return "REDIS_ONLINE"

    def config(self):
        return {
            "Addr": f"{self.host}:{self.port}",
            "Password": self.password,
            "DB": self.db,
        }


@typechecked
@dataclass
class GCPCredentials(Config):
    def __init__(
        self,
        project_id: str,
        credentials_path: str,
    ):
        self.project_id = project_id
        self.credentials = json.load(open(credentials_path))

    @staticmethod
    def type():
        return "GCPCredentials"

    def config(self):
        return {
            "ProjectId": self.project_id,
            "JSON": self.credentials,
        }


@typechecked
@dataclass
class GCSFileStoreConfig(FileStoreConfig):
    credentials: GCPCredentials
    bucket_name: str
    bucket_path: str = ""

    def type(self) -> str:
        return "GCS"

    def config(self):
        return {
            "BucketName": self.bucket_name,
            "BucketPath": self.bucket_path,
            "Credentials": self.credentials.config(),
        }


@typechecked
@dataclass
class AzureFileStoreConfig(FileStoreConfig):
    account_name: str
    account_key: str
    container_name: str
    root_path: str

    def type(self) -> str:
        return "AZURE"

    def config(self):
        return {
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "ContainerName": self.container_name,
            "Path": self.root_path,
        }


@typechecked
@dataclass
class AWSCredentials(Config):
    def __init__(
        self,
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
    ):
        empty_strings = aws_access_key_id == "" or aws_secret_access_key == ""
        if empty_strings:
            raise Exception(
                "'AWSCredentials' requires all parameters: 'aws_access_key_id', 'aws_secret_access_key'"
            )

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def type(self):
        return "AWS_CREDENTIALS"

    def config(self):
        return {
            "AWSAccessKeyId": self.aws_access_key_id,
            "AWSSecretKey": self.aws_secret_access_key,
        }


@typechecked
@dataclass
class S3StoreConfig(FileStoreConfig):
    def __init__(
        self,
        bucket_path: str,
        bucket_region: str,
        credentials: AWSCredentials,
        path: str = "",
    ):
        bucket_path_ends_with_slash = len(bucket_path) != 0 and bucket_path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.bucket_path = bucket_path
        self.bucket_region = bucket_region
        self.credentials = credentials
        self.path = path

    def type(self) -> str:
        return "S3"

    def config(self):
        return {
            "Credentials": self.credentials.config(),
            "BucketRegion": self.bucket_region,
            "BucketPath": self.bucket_path,
            "Path": self.path,
        }


@typechecked
@dataclass
class HDFSConfig(FileStoreConfig):
    def __init__(self, host: str, port: str, path: str, username: str):
        bucket_path_ends_with_slash = len(path) != 0 and path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.path = path
        self.host = host
        self.port = port
        self.username = username

    def type(self) -> str:
        return "HDFS"

    def config(self):
        return {
            "Host": self.host,
            "Port": self.port,
            "Path": self.path,
            "Username": self.username,
        }


@typechecked
@dataclass
class FirestoreConfig(Config):
    collection: str

    project_id: str
    credentials_path: str

    def type(self) -> str:
        return "FIRESTORE_ONLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Collection": self.collection,
            "ProjectID": self.project_id,
            "Credentials": json.load(open(self.credentials_path)),
        }


@typechecked
@dataclass
class CassandraConfig(Config):
    keyspace: str
    host: str
    port: str
    username: str
    password: str
    consistency: str
    replication: int

    def type(self) -> str:
        return "CASSANDRA_ONLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Keyspace": self.keyspace,
            "Addr": f"{self.host}:{self.port}",
            "Username": self.username,
            "Password": self.password,
            "Consistency": self.consistency,
            "Replication": self.replication,
        }


@typechecked
@dataclass
class DynamodbConfig(Config):
    region: str
    access_key: str
    secret_key: str

    def type(self) -> str:
        return "DYNAMODB_ONLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Region": self.region,
            "AccessKey": self.access_key,
            "SecretKey": self.secret_key,
        }


@typechecked
@dataclass
class MongoDBConfig(Config):
    username: str
    password: str
    host: str
    port: str
    database: str
    throughput: int

    def type(self) -> str:
        return "MONGODB_ONLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Username": self.username,
            "Password": self.password,
            "Host": self.host,
            "Port": self.port,
            "Database": self.database,
            "Throughput": self.throughput,
        }


@typechecked
@dataclass
class LocalConfig(Config):
    def type(self) -> str:
        return "LOCAL_ONLINE"

    def config(self) -> Dict[str, Any]:
        return {}


@typechecked
@dataclass
class SnowflakeConfig(Config):
    username: str
    password: str
    schema: str
    account: str = ""
    organization: str = ""
    database: str = ""
    account_locator: str = ""
    warehouse: str = ""
    role: str = ""

    def __post_init__(self):
        if self.__has_legacy_credentials() and self.__has_current_credentials():
            raise ValueError(
                "Cannot create configure Snowflake with both current and legacy credentials"
            )

        if not self.__has_legacy_credentials() and not self.__has_current_credentials():
            raise ValueError("Cannot create configure Snowflake without credentials")

    def __has_legacy_credentials(self) -> bool:
        return self.account_locator != ""

    def __has_current_credentials(self) -> bool:
        if (self.account != "" and self.organization == "") or (
            self.account == "" and self.organization != ""
        ):
            raise ValueError("Both Snowflake organization and account must be included")
        elif self.account != "" and self.organization != "":
            return True
        else:
            return False

    def type(self) -> str:
        return "SNOWFLAKE_OFFLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Username": self.username,
            "Password": self.password,
            "Organization": self.organization,
            "AccountLocator": self.account_locator,
            "Account": self.account,
            "Schema": self.schema,
            "Database": self.database,
            "Warehouse": self.warehouse,
            "Role": self.role,
        }


@typechecked
@dataclass
class PostgresConfig(Config):
    host: str
    port: str
    database: str
    user: str
    password: str

    def type(self) -> str:
        return "POSTGRES_OFFLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
        }


@typechecked
@dataclass
class RedshiftConfig(Config):
    host: str
    port: str
    database: str
    user: str
    password: str

    def type(self) -> str:
        return "REDSHIFT_OFFLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
        }


@typechecked
@dataclass
class BigQueryConfig(Config):
    project_id: str
    dataset_id: str
    credentials_path: str

    def type(self) -> str:
        return "BIGQUERY_OFFLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "ProjectID": self.project_id,
            "DatasetID": self.dataset_id,
            "Credentials": json.load(open(self.credentials_path)),
        }


@typechecked
@dataclass
class SparkConfig(Config):
    executor_type: str
    executor_config: dict
    store_type: str
    store_config: dict

    def type(self) -> str:
        return "SPARK_OFFLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "ExecutorType": self.executor_type,
            "StoreType": self.store_type,
            "ExecutorConfig": self.executor_config,
            "StoreConfig": self.store_config,
        }


@typechecked
@dataclass
class K8sConfig(Config):
    store_type: str
    store_config: dict
    docker_image: str = ""

    def type(self) -> str:
        return "K8S_OFFLINE"

    def config(self) -> Dict[str, Any]:
        return {
            "ExecutorType": "K8S",
            "ExecutorConfig": {"docker_image": self.docker_image},
            "StoreType": self.store_type,
            "StoreConfig": self.store_config,
        }


@typechecked
@dataclass
class EmptyConfig(Config):
    def type(self) -> str:
        return ""

    def config(self) -> Dict[str, Any]:
        return {}
