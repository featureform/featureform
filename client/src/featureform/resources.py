# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import time
from enum import Enum
from base64 import b64encode
from typeguard import typechecked
from dataclasses import dataclass
from typing import List, Tuple, Union

import grpc
from .sqlite_metadata import SQLiteMetadata
from google.protobuf.duration_pb2 import Duration

from featureform.proto import metadata_pb2 as pb
from dataclasses import dataclass, field
from featureform.format import *

NameVariant = Tuple[str, str]


class ColumnTypes(Enum):
    NIL = ""
    INT = "int"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    STRING = "string"
    BOOL = "bool"
    DATETIME = "datetime"


class ResourceStatus(Enum):
    NO_STATUS = "NO_STATUS"
    CREATED = "CREATED"
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"


@typechecked
@dataclass
class OperationType(Enum):
    GET = 0
    CREATE = 1


@typechecked
@dataclass
class SourceType(Enum):
    PRIMARY_SOURCE = "PRIMARY"
    DF_TRANSFORMATION = "DF"
    SQL_TRANSFORMATION = "SQL"


@typechecked
def valid_name_variant(nvar: NameVariant) -> bool:
    return nvar[0] != "" and nvar[1] != ""


@typechecked
@dataclass
class Schedule:
    name: str
    variant: str
    resource_type: int
    schedule_string: str

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    def type(self) -> str:
        return "schedule"

    def _create(self, stub) -> None:
        serialized = pb.SetScheduleChangeRequest(
            resource=pb.ResourceId(pb.NameVariant(name=self.name, variant=self.variant),
                                   resource_type=self.resource_type), schedule=self.schedule_string)
        stub.RequestScheduleChange(serialized)


@typechecked
@dataclass
class RedisConfig:
    host: str
    port: int
    password: str
    db: int

    def software(self) -> str:
        return "redis"

    def type(self) -> str:
        return "REDIS_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Addr": f"{self.host}:{self.port}",
            "Password": self.password,
            "DB": self.db,
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class AWSCredentials:
    def __init__(self,
                 aws_access_key_id: str = "",
                 aws_secret_access_key: str = "",):
        empty_strings = aws_access_key_id == "" or aws_secret_access_key == ""
        if empty_strings:
            raise Exception("'AWSCredentials' requires all parameters: 'aws_access_key_id', 'aws_secret_access_key'")

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
class AzureFileStoreConfig:
    account_name: str
    account_key: str
    container_name: str
    root_path: str

    def software(self) -> str:
        return "azure"

    def type(self) -> str:
        return "AZURE"

    def serialize(self) -> bytes:
        config = {
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "ContainerName": self.container_name,
            "Path": self.root_path,
        }
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "ContainerName": self.container_name,
            "Path": self.root_path,
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class S3StoreConfig:
    def __init__(self, 
                 bucket_path: str,
                 bucket_region: str,
                 credentials: AWSCredentials):
        bucket_path_ends_with_slash = len(bucket_path) != 0 and bucket_path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")
                 
        self.bucket_path = bucket_path
        self.bucket_region = bucket_region
        self.credentials = credentials

    def software(self) -> str:
        return "S3"

    def type(self) -> str:
        return "S3"

    def serialize(self) -> bytes:
        config = {
            "Credentials": self.credentials.config(),
            "BucketRegion": self.bucket_region,
            "BucketPath": self.bucket_path,
        }
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "Credentials": self.credentials.config(),
            "BucketRegion": self.bucket_region,
            "BucketPath": self.bucket_path,
        }
    
    def store_type(self):
        return self.type()


@typechecked
@dataclass
class OnlineBlobConfig:
    store_type: str
    store_config: dict

    def software(self) -> str:
        return self.store_type

    def type(self) -> str:
        return "BLOB_ONLINE"

    def config(self):
        return self.store_config

    def serialize(self) -> bytes:
        config = {
            "Type": self.store_type,
            "Config": self.store_config,
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class FirestoreConfig:
    collection: str
    project_id: str
    credentials_path: str

    def software(self) -> str:
        return "firestore"

    def type(self) -> str:
        return "FIRESTORE_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Collection": self.collection,
            "ProjectID": self.project_id,
            "Credentials": json.load(open(self.credentials_path)),
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class CassandraConfig:
    keyspace: str
    host: str
    port: str
    username: str
    password: str
    consistency: str
    replication: int

    def software(self) -> str:
        return "cassandra"

    def type(self) -> str:
        return "CASSANDRA_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Keyspace": self.keyspace,
            "Addr": f"{self.host}:{self.port}",
            "Username": self.username,
            "Password": self.password,
            "Consistency": self.consistency,
            "Replication": self.replication
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class DynamodbConfig:
    region: str
    access_key: str
    secret_key: str

    def software(self) -> str:
        return "dynamodb"

    def type(self) -> str:
        return "DYNAMODB_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Region": self.region,
            "AccessKey": self.access_key,
            "SecretKey": self.secret_key
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class MongoDBConfig:
    username: str
    password: str
    host: str
    port: str
    database: str
    throughput: int

    def software(self) -> str:
        return "mongodb"

    def type(self) -> str:
        return "MONGODB_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Username": self.username,
            "Password": self.password,
            "Host": self.host,
            "Port": self.port,
            "Database": self.database,
            "Throughput": self.throughput
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class LocalConfig:

    def software(self) -> str:
        return "localmode"

    def type(self) -> str:
        return "LOCAL_ONLINE"

    def serialize(self) -> bytes:
        config = {
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class SnowflakeConfig:
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
            raise ValueError("Cannot create configure Snowflake with both current and legacy credentials")

        if not self.__has_legacy_credentials() and not self.__has_current_credentials():
            raise ValueError("Cannot create configure Snowflake without credentials")

    def __has_legacy_credentials(self) -> bool:
        return self.account_locator != ""

    def __has_current_credentials(self) -> bool:
        if (self.account != "" and self.organization == "") or (self.account == "" and self.organization != ""):
            raise ValueError("Both Snowflake organization and account must be included")
        elif self.account != "" and self.organization != "":
            return True
        else:
            return False

    def software(self) -> str:
        return "Snowflake"

    def type(self) -> str:
        return "SNOWFLAKE_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Username": self.username,
            "Password": self.password,
            "Organization": self.organization,
            "AccountLocator": self.account_locator,
            "Account": self.account,
            "Schema": self.schema,
            "Database": self.database,
            "Warehouse": self.warehouse,
            "Role": self.role
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class PostgresConfig:
    host: str
    port: str
    database: str
    user: str
    password: str

    def software(self) -> str:
        return "postgres"

    def type(self) -> str:
        return "POSTGRES_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class RedshiftConfig:
    host: str
    port: str
    database: str
    user: str
    password: str

    def software(self) -> str:
        return "redshift"

    def type(self) -> str:
        return "REDSHIFT_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class BigQueryConfig:
    project_id: str
    dataset_id: str
    credentials_path: str

    def software(self) -> str:
        return "bigquery"

    def type(self) -> str:
        return "BIGQUERY_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "ProjectID": self.project_id,
            "DatasetID": self.dataset_id,
            "Credentials": json.load(open(self.credentials_path)),
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class SparkConfig:
    executor_type: str
    executor_config: dict
    store_type: str
    store_config: dict

    def software(self) -> str:
        return "spark"

    def type(self) -> str:
        return "SPARK_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "ExecutorType": self.executor_type,
            "StoreType": self.store_type,
            "ExecutorConfig": self.executor_config,
            "StoreConfig": self.store_config,
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class K8sArgs:
    docker_image: str

    def apply(self, transformation: pb.Transformation):
        transformation.kubernetes_args.docker_image = self.docker_image
        return transformation


@typechecked
@dataclass
class K8sConfig:
    store_type: str
    store_config: dict
    docker_image: str = ""

    def software(self) -> str:
        return "k8s"

    def type(self) -> str:
        return "K8S_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "ExecutorType": "K8S",
            "ExecutorConfig": {
                "docker_image": self.docker_image
            },
            "StoreType": self.store_type,
            "StoreConfig": self.store_config,
        }
        return bytes(json.dumps(config), "utf-8")


Config = Union[
    RedisConfig, SnowflakeConfig, PostgresConfig, RedshiftConfig, LocalConfig, BigQueryConfig,
    FirestoreConfig, SparkConfig, OnlineBlobConfig, AzureFileStoreConfig, S3StoreConfig, K8sConfig,
    MongoDBConfig
]


@typechecked
@dataclass
class Provider:
    name: str
    function: str
    config: Config
    description: str
    team: str

    def __post_init__(self):
        self.software = self.config.software()

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "provider"

    def _create(self, stub) -> None:
        serialized = pb.Provider(
            name=self.name,
            description=self.description,
            type=self.config.type(),
            software=self.config.software(),
            team=self.team,
            serialized_config=self.config.serialize(),
        )
        stub.CreateProvider(serialized)

    def _create_local(self, db) -> None:
        db.insert("providers",
                  self.name,
                  "Provider",
                  self.description,
                  self.config.type(),
                  self.config.software(),
                  self.team,
                  "sources",
                  "ready",
                  str(self.config.serialize(), 'utf-8')
                  )

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
@dataclass
class User:
    name: str

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    def type(self) -> str:
        return "user"

    def _create(self, stub) -> None:
        serialized = pb.User(name=self.name)
        stub.CreateUser(serialized)

    def _create_local(self, db) -> None:
        db.insert("users",
                  self.name,
                  "User",
                  "ready"
                  )

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
@dataclass
class SQLTable:
    name: str


Location = SQLTable


@typechecked
@dataclass
class PrimaryData:
    location: Location

    def kwargs(self):
        return {
            "primaryData":
                pb.PrimaryData(table=pb.PrimarySQLTable(
                    name=self.location.name, ), ),
        }

    def name(self):
        return self.location.name


class Transformation:
    pass


@typechecked
@dataclass
class SQLTransformation(Transformation):
    query: str
    args: K8sArgs = None

    def type(self):
        return SourceType.SQL_TRANSFORMATION.value

    def kwargs(self):
        transformation = pb.Transformation(
            SQLTransformation=pb.SQLTransformation(
                query=self.query,
            )
        )

        if self.args is not None:
            transformation = self.args.apply(transformation)

        return {
            "transformation": transformation
        }


@typechecked
@dataclass
class DFTransformation(Transformation):
    query: bytes
    inputs: list
    args: K8sArgs = None

    def type(self):
        return SourceType.DF_TRANSFORMATION.value

    def kwargs(self):
        transformation = pb.Transformation(
            DFTransformation=pb.DFTransformation(
                query=self.query,
                inputs=[pb.NameVariant(name=v[0], variant=v[1]) for v in self.inputs]
            )
        )
        
        if self.args is not None:
            transformation = self.args.apply(transformation)

        return {
            "transformation": transformation
        }


SourceDefinition = Union[PrimaryData, Transformation]


@typechecked
@dataclass
class Source:
    name: str
    definition: SourceDefinition
    owner: str
    provider: str
    description: str
    variant: str = "default"
    schedule: str = ""
    schedule_obj: Schedule = None
    is_transformation = SourceType.PRIMARY_SOURCE.value
    inputs = [],
    status: str = "NO_STATUS"

    def update_schedule(self, schedule) -> None:
        self.schedule_obj = Schedule(name=self.name, variant=self.variant, resource_type=7, schedule_string=schedule)
        self.schedule = schedule

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "source"

    def _create(self, stub) -> None:
        defArgs = self.definition.kwargs()
        serialized = pb.SourceVariant(
            name=self.name,
            variant=self.variant,
            owner=self.owner,
            description=self.description,
            schedule=self.schedule,
            provider=self.provider,
            **defArgs,
        )
        stub.CreateSourceVariant(serialized)

    def _create_local(self, db) -> None:
        if type(self.definition) == DFTransformation:
            self.is_transformation = SourceType.DF_TRANSFORMATION.value
            self.inputs = self.definition.inputs
            self.definition = self.definition.query
        elif type(self.definition) == SQLTransformation:
            self.is_transformation = SourceType.SQL_TRANSFORMATION.value
            self.definition = self.definition.query
        elif type(self.definition) == PrimaryData:
            self.definition = self.definition.name()
            self.is_transformation = SourceType.PRIMARY_SOURCE.value
        db.insert_source("source_variant",
                         str(time.time()),
                         self.description,
                         self.name,
                         "Source",
                         self.owner,
                         self.provider,
                         self.variant,
                         "ready",
                         self.is_transformation,
                         json.dumps(self.inputs),
                         self.definition
                         )
        self._create_source_resource(db)

    def _create_source_resource(self, db) -> None:
        db.insert(
            "sources",
            "Source",
            self.variant,
            self.name
        )

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
@dataclass
class Entity:
    name: str
    description: str

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "entity"

    def _create(self, stub) -> None:
        serialized = pb.Entity(
            name=self.name,
            description=self.description,
        )
        stub.CreateEntity(serialized)

    def _create_local(self, db) -> None:
        db.insert("entities",
                  self.name,
                  "Entity",
                  self.description,
                  "ready"
                  )

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
@dataclass
class ResourceColumnMapping:
    entity: str
    value: str
    timestamp: str

    def proto(self) -> pb.Columns:
        return pb.Columns(
            entity=self.entity,
            value=self.value,
            ts=self.timestamp,
        )


ResourceLocation = ResourceColumnMapping


@typechecked
@dataclass
class Feature:
    name: str
    source: NameVariant
    value_type: str
    entity: str
    owner: str
    provider: str
    location: ResourceLocation
    description: str
    variant: str = "default"
    schedule: str = ""
    schedule_obj: Schedule = None
    status: str = "NO_STATUS"

    def __post_init__(self):
        col_types = [member.value for member in ColumnTypes]
        if self.value_type not in col_types:
            raise ValueError(f"Invalid feature type ({self.value_type}) must be one of: {col_types}")

    def update_schedule(self, schedule) -> None:
        self.schedule_obj = Schedule(name=self.name, variant=self.variant, resource_type=4, schedule_string=schedule)
        self.schedule = schedule

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "feature"

    def _create(self, stub) -> None:
        serialized = pb.FeatureVariant(
            name=self.name,
            variant=self.variant,
            source=pb.NameVariant(
                name=self.source[0],
                variant=self.source[1],
            ),
            type=self.value_type,
            entity=self.entity,
            owner=self.owner,
            description=self.description,
            schedule=self.schedule,
            provider=self.provider,
            columns=self.location.proto(),
        )
        stub.CreateFeatureVariant(serialized)

    def _create_local(self, db) -> None:
        db.insert("feature_variant",
                  str(time.time()),
                  self.description,
                  self.entity,
                  self.name,
                  self.owner,
                  self.provider,
                  self.value_type,
                  self.variant,
                  "ready",
                  self.location.entity,
                  self.location.timestamp,
                  self.location.value,
                  self.source[0],
                  self.source[1]
                  )
        self._create_feature_resource(db)

    def _create_feature_resource(self, db) -> None:
        db.insert(
            "features",
            self.name,
            self.variant,
            self.value_type
        )

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
@dataclass
class Label:
    name: str
    source: NameVariant
    value_type: str
    entity: str
    owner: str
    provider: str
    description: str
    location: ResourceLocation
    variant: str = "default"
    status: str = "NO_STATUS"

    def __post_init__(self):
        col_types = [member.value for member in ColumnTypes]
        if self.value_type not in col_types:
            raise ValueError(f"Invalid label type ({self.value_type}) must be one of: {col_types}")

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "label"

    def _create(self, stub) -> None:
        serialized = pb.LabelVariant(
            name=self.name,
            variant=self.variant,
            source=pb.NameVariant(
                name=self.source[0],
                variant=self.source[1],
            ),
            type=self.value_type,
            entity=self.entity,
            owner=self.owner,
            description=self.description,
            columns=self.location.proto(),
        )
        stub.CreateLabelVariant(serialized)

    def _create_local(self, db) -> None:
        db.insert("label_variant",
                  str(time.time()),
                  self.description,
                  self.entity,
                  self.name,
                  self.owner,
                  self.provider,
                  self.value_type,
                  self.variant,
                  self.location.entity,
                  self.location.timestamp,
                  self.location.value,
                  "ready",
                  self.source[0],
                  self.source[1]
                  )
        self._create_label_resource(db)

    def _create_label_resource(self, db) -> None:
        db.insert(
            "labels",
            self.value_type,
            self.variant,
            self.name
        )

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
@dataclass
class EntityReference:
    name: str
    obj: Union[Entity, None]

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.GET

    @staticmethod
    def type() -> str:
        return "entity"

    def _get(self, stub):
        entityList = stub.GetEntities(iter([pb.Name(name=self.name)]))
        try:
            for entity in entityList:
                self.obj = entity
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError(f"Entity {self.name} not found.")

    def _get_local(self, db):
        local_entity = db.query_resource("entities", "name", self.name)
        if local_entity == []:
            raise ValueError(f"Entity {self.name} not found.")
        self.obj = local_entity


@typechecked
@dataclass
class ProviderReference:
    name: str
    provider_type: str
    obj: Union[Provider, None]

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.GET

    @staticmethod
    def type() -> str:
        return "provider"

    def _get(self, stub):
        providerList = stub.GetProviders(iter([pb.Name(name=self.name)]))
        try:
            for provider in providerList:
                self.obj = provider
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError(f"Provider {self.name} of type {self.provider_type} not found.")

    def _get_local(self, db):
        local_provider = db.query_resource("providers", "name", self.name)
        if local_provider == []:
            raise ValueError("Local mode provider not found.")
        self.obj = local_provider


@typechecked
@dataclass
class SourceReference:
    name: str
    variant: str
    obj: Union[Source, None]

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.GET

    @staticmethod
    def type() -> str:
        return "source"

    def _get(self, stub):
        sourceList = stub.GetSourceVariants(iter([pb.NameVariant(name=self.name, variant=self.variant)]))
        try:
            for source in sourceList:
                self.obj = source
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError(f"Source {self.name}, variant {self.variant} not found.")

    def _get_local(self, db):
        local_source = db.get_source_variant(self.name, self.variant)
        if local_source == []:
            raise ValueError(f"Source {self.name}, variant {self.variant} not found.")
        self.obj = local_source


@typechecked
@dataclass
class TrainingSet:
    name: str
    owner: str
    label: NameVariant
    features: List[NameVariant]
    description: str
    feature_lags: list = field(default_factory=list)
    status: str = "NO_STATUS"
    variant: str = "default"
    schedule: str = ""
    schedule_obj: Schedule = None
    provider: str = ""

    def update_schedule(self, schedule) -> None:
        self.schedule_obj = Schedule(name=self.name, variant=self.variant, resource_type=6, schedule_string=schedule)
        self.schedule = schedule

    def __post_init__(self):
        if not valid_name_variant(self.label):
            raise ValueError("Label must be set")
        if len(self.features) == 0:
            raise ValueError("A training-set must have atleast one feature")
        for feature in self.features:
            if not valid_name_variant(feature):
                raise ValueError("Invalid Feature")

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "training-set"

    def _create(self, stub) -> None:
        feature_lags = []
        for lag in self.feature_lags:
            lag_duration = Duration()
            _ = lag_duration.FromTimedelta(lag["lag"])
            feature_lag = pb.FeatureLag(
                feature=lag["feature"],
                variant=lag["variant"],
                name=lag["name"],
                lag=lag_duration,
            )
            feature_lags.append(feature_lag)

        serialized = pb.TrainingSetVariant(
            name=self.name,
            variant=self.variant,
            description=self.description,
            schedule=self.schedule,
            owner=self.owner,
            features=[
                pb.NameVariant(name=v[0], variant=v[1]) for v in self.features
            ],
            label=pb.NameVariant(name=self.label[0], variant=self.label[1]),
            feature_lags=feature_lags
        )
        stub.CreateTrainingSetVariant(serialized)

    def _create_local(self, db) -> None:
        self._check_insert_training_set_resources(db)
        db.insert("training_set_variant",
                  str(time.time()),
                  self.description,
                  self.name,
                  self.owner,
                  self.variant,
                  self.label[0],
                  self.label[1],
                  "ready"
                  )
        self._create_training_set_resource(db)

    def _create_training_set_resource(self, db) -> None:
        db.insert(
            "training_sets",
            "TrainingSet",
            self.variant,
            self.name
        )

    def _check_insert_training_set_resources(self, db) -> None:
        try:
            db.get_label_variant(self.label[0], self.label[1])
        except ValueError:
            raise ValueError(f"{self.label[0]} does not exist. Failed to register training set")

        for feature_name, feature_variant in self.features:
            try:
                db.get_feature_variant(feature_name, feature_variant)
            except Exception as e:
                raise Exception(f"{feature_name} does not exist. Failed to register training set. Error: {e}")

            db.insert(
                "training_set_features",
                self.name,
                self.variant,
                feature_name,  # feature name
                feature_variant,  # feature variant
            )

        for feature in self.feature_lags:
            feature_name = feature["feature"]
            feature_variant = feature["variant"]
            feature_new_name = feature["name"]
            feature_lag = feature["lag"].total_seconds()

            try:
                db.get_feature_variant(feature_name, feature_variant)
            except Exception as e:
                raise Exception(f"{feature_name} does not exist. Failed to register training set. Error: {e}")

            db.insert(
                "training_set_lag_features",
                self.name,
                self.variant,
                feature_name,  # feature name
                feature_variant,  # feature variant
                feature_new_name,  # feature new name
                feature_lag  # feature_lag
            )

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


Resource = Union[PrimaryData, Provider, Entity, User, Feature, Label,
                 TrainingSet, Source, Schedule, ProviderReference, SourceReference, EntityReference]


class ResourceRedefinedError(Exception):
    @typechecked
    def __init__(self, resource: Resource):
        variantStr = f" variant {resource.variant}" if hasattr(
            resource, 'variant') else ""
        resourceId = f"{resource.name}{variantStr}"
        super().__init__(
            f"{resource.type()} resource {resourceId} defined in multiple places"
        )


class ResourceState:

    def __init__(self):
        self.__state = {}
        self.__create_list = []

    @typechecked
    def add(self, resource: Resource) -> None:
        if hasattr(resource, 'variant'):
            key = (resource.operation_type().name, resource.type(), resource.name, resource.variant)
        else:
            key = (resource.operation_type().name, resource.type(), resource.name)
        if key in self.__state:
            if resource == self.__state[key]:
                print(f"Resource {resource.type()} already registered.")
                return
            raise ResourceRedefinedError(resource)
        self.__state[key] = resource
        self.__create_list.append(resource)
        if hasattr(resource, 'schedule_obj') and resource.schedule_obj != None:
            my_schedule = resource.schedule_obj
            key = (my_schedule.type(), my_schedule.name)
            self.__state[key] = my_schedule
            self.__create_list.append(my_schedule)

    def sorted_list(self) -> List[Resource]:
        resource_order = {
            "user": 0,
            "provider": 1,
            "source": 2,
            "entity": 3,
            "feature": 4,
            "label": 5,
            "training-set": 6,
            "schedule": 7,
        }

        def to_sort_key(res):
            resource_num = resource_order[res.type()]
            variant = res.variant if hasattr(res, "variant") else ""
            return (resource_num, res.name, variant)

        return sorted(self.__state.values(), key=to_sort_key)

    def create_all_dryrun(self) -> None:
        for resource in self.__create_list:
            if resource.operation_type() is OperationType.GET:
                print("Getting", resource.type(), resource.name)
            if resource.operation_type() is OperationType.CREATE:
                print("Creating", resource.type(), resource.name)

    def create_all_local(self) -> None:
        db = SQLiteMetadata()
        for resource in self.__create_list:
            if resource.operation_type() is OperationType.GET:
                print("Getting", resource.type(), resource.name)
                resource._get_local(db)
            if resource.operation_type() is OperationType.CREATE:
                print("Creating", resource.type(), resource.name)
                resource._create_local(db)
        db.close()
        return

    def create_all(self, stub) -> None:
        for resource in self.__create_list:
            if resource.type() == "provider" and resource.name == "local-mode":
                continue
            try:
                if resource.operation_type() is OperationType.GET:
                    print("Getting", resource.type(), resource.name)
                    resource._get(stub)
                if resource.operation_type() is OperationType.CREATE:
                    print("Creating", resource.type(), resource.name)
                    resource._create(stub)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                    print(resource.name, "already exists.")
                    continue

                raise e


## Executor Providers
@typechecked
class DatabricksCredentials:
    def __init__(self,
                 username: str = "",
                 password: str = "",
                 host: str = "",
                 token: str = "",
                 cluster_id: str = ""):
        self.username = username
        self.password = password
        self.host = host
        self.token = token
        self.cluster_id = cluster_id

        host_token_provided = username == "" and password == "" and host != "" and token != ""
        username_password_provided = username != "" and password != "" and host == "" and token == ""

        if not host_token_provided and not username_password_provided or host_token_provided and username_password_provided:
            raise Exception(
                "The DatabricksCredentials requires only one credentials set ('username' and 'password' or 'host' and 'token' set.)")

        if not cluster_id:
            raise Exception("Cluster_id of existing cluster must be provided")

    def type(self):
        return "DATABRICKS"

    def config(self):
        return {
            "Username": self.username,
            "Password": self.password,
            "Host": self.host,
            "Token": self.token,
            "Cluster": self.cluster_id
        }



@typechecked
@dataclass
class EMRCredentials:
    def __init__(self,
                 emr_cluster_id: str,
                 emr_cluster_region: str,
                 credentials: AWSCredentials):
        
        self.emr_cluster_id = emr_cluster_id
        self.emr_cluster_region = emr_cluster_region
        self.credentials = credentials

    def type(self):
        return "EMR"

    def config(self):
        return {
            "ClusterName": self.emr_cluster_id,
            "ClusterRegion": self.emr_cluster_region,
            "Credentials": self.credentials.config(),
        }


ExecutorCredentials = Union[EMRCredentials, DatabricksCredentials]
