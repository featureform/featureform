from dataclasses import dataclass
from enum import Enum
from featureform.proto import metadata_pb2 as pb
from typeguard import typechecked
from os import path
from fnmatch import fnmatch


class ScalarType(Enum):
    """
    ScalarType is an enum of all the scalar types supported by Featureform.

    Attributes:
        NIL: An empty string representing no specified type.
        INT: A string representing an integer type.
        INT32: A string representing a 32-bit integer type.
        INT64: A string representing a 64-bit integer type.
        FLOAT32: A string representing a 32-bit float type.
        FLOAT64: A string representing a 64-bit float type.
        STRING: A string representing a string type.
        BOOL: A string representing a boolean type.
        DATETIME: A string representing a datetime type.
    """

    NIL = ""
    INT = "int"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    STRING = "string"
    BOOL = "bool"
    DATETIME = "datetime"

    @classmethod
    def has_value(cls, value):
        try:
            cls(value)
            return True
        except ValueError:
            return False

    @classmethod
    def get_values(cls):
        return [e.value for e in cls]


class ResourceStatus(str, Enum):
    """
    ResourceStatus is an enumeration representing the possible states that a
    resource may occupy within an application.

    Each status is represented as a string, which provides a human-readable
    representation for each of the stages in the lifecycle of a resource.

    Attributes:
        NO_STATUS (str): The state of a resource that cannot have another status.
        CREATED (str): The state after a resource has been successfully created.
        PENDING (str): The state indicating that the resource is in the process of being prepared, but is not yet ready.
        READY (str): The state indicating that the resource has been successfully prepared and is now ready for use.
        FAILED (str): The state indicating that an error occurred during the creation or preparation of the resource.
    """

    NO_STATUS = "NO_STATUS"
    CREATED = "CREATED"
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"


class ComputationMode(Enum):
    PRECOMPUTED = "PRECOMPUTED"
    CLIENT_COMPUTED = "CLIENT_COMPUTED"

    def __eq__(self, other: str) -> bool:
        return self.value == other

    def proto(self) -> int:
        if self == ComputationMode.PRECOMPUTED:
            return pb.ComputationMode.PRECOMPUTED
        elif self == ComputationMode.CLIENT_COMPUTED:
            return pb.ComputationMode.CLIENT_COMPUTED


@typechecked
@dataclass
class OperationType(Enum):
    GET = 0
    CREATE = 1


@typechecked
@dataclass
class SourceType(str, Enum):
    PRIMARY_SOURCE = "PRIMARY"
    DIRECTORY = "DIRECTORY"
    DF_TRANSFORMATION = "DF"
    SQL_TRANSFORMATION = "SQL"


class FilePrefix(Enum):
    S3 = ("s3://", "s3a://")
    S3A = ("s3a://",)
    HDFS = ("hdfs://",)
    GCS = ("gs://",)
    AZURE = ("abfss://",)

    def __init__(self, *valid_prefixes):
        self.prefixes = valid_prefixes

    @property
    def value(self):
        return self.prefixes[0]

    def validate_file_scheme(self, file_path: str) -> (bool, str):
        if not any(file_path.startswith(prefix) for prefix in self.prefixes):
            raise Exception(
                f"File path '{file_path}' must be a full path. Must start with '{self.prefixes}'"
            )

    @staticmethod
    def validate(store_type: str, file_path: str):
        try:
            prefix = FilePrefix[store_type]
            prefix.validate_file_scheme(file_path)
        except KeyError:
            raise Exception(f"Invalid store type: {store_type}")


class FileFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"

    @classmethod
    def is_supported(cls, file_path: str) -> bool:
        file_name = path.basename(file_path)

        for file_format in cls:
            if fnmatch(file_name, f"*.{file_format.value}"):
                return True

        return False

    @classmethod
    def get_format(cls, file_path: str, default: str = "") -> str:
        file_name = path.basename(file_path)

        for file_format in cls:
            if fnmatch(file_name, f"*.{file_format.value}"):
                return file_format.value

        if default != "":
            return default
        raise ValueError(f"File format not supported: {file_name}")

    @classmethod
    def supported_formats(cls) -> str:
        return ", ".join([file_format.value for file_format in cls])


@typechecked
@dataclass
class ResourceType(Enum):
    # ResourceType is an enumeration representing the possible types of
    # resources that may be registered with Featureform. Each value is based
    # on OfflineResourceType in providers/offline.go

    NO_TYPE = 0
    LABEL = 1
    FEATURE = 2
    TRAINING_SET = 3
    PRIMARY = 4
    TRANSFORMATION = 5
    FEATURE_MATERIALIZATION = 6
