from dataclasses import dataclass
from enum import Enum
from featureform.proto import metadata_pb2 as pb
from typeguard import typechecked
from os import path
from fnmatch import fnmatch


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
    DF_TRANSFORMATION = "DF"
    SQL_TRANSFORMATION = "SQL"


@typechecked
@dataclass
class FilePrefix(Enum):
    S3 = "s3://"
    S3A = "s3a://"


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
    def get_format(cls, file_path: str) -> str:
        file_name = path.basename(file_path)

        for file_format in cls:
            if fnmatch(file_name, f"*.{file_format.value}"):
                return file_format.value

        raise ValueError(f"File format not supported: {file_name}")

    @classmethod
    def supported_formats(cls) -> str:
        return ", ".join([file_format.value for file_format in cls])
