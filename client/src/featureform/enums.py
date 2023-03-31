from dataclasses import dataclass
from enum import Enum
from featureform.proto import metadata_pb2 as pb
from typeguard import typechecked


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
class SourceType(Enum):
    PRIMARY_SOURCE = "PRIMARY"
    DF_TRANSFORMATION = "DF"
    SQL_TRANSFORMATION = "SQL"


@typechecked
@dataclass
class FilePrefix(Enum):
    S3 = "s3://"
    S3A = "s3a://"
