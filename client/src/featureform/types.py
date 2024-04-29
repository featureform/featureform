import numpy
from .enums import ScalarType
from typing import Union

pd_to_ff_datatype = {
    numpy.dtype("float64"): ScalarType.FLOAT64,
    numpy.dtype("float32"): ScalarType.FLOAT32,
    numpy.dtype("int64"): ScalarType.INT64,
    numpy.dtype("int"): ScalarType.INT,
    numpy.dtype("int32"): ScalarType.INT32,
    numpy.dtype("O"): ScalarType.STRING,
    numpy.dtype("str"): ScalarType.STRING,
    numpy.dtype("bool"): ScalarType.BOOL,
}


class VectorType:
    def __init__(
        self, scalarType: Union[ScalarType, str], dims: int, is_embedding: bool = False
    ):
        self.scalarType = (
            scalarType if isinstance(scalarType, ScalarType) else ScalarType(scalarType)
        )
        self.dims = dims
        self.is_embedding = is_embedding

    def to_proto(self):
        proto_vec = pb.VectorType(
            scalar=self.scalarType.to_proto_enum(),
            dimension=self.dims,
            is_embedding=self.is_embedding,
        )
        return pb.ValueType(vector=proto_vec)

    @classmethod
    def from_proto(cls, proto_val):
        proto_vec = protoVal.vector_type
        scalar = ScalarType.from_proto(proto_vec.scalar)
        dims = proto_vec.dimension
        is_embedding = proto_vec.is_embedding
        return VectorType(scalar, dims, is_embedding)


def type_from_proto(proto_val):
    value_type = proto_val.WhichOneof("Type")
    if value_type == "scalar":
        return ScalarType.from_proto(proto_val.scalar)
    elif value_type == "vector":
        return VectorType.from_proto(proto_val.vector)
    else:
        return ScalarType.NIL
