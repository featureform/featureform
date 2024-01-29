import numpy
from .resources import ScalarType

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
