package snowflake

import . "github.com/featureform/fftypes"

var TypeMap = NativeToValueTypeMapper{
	// Integer types
	"INTEGER":  Int32,
	"BIGINT":   Int64,
	"SMALLINT": Int32,

	// Floating point types
	"NUMBER":           Float64,
	"DECIMAL":          Float64,
	"NUMERIC":          Float64,
	"FLOAT":            Float32,
	"FLOAT4":           Float32,
	"FLOAT8":           Float64,
	"DOUBLE":           Float64,
	"DOUBLE PRECISION": Float64,
	"REAL":             Float32,

	// String types
	"VARCHAR":   String,
	"STRING":    String,
	"TEXT":      String,
	"CHAR":      String,
	"CHARACTER": String,

	// Boolean type
	"BOOLEAN": Bool,

	// Date/Time types
	"DATE":          Datetime,
	"DATETIME":      Datetime,
	"TIME":          Datetime,
	"TIMESTAMP":     Datetime,
	"TIMESTAMP_LTZ": Datetime,
	"TIMESTAMP_NTZ": Datetime,
	"TIMESTAMP_TZ":  Datetime,

	"ARRAY": VectorType{
		ScalarType:  Unknown,
		Dimension:   1,
		IsEmbedding: false,
	},
}
