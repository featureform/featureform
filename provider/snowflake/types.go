package snowflake

import types "github.com/featureform/fftypes"

var SnowflakeTypeMap = map[string]types.ValueType{
	// Integer types
	"INTEGER":  types.Int32,
	"BIGINT":   types.Int64,
	"SMALLINT": types.Int32,

	// Floating point types
	"NUMBER":           types.Float64,
	"DECIMAL":          types.Float64,
	"NUMERIC":          types.Float64,
	"FLOAT":            types.Float32,
	"FLOAT4":           types.Float32,
	"FLOAT8":           types.Float64,
	"DOUBLE":           types.Float64,
	"DOUBLE PRECISION": types.Float64,
	"REAL":             types.Float32,

	// String types
	"VARCHAR":   types.String,
	"STRING":    types.String,
	"TEXT":      types.String,
	"CHAR":      types.String,
	"CHARACTER": types.String,

	// Boolean type
	"BOOLEAN": types.Bool,

	// Date/Time types
	"DATE":          types.Datetime,
	"DATETIME":      types.Datetime,
	"TIME":          types.Datetime,
	"TIMESTAMP":     types.Datetime,
	"TIMESTAMP_LTZ": types.Datetime,
	"TIMESTAMP_NTZ": types.Datetime,
	"TIMESTAMP_TZ":  types.Datetime,

	// Types without direct mapping - default to string
	"VARIANT":   types.String,
	"OBJECT":    types.String,
	"ARRAY":     types.String,
	"GEOGRAPHY": types.String,
	"BINARY":    types.String,
}
