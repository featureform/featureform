package bigquery

import types "github.com/featureform/fftypes"

var TypeMap = types.NativeToValueTypeMapper{
	// Integer types
	"INT64": types.Int64,

	// Floating point types
	"FLOAT64":    types.Float64,
	"NUMERIC":    types.Float64,
	"BIGNUMERIC": types.Float64,

	// Boolean type
	"BOOL": types.Bool,

	// String type
	"STRING": types.String,

	// Date/Time types
	"DATE":      types.Datetime,
	"DATETIME":  types.Datetime,
	"TIME":      types.Datetime,
	"TIMESTAMP": types.Datetime,

	//Complex types - mapped to string by default
	//These may require custom handling depending on the specific use case
	//"BYTES":      types.String,
	//"JSON":       types.String,
	//"STRUCT":     types.String,
	//"ARRAY":      types.String,
	//"GEOGRAPHY":  types.String,
	//"INTERVAL":   types.String,
}
