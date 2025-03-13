package clickhouse

import . "github.com/featureform/fftypes"

var TypeMap = NativeToValueTypeMapper{
	// Integer types
	"Int8":   Int32,
	"Int16":  Int32,
	"Int32":  Int32,
	"Int64":  Int64,
	"UInt8":  Int32,
	"UInt16": Int32,
	"UInt32": Int64,
	"UInt64": Int64,

	// Floating point types
	"Float32": Float32,
	"Float64": Float64,

	// String types
	"String":      String,
	"FixedString": String,

	// Boolean type (ClickHouse does not have native bool, usually UInt8 is used)
	"Bool": Bool,

	// Date/Time types
	"Date":       Datetime,
	"DateTime":   Datetime,
	"DateTime64": Datetime,
}
