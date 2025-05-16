// Package snowflake provides Snowflake-specific type definitions
package snowflake

import (
	fftypes "github.com/featureform/fftypes"
)

var (
	INTEGER  = fftypes.NativeTypeLiteral("INTEGER")
	INT      = fftypes.NativeTypeLiteral("INT")
	BIGINT   = fftypes.NativeTypeLiteral("BIGINT")
	SMALLINT = fftypes.NativeTypeLiteral("SMALLINT")

	FLOAT            = fftypes.NativeTypeLiteral("FLOAT")
	FLOAT4           = fftypes.NativeTypeLiteral("FLOAT4")
	REAL             = fftypes.NativeTypeLiteral("REAL")
	FLOAT8           = fftypes.NativeTypeLiteral("FLOAT8")
	DOUBLE           = fftypes.NativeTypeLiteral("DOUBLE")
	DOUBLE_PRECISION = fftypes.NativeTypeLiteral("DOUBLE PRECISION")

	VARCHAR   = fftypes.NativeTypeLiteral("VARCHAR")
	CHAR      = fftypes.NativeTypeLiteral("CHAR")
	CHARACTER = fftypes.NativeTypeLiteral("CHARACTER")
	STRING    = fftypes.NativeTypeLiteral("STRING")
	TEXT      = fftypes.NativeTypeLiteral("TEXT")

	BOOLEAN = fftypes.NativeTypeLiteral("BOOLEAN")
	BOOL    = fftypes.NativeTypeLiteral("BOOL")

	DATE          = fftypes.NativeTypeLiteral("DATE")
	DATETIME      = fftypes.NativeTypeLiteral("DATETIME")
	TIME          = fftypes.NativeTypeLiteral("TIME")
	TIMESTAMP     = fftypes.NativeTypeLiteral("TIMESTAMP")
	TIMESTAMP_LTZ = fftypes.NativeTypeLiteral("TIMESTAMP_LTZ")
	TIMESTAMP_NTZ = fftypes.NativeTypeLiteral("TIMESTAMP_NTZ")
	TIMESTAMP_TZ  = fftypes.NativeTypeLiteral("TIMESTAMP_TZ")

	NUMBER = NewNumberType()
)

// StringToTypeLiteral maps the string representation to the corresponding NativeTypeLiteral
var StringToNativeType = map[string]fftypes.NativeType{
	"INTEGER":          INTEGER,
	"INT":              INT,
	"BIGINT":           BIGINT,
	"SMALLINT":         SMALLINT,
	"FLOAT":            FLOAT,
	"FLOAT4":           FLOAT4,
	"REAL":             REAL,
	"FLOAT8":           FLOAT8,
	"DOUBLE":           DOUBLE,
	"DOUBLE PRECISION": DOUBLE_PRECISION,
	"VARCHAR":          VARCHAR,
	"CHAR":             CHAR,
	"CHARACTER":        CHARACTER,
	"STRING":           STRING,
	"TEXT":             TEXT,
	"BOOLEAN":          BOOLEAN,
	"BOOL":             BOOL,
	"DATE":             DATE,
	"DATETIME":         DATETIME,
	"TIME":             TIME,
	"TIMESTAMP":        TIMESTAMP,
	"TIMESTAMP_LTZ":    TIMESTAMP_LTZ,
	"TIMESTAMP_NTZ":    TIMESTAMP_NTZ,
	"TIMESTAMP_TZ":     TIMESTAMP_TZ,
	"NUMBER":           NUMBER,
}

// NumberType represents a numeric database type with optional precision and scale
type NumberType struct {
	name      string
	Precision int64
	Scale     int64
}

func NewNumberType() *NumberType {
	return &NumberType{
		name:      "NUMBER",
		Precision: -1,
		Scale:     -1,
	}
}

func (t *NumberType) WithPrecision(precision int64) *NumberType {
	t.Precision = precision
	return t
}

func (t *NumberType) WithScale(scale int64) *NumberType {
	t.Scale = scale
	return t
}

func (t *NumberType) TypeName() string {
	if t.Precision == -1 {
		return t.name
	}
	if t.Scale == -1 {
		return t.name + "(" + string(rune(t.Precision)) + ")"
	}
	return t.name + "(" + string(rune(t.Precision)) + "," + string(rune(t.Scale)) + ")"
}

func (t *NumberType) GetPrecision() int64 {
	return t.Precision
}

func (t *NumberType) GetScale() int64 {
	return t.Scale
}
