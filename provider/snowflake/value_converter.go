package snowflake

import (
	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

// SupportedTypes is a set of all supported Snowflake types
// NOTE: If you add a new type here, make sure to add a corresponding conversion function in ConvertValue
var SupportedTypes = map[types.NativeType]bool{
	// Integer types
	"INTEGER":  true,
	"BIGINT":   true,
	"SMALLINT": true,

	// Floating point types
	"NUMBER":           true,
	"DECIMAL":          true,
	"NUMERIC":          true,
	"FLOAT":            true,
	"FLOAT4":           true,
	"FLOAT8":           true,
	"DOUBLE":           true,
	"DOUBLE PRECISION": true,
	"REAL":             true,

	// String types
	"VARCHAR":   true,
	"STRING":    true,
	"TEXT":      true,
	"CHAR":      true,
	"CHARACTER": true,

	// Boolean type
	"BOOLEAN": true,

	// Date/Time types
	"DATE":          true,
	"DATETIME":      true,
	"TIME":          true,
	"TIMESTAMP":     true,
	"TIMESTAMP_LTZ": true,
	"TIMESTAMP_NTZ": true,
	"TIMESTAMP_TZ":  true,

	// Array type
	"ARRAY": true,
}

var sfConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering Snowflake converter")
	provider_type.RegisterConverter(provider_type.SnowflakeOffline, sfConverter)
}

type Converter struct{}

func (c Converter) IsSupportedType(nativeType types.NativeType) bool {
	_, exists := SupportedTypes[nativeType]
	return exists
}

// ConvertValue converts a value from its Snowflake representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Handle nil values
	if value == nil {
		return types.Value{
			NativeType: nativeType,
			Type:       nil,
			Value:      nil,
		}, nil
	}

	// Check if the type is supported
	if !c.IsSupportedType(nativeType) {
		return types.Value{}, fferr.NewInternalErrorf("Unsupported Snowflake type: %s", nativeType)
	}

	// Convert the value based on the native type
	switch nativeType {
	// Integer types
	case "INTEGER", "SMALLINT":
		convertedValue, err := types.ConvertNumberToInt32(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int32,
			Value:      convertedValue,
		}, nil

	case "BIGINT":
		convertedValue, err := types.ConvertNumberToInt64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int64,
			Value:      convertedValue,
		}, nil

	// Floating point types
	case "FLOAT", "FLOAT4", "REAL":
		convertedValue, err := types.ConvertNumberToFloat32(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Float32,
			Value:      convertedValue,
		}, nil

	case "NUMBER", "DECIMAL", "NUMERIC", "FLOAT8", "DOUBLE", "DOUBLE PRECISION":
		convertedValue, err := types.ConvertNumberToFloat64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Float64,
			Value:      convertedValue,
		}, nil

	// String types
	case "VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER":
		convertedValue, err := types.ConvertToString(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.String,
			Value:      convertedValue,
		}, nil

	// Boolean type
	case "BOOLEAN":
		convertedValue, err := types.ConvertToBool(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Bool,
			Value:      convertedValue,
		}, nil

	// Date/Time types
	case "DATE", "DATETIME", "TIME":
		convertedValue, err := types.ConvertDatetime(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Datetime,
			Value:      convertedValue,
		}, nil

	case "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ":
		convertedValue, err := types.ConvertDatetime(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Timestamp,
			Value:      convertedValue,
		}, nil

	// Array type
	case "ARRAY":
		// Extract the array elements and infer the scalar type
		scalarType, convertedValue, err := types.ConvertVectorValue(value)
		if err != nil {
			return types.Value{}, err
		}

		return types.Value{
			NativeType: nativeType,
			Type: types.VectorType{
				ScalarType:  scalarType,
				Dimension:   1,
				IsEmbedding: false,
			},
			Value: convertedValue,
		}, nil

	default:
		// This should never happen since we check IsSupported above
		return types.Value{}, fferr.NewInternalErrorf("Unsupported Snowflake type: %s", nativeType)
	}
}
