package bigquery

import (
	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

// SupportedTypes is a set of all supported BigQuery types
// NOTE: If you add a new type here, make sure to add a corresponding conversion function in ConvertValue
var SupportedTypes = map[types.NativeType]bool{
	// Integer types
	"INT64": true,

	// Floating point types
	"FLOAT64":    true,
	"NUMERIC":    true,
	"BIGNUMERIC": true,

	// Boolean type
	"BOOL": true,

	// String type
	"STRING": true,

	// Date/Time types
	"DATE":      true,
	"DATETIME":  true,
	"TIME":      true,
	"TIMESTAMP": true,

	// Array type
	"ARRAY": true,
}

var bqConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering BigQuery converter")
	provider_type.RegisterConverter(provider_type.BigQueryOffline, bqConverter)
}

type Converter struct{}

func (c Converter) IsSupportedType(nativeType types.NativeType) bool {
	_, exists := SupportedTypes[nativeType]
	return exists
}

// ConvertValue converts a value from its BigQuery representation to a types.Value
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
		return types.Value{}, fferr.NewInternalErrorf("Unsupported BigQuery type: %s", nativeType)
	}

	// Convert the value based on the native type
	switch nativeType {
	// Integer types
	case "INT64":
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
	case "FLOAT64", "NUMERIC", "BIGNUMERIC":
		convertedValue, err := types.ConvertNumberToFloat64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Float64,
			Value:      convertedValue,
		}, nil

	// String type
	case "STRING":
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
	case "BOOL":
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

	case "TIMESTAMP":
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
				Dimension:   1, // BigQuery arrays are 1-dimensional
				IsEmbedding: false,
			},
			Value: convertedValue,
		}, nil

	default:
		// This should never happen since we check IsSupportedType above
		return types.Value{}, fferr.NewInternalErrorf("Unsupported BigQuery type: %s", nativeType)
	}
}
