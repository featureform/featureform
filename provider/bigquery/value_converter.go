package bigquery

import (
	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var bqConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering BigQuery converter")
	provider_type.RegisterConverter(provider_type.BigQueryOffline, bqConverter)
}

type Converter struct{}

func (c Converter) GetType(nativeType types.NativeType) (types.ValueType, error) {
	conv, err := c.ConvertValue(nativeType, nil)
	if err != nil {
		return nil, err
	}
	return conv.Type, nil
}

// ConvertValue converts a value from its BigQuery representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Convert the value based on the native type
	switch nativeType {
	// Integer types
	case "INT64":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int64,
				Value:      nil,
			}, nil
		}
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
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Float64,
				Value:      nil,
			}, nil
		}
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
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.String,
				Value:      nil,
			}, nil
		}
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
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Bool,
				Value:      nil,
			}, nil
		}
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
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Datetime,
				Value:      nil,
			}, nil
		}
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
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Timestamp,
				Value:      nil,
			}, nil
		}
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
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type: types.VectorType{
					ScalarType:  types.Float64, // Default scalar type
					Dimension:   1,
					IsEmbedding: false,
				},
				Value: nil,
			}, nil
		}
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
		return types.Value{}, fferr.NewUnsupportedTypeError(nativeType)
	}
}
