// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package bigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var BqConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering BigQuery converter")
	provider_type.RegisterConverter(provider_type.BigQueryOffline, BqConverter)
}

type Converter struct{}

func (c Converter) GetType(nativeType types.NewNativeType) (types.ValueType, error) {
	conv, err := c.ConvertValue(nativeType, nil)
	if err != nil {
		return nil, err
	}
	return conv.Type, nil
}

// ConvertValue converts a value from its BigQuery representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NewNativeType, value any) (types.Value, error) {
	// First, determine the target type for this native type
	var targetType types.ValueType

	switch nativeType {
	// Integer types
	case INT64, INTEGER, BIGINT:
		targetType = types.Int64
	// Floating point types
	case FLOAT64, DECIMAL:
		targetType = types.Float64
	// String type
	case STRING:
		targetType = types.String
	// Boolean type
	case BOOL:
		targetType = types.Bool
	// Date/Time types
	case DATE, DATETIME, TIME:
		targetType = types.Datetime
	case TIMESTAMP:
		targetType = types.Timestamp
	default:
		if typeName, ok := nativeType.(types.NativeTypeLiteral); ok {
			return types.Value{}, fferr.NewUnsupportedTypeError(string(typeName))
		}
		return types.Value{}, fferr.NewUnsupportedTypeError("unknown type")
	}

	// Handle nil value case once
	if value == nil {
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      nil,
		}, nil
	}

	// Handle the non-nil case based on native type
	switch nativeType {
	// Integer types
	case INT64, INTEGER, BIGINT:
		convertedValue, err := types.ConvertNumberToInt64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	// Floating point types
	case FLOAT64, DECIMAL:
		convertedValue, err := types.ConvertNumberToFloat64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	// String type
	case STRING:
		convertedValue, err := types.ConvertToString(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	// Boolean type
	case BOOL:
		convertedValue, err := types.ConvertToBool(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	// Date/Time types
	case DATE, DATETIME, TIME:
		convertedValue, err := types.ConvertDatetime(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case TIMESTAMP:
		convertedValue, err := types.ConvertDatetime(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil
	default:
		if typeName, ok := nativeType.(types.NativeTypeLiteral); ok {
			return types.Value{}, fferr.NewUnsupportedTypeError(string(typeName))
		}
		return types.Value{}, fferr.NewUnsupportedTypeError("unknown type")
	}
}

func GetBigQueryType(valueType types.ValueType) (bigquery.FieldType, error) {
	switch valueType {
	case types.Int, types.Int32, types.Int64:
		return bigquery.IntegerFieldType, nil
	case types.Float32, types.Float64:
		// The BigQuery client names the Float type differently internally than what BigQuery is itself expecting.
		return "FLOAT64", nil
	case types.String:
		return bigquery.StringFieldType, nil
	case types.Bool:
		return bigquery.BooleanFieldType, nil
	case types.Timestamp:
		return bigquery.TimestampFieldType, nil
	default:
		return "", fferr.NewDataTypeNotFoundErrorf(valueType, "cannot find column type for value type")
	}
}
