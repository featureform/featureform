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

func (c Converter) ParseNativeType(typeDetails types.NativeTypeDetails) (types.NewNativeType, error) {
	typeName := typeDetails.ColumnName()

	// Look up the type in our mapping
	nativeType, ok := StringToNativeType[typeName]
	if !ok {
		return nil, fferr.NewUnsupportedTypeError("Unsupported native type: " + typeName)
	}

	return nativeType, nil
}

func (c Converter) GetType(nativeType types.NewNativeType) (types.ValueType, error) {
	// Handle nil case
	if nativeType == nil {
		return types.String, nil
	}

	switch nativeType {
	// Integer types
	case INT64, INTEGER, BIGINT:
		return types.Int64, nil
	// Floating point types
	case FLOAT64, DECIMAL:
		return types.Float64, nil
	// String type
	case STRING:
		return types.String, nil
	// Boolean type
	case BOOL:
		return types.Bool, nil
	// Date/Time types
	case DATE, DATETIME, TIME:
		return types.Datetime, nil
	case TIMESTAMP:
		return types.Timestamp, nil
	default:
		if typeName, ok := nativeType.(types.NativeTypeLiteral); ok {
			return nil, fferr.NewUnsupportedTypeError(string(typeName))
		}
		return nil, fferr.NewUnsupportedTypeError("unknown type")
	}
}

// ConvertValue converts a value from its BigQuery representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NewNativeType, value any) (types.Value, error) {
	// Get the target type for this native type
	targetType, err := c.GetType(nativeType)
	if err != nil {
		return types.Value{}, err
	}

	// Handle nil value case
	if value == nil {
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      nil,
		}, nil
	}

	// Convert the value according to target type
	var convertedValue any
	var convErr error

	switch targetType {
	case types.Int32:
		convertedValue, convErr = types.ConvertNumberToInt32(value)
	case types.Int64:
		convertedValue, convErr = types.ConvertNumberToInt64(value)
	case types.Float32:
		convertedValue, convErr = types.ConvertNumberToFloat32(value)
	case types.Float64:
		convertedValue, convErr = types.ConvertNumberToFloat64(value)
	case types.String:
		convertedValue, convErr = types.ConvertToString(value)
	case types.Bool:
		convertedValue, convErr = types.ConvertToBool(value)
	case types.Timestamp, types.Datetime:
		convertedValue, convErr = types.ConvertDatetime(value)
	default:
		return types.Value{}, fferr.NewUnsupportedTypeError("Unsupported target type")
	}

	if convErr != nil {
		return types.Value{}, fferr.NewInternalErrorf("conversion error: %v", convErr)
	}

	return types.Value{
		NativeType: nativeType,
		Type:       targetType,
		Value:      convertedValue,
	}, nil
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
