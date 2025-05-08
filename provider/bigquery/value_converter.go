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
	case "INT64", "INTEGER", "BIGINT":
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
	case "FLOAT64", "DECIMAL":
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
	default:
		return types.Value{}, fferr.NewUnsupportedTypeError(string(nativeType))
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
