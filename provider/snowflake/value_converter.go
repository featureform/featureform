// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package snowflake

import (
	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var SfConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering Snowflake converter")
	provider_type.RegisterConverter(provider_type.SnowflakeOffline, SfConverter)
}

type Converter struct{}

func (c Converter) GetType(nativeType types.NativeType) (types.ValueType, error) {
	conv, err := c.ConvertValue(nativeType, nil)
	if err != nil {
		return nil, err
	}
	return conv.Type, nil
}

// ConvertValue converts a value from its Snowflake representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Convert the value based on the native type
	switch nativeType {
	// Integer types
	case "INTEGER", "SMALLINT":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int32,
				Value:      nil,
			}, nil
		}
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
	case "FLOAT", "FLOAT4", "REAL":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Float32,
				Value:      nil,
			}, nil
		}
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

	// String types
	case "VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER":
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
	case "BOOLEAN":
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

	case "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ":
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
		// This should never happen since we check IsSupported above
		return types.Value{}, fferr.NewUnsupportedTypeError(string(nativeType))
	}
}
