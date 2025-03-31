// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package postgres

import (
	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var PgConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering PostgreSQL converter")
	provider_type.RegisterConverter(provider_type.PostgresOffline, PgConverter)
}

type Converter struct{}

func (c Converter) GetType(nativeType types.NativeType) (types.ValueType, error) {
	conv, err := c.ConvertValue(nativeType, nil)
	if err != nil {
		return nil, err
	}
	return conv.Type, nil
}

// ConvertValue converts a value from its PostgreSQL representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Convert the value based on the native type
	switch nativeType {
	case "integer":
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

	case "bigint":
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

	case "float8":
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

	case "varchar":
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

	case "boolean":
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

	case "timestamp with time zone":
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
