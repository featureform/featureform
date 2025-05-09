// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package mysql

import (
	"time"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var MySqlConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering MySQL converter")
	provider_type.RegisterConverter(provider_type.MySqlOffline, MySqlConverter)
}

type Converter struct{}

func (c Converter) ParseNativeType(typeDetails types.NativeTypeDetails) (types.NewNativeType, error) {
	nativeType, ok := StringToNativeType[typeDetails.ColumnName()]
	if !ok {
		return nil, fferr.NewUnsupportedTypeError("Unsupported native type")
	}

	return nativeType, nil
}

func (c Converter) GetType(nativeType types.NewNativeType) (types.ValueType, error) {
	conv, err := c.ConvertValue(nativeType, nil)
	if err != nil {
		return nil, err
	}
	return conv.Type, nil
}

// ConvertValue converts a value from its MySQL representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NewNativeType, value any) (types.Value, error) {
	// First, get the target type for this native type
	var targetType types.ValueType

	switch nativeType {
	case INTEGER:
		targetType = types.Int32
	case BIGINT:
		targetType = types.Int64
	case FLOAT:
		targetType = types.Float64
	case VARCHAR:
		targetType = types.String
	case BOOLEAN:
		targetType = types.Bool
	case TIMESTAMP:
		targetType = types.Timestamp
	default:
		if typeLiteral, ok := nativeType.(types.NativeTypeLiteral); ok {
			return types.Value{}, fferr.NewUnsupportedTypeError(string(typeLiteral))
		}
		return types.Value{}, fferr.NewUnsupportedTypeError("unknown type")
	}

	// If value is nil, return nil value of the correct type
	if value == nil {
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      nil,
		}, nil
	}

	// Handle the non-nil case based on the target type
	switch nativeType {
	case INTEGER:
		convertedValue, err := types.ConvertNumberToInt32(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case BIGINT:
		convertedValue, err := types.ConvertNumberToInt64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case FLOAT:
		convertedValue, err := types.ConvertNumberToFloat64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case VARCHAR:
		convertedValue, err := types.ConvertToString(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case BOOLEAN:
		convertedValue, err := types.ConvertToBool(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case TIMESTAMP:
		if t, ok := value.(time.Time); ok {
			return types.Value{
				NativeType: nativeType,
				Type:       targetType,
				Value:      t.UTC(),
			}, nil
		}
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
		if typeLiteral, ok := nativeType.(types.NativeTypeLiteral); ok {
			return types.Value{}, fferr.NewUnsupportedTypeError(string(typeLiteral))
		}
		return types.Value{}, fferr.NewUnsupportedTypeError("unknown type")
	}
}
