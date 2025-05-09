// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package postgres

import (
	"strconv"
	"strings"

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

// ConvertValue converts a value from its PostgreSQL representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NewNativeType, value any) (types.Value, error) {
	// First, determine the target type for this native type
	var targetType types.ValueType

	switch nativeType {
	case INTEGER, INT:
		targetType = types.Int32
	case BIGINT:
		targetType = types.Int64
	case FLOAT8, NUMERIC, DOUBLE_PRECISION:
		targetType = types.Float64
	case VARCHAR, CHARACTER_VARYING:
		targetType = types.String
	case BOOLEAN:
		targetType = types.Bool
	case DATE, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMPTZ:
		targetType = types.Timestamp
	default:
		// Try to handle string-based types if passed directly
		if typeLiteral, ok := nativeType.(types.NativeTypeLiteral); ok {
			normalizedType := strings.ToLower(string(typeLiteral))

			// Check if this is a known type in normalized form
			for knownType, knownLiteral := range StringToNativeType {
				if normalizedType == knownType {
					return c.ConvertValue(knownLiteral, value)
				}
			}

			return types.Value{}, fferr.NewUnsupportedTypeError(string(typeLiteral))
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

	// Now handle the non-nil case based on native type
	switch nativeType {
	case INTEGER, INT:
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

	case FLOAT8:
		convertedValue, err := types.ConvertNumberToFloat64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case NUMERIC, DOUBLE_PRECISION:
		if byteArray, ok := value.([]uint8); ok {
			floatVal, err := strconv.ParseFloat(string(byteArray), 64)
			if err != nil {
				return types.Value{}, err
			}
			return types.Value{
				NativeType: nativeType,
				Type:       targetType,
				Value:      floatVal,
			}, nil
		}
		convertedValue, err := types.ConvertNumberToFloat64(value)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      convertedValue,
		}, nil

	case VARCHAR, CHARACTER_VARYING:
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

	case DATE, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMPTZ:
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
		// Try to handle string-based types if passed directly
		if typeLiteral, ok := nativeType.(types.NativeTypeLiteral); ok {
			normalizedType := strings.ToLower(string(typeLiteral))

			// Check if this is a known type in normalized form
			for knownType, knownLiteral := range StringToNativeType {
				if normalizedType == knownType {
					return c.ConvertValue(knownLiteral, value)
				}
			}

			return types.Value{}, fferr.NewUnsupportedTypeError(string(typeLiteral))
		}

		return types.Value{}, fferr.NewUnsupportedTypeError("unknown type")
	}
}
