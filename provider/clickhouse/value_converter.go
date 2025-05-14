// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package clickhouse

import (
	"regexp"
	"time"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var ChConverter = Converter{}
var nullableRe = regexp.MustCompile(`Nullable\((.*)\)`)

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering ClickHouse converter")
	provider_type.RegisterConverter(provider_type.ClickHouseOffline, ChConverter)
}

// checkZeroTime returns a zero UTC time if the time is zero
func checkZeroTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.UnixMilli(0).UTC()
	}
	return t.UTC()
}

type Converter struct{}

func (c Converter) ParseNativeType(typeDetails types.NativeTypeDetails) (types.NewNativeType, error) {
	typeName := typeDetails.ColumnName()

	// Check if it's a Nullable type
	match := nullableRe.FindStringSubmatch(typeName)
	if len(match) == 2 {
		innerTypeStr := match[1]
		innerTypeDetails := types.NewSimpleNativeTypeDetails(innerTypeStr)
		innerType, err := c.ParseNativeType(innerTypeDetails)
		if err != nil {
			return nil, err
		}
		return NewNullableType(innerType), nil
	}

	// For other types, look up in the mapping
	if nativeType, ok := StringToNativeType[typeName]; ok {
		return nativeType, nil
	}

	return nil, fferr.NewUnsupportedTypeError("unknown type")
}

func (c Converter) GetType(nativeType types.NewNativeType) (types.ValueType, error) {
	// Handle nil case
	if nativeType == nil {
		return types.String, nil
	}

	// Handle NullableType by unwrapping it
	if nullableType, ok := nativeType.(*NullableType); ok {
		return c.GetType(nullableType.GetInnerType())
	}

	switch nativeType {
	// String type
	case STRING:
		return types.String, nil

	// Boolean type
	case BOOL:
		return types.Bool, nil

	// Integer types
	case INT:
		return types.Int, nil
	case INT8:
		return types.Int8, nil
	case INT16:
		return types.Int16, nil
	case INT32:
		return types.Int32, nil
	case INT64:
		return types.Int64, nil

	// Unsigned integer types
	case UINT8:
		return types.UInt8, nil
	case UINT16:
		return types.UInt16, nil
	case UINT32:
		return types.UInt32, nil
	case UINT64:
		return types.UInt64, nil

	// Floating point types
	case FLOAT32:
		return types.Float32, nil
	case FLOAT64:
		return types.Float64, nil

	// DateTime types
	case DATETIME64:
		return types.Timestamp, nil

	default:
		if typeLiteral, ok := nativeType.(types.NativeTypeLiteral); ok {
			return nil, fferr.NewUnsupportedTypeError(string(typeLiteral))
		}

		// If we can't determine the type at all
		return nil, fferr.NewUnsupportedTypeError("unknown type")
	}

}

// ConvertValue converts a value from its ClickHouse representation to a types.Value
// ConvertValue converts a value from its ClickHouse representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NewNativeType, value any) (types.Value, error) {
	if nullableType, ok := nativeType.(*NullableType); ok {
		return c.ConvertValue(nullableType.GetInnerType(), value)
	}

	// Get the target Featureform type for this native type
	targetType, err := c.GetType(nativeType)
	if err != nil {
		return types.Value{}, err
	}

	// If value is nil, return a nil value of the target type
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
	case types.Int:
		convertedValue, convErr = types.ConvertNumberToInt(value)
	case types.Int8:
		convertedValue, convErr = types.ConvertNumberToInt8(value)
	case types.Int16:
		convertedValue, convErr = types.ConvertNumberToInt16(value)
	case types.Int32:
		convertedValue, convErr = types.ConvertNumberToInt32(value)
	case types.Int64:
		convertedValue, convErr = types.ConvertNumberToInt64(value)
	case types.UInt8:
		convertedValue, convErr = types.ConvertNumberToUint8(value)
	case types.UInt16:
		convertedValue, convErr = types.ConvertNumberToUint16(value)
	case types.UInt32:
		convertedValue, convErr = types.ConvertNumberToUint32(value)
	case types.UInt64:
		convertedValue, convErr = types.ConvertNumberToUint64(value)
	case types.Float32:
		convertedValue, convErr = types.ConvertNumberToFloat32(value)
	case types.Float64:
		convertedValue, convErr = types.ConvertNumberToFloat64(value)
	case types.String:
		convertedValue, convErr = types.ConvertToString(value)
	case types.Bool:
		convertedValue, convErr = types.ConvertToBool(value)
	case types.Timestamp:
		if t, ok := value.(time.Time); ok {
			convertedValue = checkZeroTime(t)
			convErr = nil
		} else {
			convertedValue, convErr = types.ConvertDatetime(value)
		}
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
