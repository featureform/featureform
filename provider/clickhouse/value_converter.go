// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package clickhouse

import (
	"math"
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

// deferencePointer handles potential pointer-to-pointer scenarios
func deferencePointer(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	// If v is a pointer, dereference it
	if ptr, ok := v.(*interface{}); ok && ptr != nil {
		return deferencePointer(*ptr)
	}

	return v
}

type Converter struct{}

func (c Converter) GetType(nativeType types.NativeType) (types.ValueType, error) {
	conv, err := c.ConvertValue(nativeType, nil)
	if err != nil {
		return nil, err
	}
	return conv.Type, nil
}

// ConvertValue converts a value from its ClickHouse representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Dereference any pointers
	value = deferencePointer(value)

	// Extract the underlying type if it's Nullable
	typeStr := string(nativeType)
	match := nullableRe.FindStringSubmatch(typeStr)
	if len(match) == 2 {
		typeStr = match[1]
	}

	// Get the value type
	var convertedValue interface{}

	switch typeStr {
	case "String":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.String,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.String,
			Value:      value.(string),
		}, nil
	case "Bool":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Bool,
				Value:      nil,
			}, nil
		}
		toBool, err := types.ConvertToBool(value)
		if err != nil {
			return types.Value{}, fferr.NewInternalErrorf("could not convert value to bool: %v", err)
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Bool,
			Value:      toBool,
		}, nil
	case "Int":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int,
			Value:      value.(int),
		}, nil
	case "Int8":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int8,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int8,
			Value:      value.(int8),
		}, nil
	case "Int16":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int16,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int16,
			Value:      value.(int16),
		}, nil
	case "Int32":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int32,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int32,
			Value:      value.(int32),
		}, nil
	case "Int64":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int64,
				Value:      nil,
			}, nil
		}
		if intValue := value.(int64); intValue >= math.MinInt && intValue <= math.MaxInt {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Int64,
				Value:      intValue,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Int64,
			Value:      value.(int64),
		}, nil
	case "UInt8":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.UInt8,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.UInt8,
			Value:      value.(uint8),
		}, nil
	case "UInt16":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.UInt16,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.UInt16,
			Value:      value.(uint16),
		}, nil
	case "UInt32":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.UInt32,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.UInt32,
			Value:      value.(uint32),
		}, nil
	case "UInt64":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.UInt64,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.UInt64,
			Value:      value.(uint64),
		}, nil
	case "Float32":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Float32,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Float32,
			Value:      value.(float32),
		}, nil
	case "Float64":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Float64,
				Value:      nil,
			}, nil
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Float64,
			Value:      value.(float64),
		}, nil
	case "DateTime64(9)":
		if value == nil {
			return types.Value{
				NativeType: nativeType,
				Type:       types.Timestamp,
				Value:      nil,
			}, nil
		}
		if t, ok := value.(time.Time); !ok {
			convertedValue = time.UnixMilli(0).UTC()
		} else {
			convertedValue = checkZeroTime(t)
		}
		return types.Value{
			NativeType: nativeType,
			Type:       types.Timestamp,
			Value:      convertedValue,
		}, nil
	default:
		// throw an error if the type is not supported
		return types.Value{}, fferr.NewUnsupportedTypeError(typeStr)
	}
}
