// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package clickhouse

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	types "github.com/featureform/fftypes"
)

func TestRegister(t *testing.T) {
	// This is a simple test to ensure the Register function doesn't panic
	Register()
}

func TestConverterGetType(t *testing.T) {
	converter := Converter{}

	tests := []struct {
		name       string
		nativeType types.NativeType
		expected   types.ValueType
		expectErr  bool
	}{
		// String type
		{"String", "String", types.String, false},
		{"Nullable(String)", "Nullable(String)", types.String, false},

		// Boolean type
		{"Bool", "Bool", types.Bool, false},
		{"Nullable(Bool)", "Nullable(Bool)", types.Bool, false},

		// Integer types
		{"Int", "Int", types.Int, false},
		{"Int8", "Int8", types.Int8, false},
		{"Int16", "Int16", types.Int16, false},
		{"Int32", "Int32", types.Int32, false},
		{"Int64", "Int64", types.Int64, false},
		{"Nullable(Int)", "Nullable(Int)", types.Int, false},
		{"Nullable(Int32)", "Nullable(Int32)", types.Int32, false},

		// Unsigned integer types
		{"UInt8", "UInt8", types.UInt8, false},
		{"UInt16", "UInt16", types.UInt16, false},
		{"UInt32", "UInt32", types.UInt32, false},
		{"UInt64", "UInt64", types.UInt64, false},
		{"Nullable(UInt8)", "Nullable(UInt8)", types.UInt8, false},

		// Float types
		{"Float32", "Float32", types.Float32, false},
		{"Float64", "Float64", types.Float64, false},
		{"Nullable(Float32)", "Nullable(Float32)", types.Float32, false},

		// DateTime64 type for timestamps
		{"DateTime64(9)", "DateTime64(9)", types.Timestamp, false},
		{"Nullable(DateTime64(9))", "Nullable(DateTime64(9))", types.Timestamp, false},

		// Unsupported type
		{"UnsupportedType", "UnsupportedType", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueType, err := converter.GetType(tt.nativeType)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, valueType)
			}
		})
	}
}

func TestConverterConvertValue(t *testing.T) {
	converter := Converter{}

	// Fixed test time for consistency
	testTime := time.Date(2025, 3, 28, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		nativeType types.NativeType
		value      interface{}
		expected   types.Value
		expectErr  bool
	}{
		// String tests
		{"String nil", "String", nil, types.Value{NativeType: "String", Type: types.String, Value: nil}, false},
		{"String value", "String", "test", types.Value{NativeType: "String", Type: types.String, Value: "test"}, false},
		{"Nullable(String) nil", "Nullable(String)", nil, types.Value{NativeType: "Nullable(String)", Type: types.String, Value: nil}, false},
		{"Nullable(String) value", "Nullable(String)", "test", types.Value{NativeType: "Nullable(String)", Type: types.String, Value: "test"}, false},

		// Bool tests
		{"Bool nil", "Bool", nil, types.Value{NativeType: "Bool", Type: types.Bool, Value: nil}, false},
		{"Bool true", "Bool", true, types.Value{NativeType: "Bool", Type: types.Bool, Value: true}, false},
		{"Bool false", "Bool", false, types.Value{NativeType: "Bool", Type: types.Bool, Value: false}, false},
		{"Bool int 1", "Bool", 1, types.Value{NativeType: "Bool", Type: types.Bool, Value: true}, false},
		{"Bool int 0", "Bool", 0, types.Value{NativeType: "Bool", Type: types.Bool, Value: false}, false},

		// Int tests
		{"Int nil", "Int", nil, types.Value{NativeType: "Int", Type: types.Int, Value: nil}, false},
		{"Int value", "Int", 123, types.Value{NativeType: "Int", Type: types.Int, Value: 123}, false},

		// Int8 tests
		{"Int8 nil", "Int8", nil, types.Value{NativeType: "Int8", Type: types.Int8, Value: nil}, false},
		{"Int8 value", "Int8", int8(127), types.Value{NativeType: "Int8", Type: types.Int8, Value: int8(127)}, false},

		// Int16 tests
		{"Int16 nil", "Int16", nil, types.Value{NativeType: "Int16", Type: types.Int16, Value: nil}, false},
		{"Int16 value", "Int16", int16(32767), types.Value{NativeType: "Int16", Type: types.Int16, Value: int16(32767)}, false},

		// Int32 tests
		{"Int32 nil", "Int32", nil, types.Value{NativeType: "Int32", Type: types.Int32, Value: nil}, false},
		{"Int32 value", "Int32", int32(2147483647), types.Value{NativeType: "Int32", Type: types.Int32, Value: int32(2147483647)}, false},

		// Int64 tests
		{"Int64 nil", "Int64", nil, types.Value{NativeType: "Int64", Type: types.Int64, Value: nil}, false},
		{"Int64 value", "Int64", int64(9223372036854775807), types.Value{NativeType: "Int64", Type: types.Int64, Value: int64(9223372036854775807)}, false},

		// UInt8 tests
		{"UInt8 nil", "UInt8", nil, types.Value{NativeType: "UInt8", Type: types.UInt8, Value: nil}, false},
		{"UInt8 value", "UInt8", uint8(255), types.Value{NativeType: "UInt8", Type: types.UInt8, Value: uint8(255)}, false},

		// UInt16 tests
		{"UInt16 nil", "UInt16", nil, types.Value{NativeType: "UInt16", Type: types.UInt16, Value: nil}, false},
		{"UInt16 value", "UInt16", uint16(65535), types.Value{NativeType: "UInt16", Type: types.UInt16, Value: uint16(65535)}, false},

		// UInt32 tests
		{"UInt32 nil", "UInt32", nil, types.Value{NativeType: "UInt32", Type: types.UInt32, Value: nil}, false},
		{"UInt32 value small", "UInt32", uint32(2147483647), types.Value{NativeType: "UInt32", Type: types.UInt32, Value: uint32(2147483647)}, false},
		{"UInt32 value large", "UInt32", uint32(4294967295), types.Value{NativeType: "UInt32", Type: types.UInt32, Value: uint32(4294967295)}, false},

		// UInt64 tests
		{"UInt64 nil", "UInt64", nil, types.Value{NativeType: "UInt64", Type: types.UInt64, Value: nil}, false},
		{"UInt64 value small", "UInt64", uint64(9223372036854775807), types.Value{NativeType: "UInt64", Type: types.UInt64, Value: uint64(9223372036854775807)}, false},
		{"UInt64 value large", "UInt64", uint64(18446744073709551615), types.Value{NativeType: "UInt64", Type: types.UInt64, Value: uint64(18446744073709551615)}, false},

		// Float32 tests
		{"Float32 nil", "Float32", nil, types.Value{NativeType: "Float32", Type: types.Float32, Value: nil}, false},
		{"Float32 value", "Float32", float32(3.14159), types.Value{NativeType: "Float32", Type: types.Float32, Value: float32(3.14159)}, false},

		// Float64 tests
		{"Float64 nil", "Float64", nil, types.Value{NativeType: "Float64", Type: types.Float64, Value: nil}, false},
		{"Float64 value", "Float64", float64(3.14159265359), types.Value{NativeType: "Float64", Type: types.Float64, Value: float64(3.14159265359)}, false},

		// DateTime64 tests
		{"DateTime64(9) nil", "DateTime64(9)", nil, types.Value{NativeType: "DateTime64(9)", Type: types.Timestamp, Value: nil}, false},
		{"DateTime64(9) value", "DateTime64(9)", testTime, types.Value{NativeType: "DateTime64(9)", Type: types.Timestamp, Value: testTime}, false},
		{"DateTime64(9) zero time", "DateTime64(9)", time.Time{}, types.Value{NativeType: "DateTime64(9)", Type: types.Timestamp, Value: time.UnixMilli(0).UTC()}, false},

		// Unsupported type
		{"UnsupportedType nil", "UnsupportedType", nil, types.Value{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := converter.ConvertValue(tt.nativeType, tt.value)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.NativeType, value.NativeType)
				assert.Equal(t, tt.expected.Type, value.Type)
				assert.Equal(t, tt.expected.Value, value.Value)
			}
		})
	}
}

func TestPointerDereference(t *testing.T) {
	converter := Converter{}

	// Test the pointer dereferencing functionality
	stringPtr := new(interface{})
	stringVal := "test"
	*stringPtr = stringVal

	intPtr := new(interface{})
	intVal := int32(123)
	*intPtr = intVal

	tests := []struct {
		name          string
		nativeType    types.NativeType
		value         interface{}
		expectedValue interface{}
	}{
		{"String pointer", "String", stringPtr, "test"},
		{"Int32 pointer", "Int32", intPtr, int32(123)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := converter.ConvertValue(tt.nativeType, tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedValue, value.Value)
		})
	}
}

func TestEdgeCases(t *testing.T) {
	converter := Converter{}

	// Test edge cases specific to the ClickHouse converter
	t.Run("Integer boundaries", func(t *testing.T) {
		// Int8 boundaries
		int8Min := int8(math.MinInt8)
		int8Max := int8(math.MaxInt8)
		valueMin, err := converter.ConvertValue("Int8", int8Min)
		assert.NoError(t, err)
		assert.Equal(t, int8Min, valueMin.Value)
		valueMax, err := converter.ConvertValue("Int8", int8Max)
		assert.NoError(t, err)
		assert.Equal(t, int8Max, valueMax.Value)

		// Int16 boundaries
		int16Min := int16(math.MinInt16)
		int16Max := int16(math.MaxInt16)
		valueMin, err = converter.ConvertValue("Int16", int16Min)
		assert.NoError(t, err)
		assert.Equal(t, int16Min, valueMin.Value)
		valueMax, err = converter.ConvertValue("Int16", int16Max)
		assert.NoError(t, err)
		assert.Equal(t, int16Max, valueMax.Value)

		// Int32 boundaries
		int32Min := int32(math.MinInt32)
		int32Max := int32(math.MaxInt32)
		valueMin, err = converter.ConvertValue("Int32", int32Min)
		assert.NoError(t, err)
		assert.Equal(t, int32Min, valueMin.Value)
		valueMax, err = converter.ConvertValue("Int32", int32Max)
		assert.NoError(t, err)
		assert.Equal(t, int32Max, valueMax.Value)

		// Int64 boundaries
		int64Min := int64(math.MinInt64)
		int64Max := int64(math.MaxInt64)
		valueMin, err = converter.ConvertValue("Int64", int64Min)
		assert.NoError(t, err)
		assert.Equal(t, int64Min, valueMin.Value)
		valueMax, err = converter.ConvertValue("Int64", int64Max)
		assert.NoError(t, err)
		assert.Equal(t, int64Max, valueMax.Value)

		// UInt8 boundaries
		uint8Max := uint8(math.MaxUint8)
		valueMax, err = converter.ConvertValue("UInt8", uint8Max)
		assert.NoError(t, err)
		assert.Equal(t, uint8Max, valueMax.Value)

		// UInt16 boundaries
		uint16Max := uint16(math.MaxUint16)
		valueMax, err = converter.ConvertValue("UInt16", uint16Max)
		assert.NoError(t, err)
		assert.Equal(t, uint16Max, valueMax.Value)

		// UInt32 boundaries
		uint32Max := uint32(math.MaxUint32)
		valueMax, err = converter.ConvertValue("UInt32", uint32Max)
		assert.NoError(t, err)
		assert.Equal(t, uint32Max, valueMax.Value)

		// UInt64 boundaries
		uint64Max := uint64(math.MaxUint64)
		valueMax, err = converter.ConvertValue("UInt64", uint64Max)
		assert.NoError(t, err)
		assert.Equal(t, uint64Max, valueMax.Value)
	})

	t.Run("Zero time handling", func(t *testing.T) {
		// Test the special handling for zero time
		zeroTime := time.Time{}
		value, err := converter.ConvertValue("DateTime64(9)", zeroTime)
		assert.NoError(t, err)
		assert.Equal(t, time.UnixMilli(0).UTC(), value.Value)

		// Test with a normal time
		normalTime := time.Date(2025, 3, 28, 12, 0, 0, 0, time.Local)
		value, err = converter.ConvertValue("DateTime64(9)", normalTime)
		assert.NoError(t, err)

		// Ensure the time is in UTC
		resultTime, ok := value.Value.(time.Time)
		assert.True(t, ok)
		assert.Equal(t, time.UTC, resultTime.Location())
	})
}
