// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package mysql

import (
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
		{"integer", "integer", types.Int32, false},
		{"bigint", "bigint", types.Int64, false},
		{"float", "float", types.Float64, false},
		{"varchar", "varchar", types.String, false},
		{"boolean", "boolean", types.Bool, false},
		{"timestamp", "timestamp", types.Timestamp, false},
		{"unsupported", "unsupported", nil, true},
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
		// Integer tests
		{"integer nil", "integer", nil, types.Value{NativeType: "integer", Type: types.Int32, Value: nil}, false},
		{"integer int", "integer", 123, types.Value{NativeType: "integer", Type: types.Int32, Value: int32(123)}, false},
		{"integer float", "integer", 123.45, types.Value{NativeType: "integer", Type: types.Int32, Value: int32(123)}, false},
		{"integer string", "integer", "123", types.Value{NativeType: "integer", Type: types.Int32, Value: int32(123)}, false},
		{"integer invalid", "integer", "abc", types.Value{}, true},

		// Bigint tests
		{"bigint nil", "bigint", nil, types.Value{NativeType: "bigint", Type: types.Int64, Value: nil}, false},
		{"bigint int", "bigint", 123, types.Value{NativeType: "bigint", Type: types.Int64, Value: int64(123)}, false},
		{"bigint large int", "bigint", 9223372036854775807, types.Value{NativeType: "bigint", Type: types.Int64, Value: int64(9223372036854775807)}, false},
		{"bigint string", "bigint", "123", types.Value{NativeType: "bigint", Type: types.Int64, Value: int64(123)}, false},
		{"bigint invalid", "bigint", "abc", types.Value{}, true},

		// Float tests
		{"float nil", "float", nil, types.Value{NativeType: "float", Type: types.Float64, Value: nil}, false},
		{"float float", "float", 123.45, types.Value{NativeType: "float", Type: types.Float64, Value: float64(123.45)}, false},
		{"float int", "float", 123, types.Value{NativeType: "float", Type: types.Float64, Value: float64(123)}, false},
		{"float string", "float", "123.45", types.Value{NativeType: "float", Type: types.Float64, Value: float64(123.45)}, false},
		{"float invalid", "float", "abc", types.Value{}, true},

		// Varchar tests
		{"varchar nil", "varchar", nil, types.Value{NativeType: "varchar", Type: types.String, Value: nil}, false},
		{"varchar string", "varchar", "test", types.Value{NativeType: "varchar", Type: types.String, Value: "test"}, false},
		{"varchar int", "varchar", 123, types.Value{NativeType: "varchar", Type: types.String, Value: "123"}, false},
		{"varchar float", "varchar", 123.45, types.Value{NativeType: "varchar", Type: types.String, Value: "123.45"}, false},

		// Boolean tests
		{"boolean nil", "boolean", nil, types.Value{NativeType: "boolean", Type: types.Bool, Value: nil}, false},
		{"boolean true", "boolean", true, types.Value{NativeType: "boolean", Type: types.Bool, Value: true}, false},
		{"boolean false", "boolean", false, types.Value{NativeType: "boolean", Type: types.Bool, Value: false}, false},
		{"boolean string true", "boolean", "true", types.Value{NativeType: "boolean", Type: types.Bool, Value: true}, false},
		{"boolean string false", "boolean", "false", types.Value{NativeType: "boolean", Type: types.Bool, Value: false}, false},
		{"boolean int 1", "boolean", 1, types.Value{NativeType: "boolean", Type: types.Bool, Value: true}, false},
		{"boolean int 0", "boolean", 0, types.Value{NativeType: "boolean", Type: types.Bool, Value: false}, false},
		{"boolean invalid", "boolean", "abc", types.Value{}, true},

		// Timestamp tests
		{"timestamp nil", "timestamp", nil, types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: nil}, false},
		{"timestamp time", "timestamp", testTime, types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: testTime}, false},

		// Unsupported type
		{"unsupported nil", "unsupported", nil, types.Value{}, true},
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

func TestTimestampConversion(t *testing.T) {
	converter := Converter{}

	// Test with different time formats
	t.Run("Timestamp with time.Time", func(t *testing.T) {
		testTime := time.Now().UTC()
		value, err := converter.ConvertValue("timestamp", testTime)
		assert.NoError(t, err)
		assert.Equal(t, types.Timestamp, value.Type)
		assert.Equal(t, testTime, value.Value)
	})

	t.Run("Timestamp with string", func(t *testing.T) {
		// This test depends on types.ConvertDatetime implementation
		timeStr := "2025-03-28T12:00:00Z"
		value, err := converter.ConvertValue("timestamp", timeStr)
		assert.NoError(t, err)
		assert.Equal(t, types.Timestamp, value.Type)
		// We can't directly check the value since ConvertDatetime might parse it differently
		// But we can check it's a valid time.Time object
		_, ok := value.Value.(time.Time)
		assert.True(t, ok)
	})

	t.Run("Timestamp with invalid value", func(t *testing.T) {
		_, err := converter.ConvertValue("timestamp", "not a timestamp")
		assert.Error(t, err)
	})
}

// Test edge cases not covered by the main test suite
func TestEdgeCases(t *testing.T) {
	converter := Converter{}

	t.Run("Integer boundary values", func(t *testing.T) {
		// Max int32
		value, err := converter.ConvertValue("integer", 2147483647)
		assert.NoError(t, err)
		assert.Equal(t, int32(2147483647), value.Value)

		// Min int32
		value, err = converter.ConvertValue("integer", -2147483648)
		assert.NoError(t, err)
		assert.Equal(t, int32(-2147483648), value.Value)
	})

	t.Run("Bigint boundary values", func(t *testing.T) {
		// Max int64
		value, err := converter.ConvertValue("bigint", 9223372036854775807)
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), value.Value)

		// Min int64
		value, err = converter.ConvertValue("bigint", -9223372036854775808)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9223372036854775808), value.Value)
	})

	t.Run("Empty string handling", func(t *testing.T) {
		value, err := converter.ConvertValue("varchar", "")
		assert.NoError(t, err)
		assert.Equal(t, "", value.Value)
	})
}
