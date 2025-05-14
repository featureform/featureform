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

func TestParseNativeType(t *testing.T) {
	converter := Converter{}

	tests := []struct {
		name      string
		typeName  string
		expected  types.NewNativeType
		expectErr bool
	}{
		{"integer", "integer", INTEGER, false},
		{"bigint", "bigint", BIGINT, false},
		{"float", "float", FLOAT, false},
		{"varchar", "varchar", VARCHAR, false},
		{"boolean", "boolean", BOOLEAN, false},
		{"timestamp", "timestamp", TIMESTAMP, false},
		{"unsupported", "unsupported", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typeDetails := types.NewSimpleNativeTypeDetails(tt.typeName)
			nativeType, err := converter.ParseNativeType(typeDetails)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, nativeType)
			}
		})
	}
}

func TestConverterGetType(t *testing.T) {
	converter := Converter{}

	tests := []struct {
		name       string
		nativeType types.NewNativeType
		expected   types.ValueType
		expectErr  bool
	}{
		{"integer", INTEGER, types.Int32, false},
		{"bigint", BIGINT, types.Int64, false},
		{"float", FLOAT, types.Float64, false},
		{"varchar", VARCHAR, types.String, false},
		{"boolean", BOOLEAN, types.Bool, false},
		{"timestamp", TIMESTAMP, types.Timestamp, false},
		{"unsupported", types.NativeTypeLiteral("unsupported"), nil, true},
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
		nativeType types.NewNativeType
		value      interface{}
		expected   types.Value
		expectErr  bool
	}{
		// Integer tests
		{"integer nil", INTEGER, nil, types.Value{NativeType: INTEGER, Type: types.Int32, Value: nil}, false},
		{"integer int", INTEGER, 123, types.Value{NativeType: INTEGER, Type: types.Int32, Value: int32(123)}, false},
		{"integer float", INTEGER, 123.45, types.Value{NativeType: INTEGER, Type: types.Int32, Value: int32(123)}, false},
		{"integer string", INTEGER, "123", types.Value{NativeType: INTEGER, Type: types.Int32, Value: int32(123)}, false},
		{"integer invalid", INTEGER, "abc", types.Value{}, true},

		// Bigint tests
		{"bigint nil", BIGINT, nil, types.Value{NativeType: BIGINT, Type: types.Int64, Value: nil}, false},
		{"bigint int", BIGINT, 123, types.Value{NativeType: BIGINT, Type: types.Int64, Value: int64(123)}, false},
		{"bigint large int", BIGINT, 9223372036854775807, types.Value{NativeType: BIGINT, Type: types.Int64, Value: int64(9223372036854775807)}, false},
		{"bigint string", BIGINT, "123", types.Value{NativeType: BIGINT, Type: types.Int64, Value: int64(123)}, false},
		{"bigint invalid", BIGINT, "abc", types.Value{}, true},

		// Float tests
		{"float nil", FLOAT, nil, types.Value{NativeType: FLOAT, Type: types.Float64, Value: nil}, false},
		{"float float", FLOAT, 123.45, types.Value{NativeType: FLOAT, Type: types.Float64, Value: float64(123.45)}, false},
		{"float int", FLOAT, 123, types.Value{NativeType: FLOAT, Type: types.Float64, Value: float64(123)}, false},
		{"float string", FLOAT, "123.45", types.Value{NativeType: FLOAT, Type: types.Float64, Value: float64(123.45)}, false},
		{"float invalid", FLOAT, "abc", types.Value{}, true},

		// Varchar tests
		{"varchar nil", VARCHAR, nil, types.Value{NativeType: VARCHAR, Type: types.String, Value: nil}, false},
		{"varchar string", VARCHAR, "test", types.Value{NativeType: VARCHAR, Type: types.String, Value: "test"}, false},
		{"varchar int", VARCHAR, 123, types.Value{NativeType: VARCHAR, Type: types.String, Value: "123"}, false},
		{"varchar float", VARCHAR, 123.45, types.Value{NativeType: VARCHAR, Type: types.String, Value: "123.45"}, false},

		// Boolean tests
		{"boolean nil", BOOLEAN, nil, types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: nil}, false},
		{"boolean true", BOOLEAN, true, types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: true}, false},
		{"boolean false", BOOLEAN, false, types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: false}, false},
		{"boolean string true", BOOLEAN, "true", types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: true}, false},
		{"boolean string false", BOOLEAN, "false", types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: false}, false},
		{"boolean int 1", BOOLEAN, 1, types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: true}, false},
		{"boolean int 0", BOOLEAN, 0, types.Value{NativeType: BOOLEAN, Type: types.Bool, Value: false}, false},
		{"boolean invalid", BOOLEAN, "abc", types.Value{}, true},

		// Timestamp tests
		{"timestamp nil", TIMESTAMP, nil, types.Value{NativeType: TIMESTAMP, Type: types.Timestamp, Value: nil}, false},
		{"timestamp time", TIMESTAMP, testTime, types.Value{NativeType: TIMESTAMP, Type: types.Timestamp, Value: testTime}, false},

		// Unsupported type
		{"unsupported nil", types.NativeTypeLiteral("unsupported"), nil, types.Value{}, true},
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
		value, err := converter.ConvertValue(TIMESTAMP, testTime)
		assert.NoError(t, err)
		assert.Equal(t, types.Timestamp, value.Type)
		assert.Equal(t, testTime, value.Value)
	})

	t.Run("Timestamp with string", func(t *testing.T) {
		// This test depends on types.ConvertDatetime implementation
		timeStr := "2025-03-28T12:00:00Z"
		value, err := converter.ConvertValue(TIMESTAMP, timeStr)
		assert.NoError(t, err)
		assert.Equal(t, types.Timestamp, value.Type)
		// We can't directly check the value since ConvertDatetime might parse it differently
		// But we can check it's a valid time.Time object
		_, ok := value.Value.(time.Time)
		assert.True(t, ok)
	})

	t.Run("Timestamp with invalid value", func(t *testing.T) {
		_, err := converter.ConvertValue(TIMESTAMP, "not a timestamp")
		assert.Error(t, err)
	})
}

// Test edge cases not covered by the main test suite
func TestEdgeCases(t *testing.T) {
	converter := Converter{}

	t.Run("Integer boundary values", func(t *testing.T) {
		// Max int32
		value, err := converter.ConvertValue(INTEGER, 2147483647)
		assert.NoError(t, err)
		assert.Equal(t, int32(2147483647), value.Value)

		// Min int32
		value, err = converter.ConvertValue(INTEGER, -2147483648)
		assert.NoError(t, err)
		assert.Equal(t, int32(-2147483648), value.Value)
	})

	t.Run("Bigint boundary values", func(t *testing.T) {
		// Max int64
		value, err := converter.ConvertValue(BIGINT, 9223372036854775807)
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), value.Value)

		// Min int64
		value, err = converter.ConvertValue(BIGINT, -9223372036854775808)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9223372036854775808), value.Value)
	})

	t.Run("Empty string handling", func(t *testing.T) {
		value, err := converter.ConvertValue(VARCHAR, "")
		assert.NoError(t, err)
		assert.Equal(t, "", value.Value)
	})
}
