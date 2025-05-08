// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package postgres

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
		// Integer types
		{"integer", "integer", types.Int32, false},
		{"int", "int", types.Int32, false},

		// Bigint type
		{"bigint", "bigint", types.Int64, false},

		// Float types
		{"float8", "float8", types.Float64, false},
		{"numeric", "numeric", types.Float64, false},

		// String types
		{"varchar", "varchar", types.String, false},

		// Boolean type
		{"boolean", "boolean", types.Bool, false},

		// Timestamp types
		{"timestamp with time zone", "timestamp with time zone", types.Timestamp, false},
		{"timestamptz", "timestamptz", types.Timestamp, false},

		// Unsupported type
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
		{"INT nil", "INT", nil, types.Value{NativeType: "INT", Type: types.Int32, Value: nil}, false},
		{"INT int", "INT", 123, types.Value{NativeType: "INT", Type: types.Int32, Value: int32(123)}, false},
		{"INT float", "INT", 123.45, types.Value{NativeType: "INT", Type: types.Int32, Value: int32(123)}, false},
		{"INT string", "INT", "123", types.Value{NativeType: "INT", Type: types.Int32, Value: int32(123)}, false},
		{"INT invalid", "INT", "abc", types.Value{}, true},

		// Bigint tests
		{"bigint nil", "bigint", nil, types.Value{NativeType: "bigint", Type: types.Int64, Value: nil}, false},
		{"bigint int", "bigint", 123, types.Value{NativeType: "bigint", Type: types.Int64, Value: int64(123)}, false},
		{"bigint large", "bigint", 9223372036854775807, types.Value{NativeType: "bigint", Type: types.Int64, Value: int64(9223372036854775807)}, false},

		// Float tests
		{"float8 nil", "float8", nil, types.Value{NativeType: "float8", Type: types.Float64, Value: nil}, false},
		{"float8 float", "float8", 123.45, types.Value{NativeType: "float8", Type: types.Float64, Value: float64(123.45)}, false},
		{"float8 int", "float8", 123, types.Value{NativeType: "float8", Type: types.Float64, Value: float64(123)}, false},
		{"float8 string", "float8", "123.45", types.Value{NativeType: "float8", Type: types.Float64, Value: float64(123.45)}, false},
		{"float8 invalid", "float8", "abc", types.Value{}, true},
		{"numeric nil", "numeric", nil, types.Value{NativeType: "numeric", Type: types.Float64, Value: nil}, false},
		{"numeric float", "numeric", 123.45, types.Value{NativeType: "numeric", Type: types.Float64, Value: float64(123.45)}, false},
		{"numeric int", "numeric", 123, types.Value{NativeType: "numeric", Type: types.Float64, Value: float64(123)}, false},
		{"numeric string", "numeric", "123.45", types.Value{NativeType: "numeric", Type: types.Float64, Value: float64(123.45)}, false},
		{"numeric byte array", "numeric", []uint8{49, 50, 51, 46, 52, 53}, types.Value{NativeType: "numeric", Type: types.Float64, Value: float64(123.45)}, false},
		{"numeric invalid", "numeric", "abc", types.Value{}, true},

		// String tests
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
		{"timestamp with time zone nil", "timestamp with time zone", nil, types.Value{NativeType: "timestamp with time zone", Type: types.Timestamp, Value: nil}, false},
		{"timestamp with time zone time", "timestamp with time zone", testTime, types.Value{NativeType: "timestamp with time zone", Type: types.Timestamp, Value: testTime}, false},
		{"timestamptz nil", "timestamptz", nil, types.Value{NativeType: "timestamptz", Type: types.Timestamp, Value: nil}, false},
		{"timestamptz time", "timestamptz", testTime, types.Value{NativeType: "timestamptz", Type: types.Timestamp, Value: testTime}, false},

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

// Test different time zones to ensure proper handling
func TestTimeZoneHandling(t *testing.T) {
	converter := Converter{}

	// Define times in different time zones that represent the same instant
	utcTime := time.Date(2025, 3, 28, 12, 0, 0, 0, time.UTC)

	// Create a time in the New York time zone
	nycLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("Skipping test because New York timezone not available")
	}
	nycTime := time.Date(2025, 3, 28, 8, 0, 0, 0, nycLoc) // 8 AM in NYC = 12 PM UTC

	// Test that both times are treated the same
	utcResult, err := converter.ConvertValue("timestamp with time zone", utcTime)
	assert.NoError(t, err)
	nycResult, err := converter.ConvertValue("timestamp with time zone", nycTime)
	assert.NoError(t, err)

	// Compare the unix timestamps (which are timezone agnostic)
	utcVal, ok := utcResult.Value.(time.Time)
	assert.True(t, ok)
	nycVal, ok := nycResult.Value.(time.Time)
	assert.True(t, ok)

	assert.Equal(t, utcVal.Unix(), nycVal.Unix(), "Times should represent the same instant")
}

// Test edge cases and boundary values
func TestEdgeCases(t *testing.T) {
	converter := Converter{}

	t.Run("Integer boundaries", func(t *testing.T) {
		// Max int32
		value, err := converter.ConvertValue("integer", 2147483647)
		assert.NoError(t, err)
		assert.Equal(t, int32(2147483647), value.Value)

		// Min int32
		value, err = converter.ConvertValue("integer", -2147483648)
		assert.NoError(t, err)
		assert.Equal(t, int32(-2147483648), value.Value)

		// Max int64
		value, err = converter.ConvertValue("bigint", 9223372036854775807)
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), value.Value)

		// Min int64
		value, err = converter.ConvertValue("bigint", -9223372036854775808)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9223372036854775808), value.Value)
	})

	t.Run("String edge cases", func(t *testing.T) {
		// Empty string
		value, err := converter.ConvertValue("varchar", "")
		assert.NoError(t, err)
		assert.Equal(t, "", value.Value)

		// Very long string
		longString := string(make([]byte, 1000))
		value, err = converter.ConvertValue("varchar", longString)
		assert.NoError(t, err)
		assert.Equal(t, longString, value.Value)
	})
}
