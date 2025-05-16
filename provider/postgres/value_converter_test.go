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
		{"integer", INTEGER, types.Int32, false},
		{"int", INT, types.Int32, false},

		// Bigint type
		{"bigint", BIGINT, types.Int64, false},

		// Float types
		{"float8", FLOAT8, types.Float64, false},
		{"numeric", NUMERIC, types.Float64, false},

		// String types
		{"varchar", VARCHAR, types.String, false},

		// Boolean type
		{"boolean", BOOLEAN, types.Bool, false},

		// Timestamp types
		{"timestamp with time zone", TIMESTAMP_WITH_TIME_ZONE, types.Timestamp, false},
		{"timestamptz", TIMESTAMPTZ, types.Timestamp, false},

		// Unsupported type
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
		nativeType types.NativeType
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
		{"INT nil", INT, nil, types.Value{NativeType: INT, Type: types.Int32, Value: nil}, false},
		{"INT int", INT, 123, types.Value{NativeType: INT, Type: types.Int32, Value: int32(123)}, false},
		{"INT float", INT, 123.45, types.Value{NativeType: INT, Type: types.Int32, Value: int32(123)}, false},
		{"INT string", INT, "123", types.Value{NativeType: INT, Type: types.Int32, Value: int32(123)}, false},
		{"INT invalid", INT, "abc", types.Value{}, true},

		// Bigint tests
		{"bigint nil", BIGINT, nil, types.Value{NativeType: BIGINT, Type: types.Int64, Value: nil}, false},
		{"bigint int", BIGINT, 123, types.Value{NativeType: BIGINT, Type: types.Int64, Value: int64(123)}, false},
		{"bigint large", BIGINT, 9223372036854775807, types.Value{NativeType: BIGINT, Type: types.Int64, Value: int64(9223372036854775807)}, false},

		// Float tests
		{"float8 nil", FLOAT8, nil, types.Value{NativeType: FLOAT8, Type: types.Float64, Value: nil}, false},
		{"float8 float", FLOAT8, 123.45, types.Value{NativeType: FLOAT8, Type: types.Float64, Value: float64(123.45)}, false},
		{"float8 int", FLOAT8, 123, types.Value{NativeType: FLOAT8, Type: types.Float64, Value: float64(123)}, false},
		{"float8 string", FLOAT8, "123.45", types.Value{NativeType: FLOAT8, Type: types.Float64, Value: float64(123.45)}, false},
		{"float8 invalid", FLOAT8, "abc", types.Value{}, true},
		{"numeric nil", NUMERIC, nil, types.Value{NativeType: NUMERIC, Type: types.Float64, Value: nil}, false},
		{"numeric float", NUMERIC, 123.45, types.Value{NativeType: NUMERIC, Type: types.Float64, Value: float64(123.45)}, false},
		{"numeric int", NUMERIC, 123, types.Value{NativeType: NUMERIC, Type: types.Float64, Value: float64(123)}, false},
		{"numeric string", NUMERIC, "123.45", types.Value{NativeType: NUMERIC, Type: types.Float64, Value: float64(123.45)}, false},
		{"numeric byte array", NUMERIC, []uint8{49, 50, 51, 46, 52, 53}, types.Value{NativeType: NUMERIC, Type: types.Float64, Value: float64(123.45)}, false},
		{"numeric invalid", NUMERIC, "abc", types.Value{}, true},

		// String tests
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
		{"timestamp with time zone nil", TIMESTAMP_WITH_TIME_ZONE, nil, types.Value{NativeType: TIMESTAMP_WITH_TIME_ZONE, Type: types.Timestamp, Value: nil}, false},
		{"timestamp with time zone time", TIMESTAMP_WITH_TIME_ZONE, testTime, types.Value{NativeType: TIMESTAMP_WITH_TIME_ZONE, Type: types.Timestamp, Value: testTime}, false},
		{"timestamptz nil", TIMESTAMPTZ, nil, types.Value{NativeType: TIMESTAMPTZ, Type: types.Timestamp, Value: nil}, false},
		{"timestamptz time", TIMESTAMPTZ, testTime, types.Value{NativeType: TIMESTAMPTZ, Type: types.Timestamp, Value: testTime}, false},

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
	utcResult, err := converter.ConvertValue(TIMESTAMP_WITH_TIME_ZONE, utcTime)
	assert.NoError(t, err)
	nycResult, err := converter.ConvertValue(TIMESTAMP_WITH_TIME_ZONE, nycTime)
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
		value, err := converter.ConvertValue(INTEGER, 2147483647)
		assert.NoError(t, err)
		assert.Equal(t, int32(2147483647), value.Value)

		// Min int32
		value, err = converter.ConvertValue(INTEGER, -2147483648)
		assert.NoError(t, err)
		assert.Equal(t, int32(-2147483648), value.Value)

		// Max int64
		value, err = converter.ConvertValue(BIGINT, 9223372036854775807)
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), value.Value)

		// Min int64
		value, err = converter.ConvertValue(BIGINT, -9223372036854775808)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9223372036854775808), value.Value)
	})

	t.Run("String edge cases", func(t *testing.T) {
		// Empty string
		value, err := converter.ConvertValue(VARCHAR, "")
		assert.NoError(t, err)
		assert.Equal(t, "", value.Value)

		// Very long string
		longString := string(make([]byte, 1000))
		value, err = converter.ConvertValue(VARCHAR, longString)
		assert.NoError(t, err)
		assert.Equal(t, longString, value.Value)
	})
}
