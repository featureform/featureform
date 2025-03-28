// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package snowflake

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
		{"INTEGER", "INTEGER", types.Int32, false},
		{"SMALLINT", "SMALLINT", types.Int32, false},
		{"BIGINT", "BIGINT", types.Int64, false},

		// Floating point types
		{"FLOAT", "FLOAT", types.Float32, false},
		{"FLOAT4", "FLOAT4", types.Float32, false},
		{"REAL", "REAL", types.Float32, false},
		{"NUMBER", "NUMBER", types.Float64, false},
		{"DECIMAL", "DECIMAL", types.Float64, false},
		{"NUMERIC", "NUMERIC", types.Float64, false},
		{"FLOAT8", "FLOAT8", types.Float64, false},
		{"DOUBLE", "DOUBLE", types.Float64, false},
		{"DOUBLE PRECISION", "DOUBLE PRECISION", types.Float64, false},

		// String types
		{"VARCHAR", "VARCHAR", types.String, false},
		{"STRING", "STRING", types.String, false},
		{"TEXT", "TEXT", types.String, false},
		{"CHAR", "CHAR", types.String, false},
		{"CHARACTER", "CHARACTER", types.String, false},

		// Boolean type
		{"BOOLEAN", "BOOLEAN", types.Bool, false},

		// Date/Time types
		{"DATE", "DATE", types.Datetime, false},
		{"DATETIME", "DATETIME", types.Datetime, false},
		{"TIME", "TIME", types.Datetime, false},
		{"TIMESTAMP", "TIMESTAMP", types.Timestamp, false},
		{"TIMESTAMP_LTZ", "TIMESTAMP_LTZ", types.Timestamp, false},
		{"TIMESTAMP_NTZ", "TIMESTAMP_NTZ", types.Timestamp, false},
		{"TIMESTAMP_TZ", "TIMESTAMP_TZ", types.Timestamp, false},

		// Unsupported type
		{"UNSUPPORTED", "UNSUPPORTED", nil, true},
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
		{"INTEGER nil", "INTEGER", nil, types.Value{NativeType: "INTEGER", Type: types.Int32, Value: nil}, false},
		{"INTEGER int", "INTEGER", 123, types.Value{NativeType: "INTEGER", Type: types.Int32, Value: int32(123)}, false},
		{"INTEGER float", "INTEGER", 123.45, types.Value{NativeType: "INTEGER", Type: types.Int32, Value: int32(123)}, false},
		{"INTEGER string", "INTEGER", "123", types.Value{NativeType: "INTEGER", Type: types.Int32, Value: int32(123)}, false},
		{"INTEGER invalid", "INTEGER", "abc", types.Value{}, true},

		{"SMALLINT nil", "SMALLINT", nil, types.Value{NativeType: "SMALLINT", Type: types.Int32, Value: nil}, false},
		{"SMALLINT int", "SMALLINT", 123, types.Value{NativeType: "SMALLINT", Type: types.Int32, Value: int32(123)}, false},

		{"BIGINT nil", "BIGINT", nil, types.Value{NativeType: "BIGINT", Type: types.Int64, Value: nil}, false},
		{"BIGINT int", "BIGINT", 123, types.Value{NativeType: "BIGINT", Type: types.Int64, Value: int64(123)}, false},
		{"BIGINT large", "BIGINT", 9223372036854775807, types.Value{NativeType: "BIGINT", Type: types.Int64, Value: int64(9223372036854775807)}, false},

		// Float tests
		{"FLOAT nil", "FLOAT", nil, types.Value{NativeType: "FLOAT", Type: types.Float32, Value: nil}, false},
		{"FLOAT float", "FLOAT", 123.45, types.Value{NativeType: "FLOAT", Type: types.Float32, Value: float32(123.45)}, false},
		{"FLOAT int", "FLOAT", 123, types.Value{NativeType: "FLOAT", Type: types.Float32, Value: float32(123)}, false},
		{"FLOAT string", "FLOAT", "123.45", types.Value{NativeType: "FLOAT", Type: types.Float32, Value: float32(123.45)}, false},
		{"FLOAT invalid", "FLOAT", "abc", types.Value{}, true},

		{"FLOAT4 nil", "FLOAT4", nil, types.Value{NativeType: "FLOAT4", Type: types.Float32, Value: nil}, false},
		{"FLOAT4 float", "FLOAT4", 123.45, types.Value{NativeType: "FLOAT4", Type: types.Float32, Value: float32(123.45)}, false},

		{"REAL nil", "REAL", nil, types.Value{NativeType: "REAL", Type: types.Float32, Value: nil}, false},
		{"REAL float", "REAL", 123.45, types.Value{NativeType: "REAL", Type: types.Float32, Value: float32(123.45)}, false},

		{"NUMBER nil", "NUMBER", nil, types.Value{NativeType: "NUMBER", Type: types.Float64, Value: nil}, false},
		{"NUMBER float", "NUMBER", 123.45, types.Value{NativeType: "NUMBER", Type: types.Float64, Value: float64(123.45)}, false},
		{"NUMBER int", "NUMBER", 123, types.Value{NativeType: "NUMBER", Type: types.Float64, Value: float64(123)}, false},
		{"NUMBER string", "NUMBER", "123.45", types.Value{NativeType: "NUMBER", Type: types.Float64, Value: float64(123.45)}, false},
		{"NUMBER invalid", "NUMBER", "abc", types.Value{}, true},

		{"DECIMAL nil", "DECIMAL", nil, types.Value{NativeType: "DECIMAL", Type: types.Float64, Value: nil}, false},
		{"DECIMAL float", "DECIMAL", 123.45, types.Value{NativeType: "DECIMAL", Type: types.Float64, Value: float64(123.45)}, false},

		{"NUMERIC nil", "NUMERIC", nil, types.Value{NativeType: "NUMERIC", Type: types.Float64, Value: nil}, false},
		{"NUMERIC float", "NUMERIC", 123.45, types.Value{NativeType: "NUMERIC", Type: types.Float64, Value: float64(123.45)}, false},

		{"FLOAT8 nil", "FLOAT8", nil, types.Value{NativeType: "FLOAT8", Type: types.Float64, Value: nil}, false},
		{"FLOAT8 float", "FLOAT8", 123.45, types.Value{NativeType: "FLOAT8", Type: types.Float64, Value: float64(123.45)}, false},

		{"DOUBLE nil", "DOUBLE", nil, types.Value{NativeType: "DOUBLE", Type: types.Float64, Value: nil}, false},
		{"DOUBLE float", "DOUBLE", 123.45, types.Value{NativeType: "DOUBLE", Type: types.Float64, Value: float64(123.45)}, false},

		{"DOUBLE PRECISION nil", "DOUBLE PRECISION", nil, types.Value{NativeType: "DOUBLE PRECISION", Type: types.Float64, Value: nil}, false},
		{"DOUBLE PRECISION float", "DOUBLE PRECISION", 123.45, types.Value{NativeType: "DOUBLE PRECISION", Type: types.Float64, Value: float64(123.45)}, false},

		// String tests
		{"VARCHAR nil", "VARCHAR", nil, types.Value{NativeType: "VARCHAR", Type: types.String, Value: nil}, false},
		{"VARCHAR string", "VARCHAR", "test", types.Value{NativeType: "VARCHAR", Type: types.String, Value: "test"}, false},
		{"VARCHAR int", "VARCHAR", 123, types.Value{NativeType: "VARCHAR", Type: types.String, Value: "123"}, false},
		{"VARCHAR float", "VARCHAR", 123.45, types.Value{NativeType: "VARCHAR", Type: types.String, Value: "123.45"}, false},

		{"STRING nil", "STRING", nil, types.Value{NativeType: "STRING", Type: types.String, Value: nil}, false},
		{"STRING string", "STRING", "test", types.Value{NativeType: "STRING", Type: types.String, Value: "test"}, false},

		{"TEXT nil", "TEXT", nil, types.Value{NativeType: "TEXT", Type: types.String, Value: nil}, false},
		{"TEXT string", "TEXT", "test", types.Value{NativeType: "TEXT", Type: types.String, Value: "test"}, false},

		{"CHAR nil", "CHAR", nil, types.Value{NativeType: "CHAR", Type: types.String, Value: nil}, false},
		{"CHAR string", "CHAR", "test", types.Value{NativeType: "CHAR", Type: types.String, Value: "test"}, false},

		{"CHARACTER nil", "CHARACTER", nil, types.Value{NativeType: "CHARACTER", Type: types.String, Value: nil}, false},
		{"CHARACTER string", "CHARACTER", "test", types.Value{NativeType: "CHARACTER", Type: types.String, Value: "test"}, false},

		// Boolean tests
		{"BOOLEAN nil", "BOOLEAN", nil, types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: nil}, false},
		{"BOOLEAN true", "BOOLEAN", true, types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: true}, false},
		{"BOOLEAN false", "BOOLEAN", false, types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: false}, false},
		{"BOOLEAN string true", "BOOLEAN", "true", types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: true}, false},
		{"BOOLEAN string false", "BOOLEAN", "false", types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: false}, false},
		{"BOOLEAN int 1", "BOOLEAN", 1, types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: true}, false},
		{"BOOLEAN int 0", "BOOLEAN", 0, types.Value{NativeType: "BOOLEAN", Type: types.Bool, Value: false}, false},
		{"BOOLEAN invalid", "BOOLEAN", "abc", types.Value{}, true},

		// Date/Time tests - these depend on the implementation of ConvertDatetime
		{"DATE nil", "DATE", nil, types.Value{NativeType: "DATE", Type: types.Datetime, Value: nil}, false},
		{"DATE time", "DATE", testTime, types.Value{NativeType: "DATE", Type: types.Datetime, Value: testTime}, false},

		{"DATETIME nil", "DATETIME", nil, types.Value{NativeType: "DATETIME", Type: types.Datetime, Value: nil}, false},
		{"DATETIME time", "DATETIME", testTime, types.Value{NativeType: "DATETIME", Type: types.Datetime, Value: testTime}, false},

		{"TIME nil", "TIME", nil, types.Value{NativeType: "TIME", Type: types.Datetime, Value: nil}, false},
		{"TIME time", "TIME", testTime, types.Value{NativeType: "TIME", Type: types.Datetime, Value: testTime}, false},

		// Timestamp tests
		{"TIMESTAMP nil", "TIMESTAMP", nil, types.Value{NativeType: "TIMESTAMP", Type: types.Timestamp, Value: nil}, false},
		{"TIMESTAMP time", "TIMESTAMP", testTime, types.Value{NativeType: "TIMESTAMP", Type: types.Timestamp, Value: testTime}, false},

		{"TIMESTAMP_LTZ nil", "TIMESTAMP_LTZ", nil, types.Value{NativeType: "TIMESTAMP_LTZ", Type: types.Timestamp, Value: nil}, false},
		{"TIMESTAMP_LTZ time", "TIMESTAMP_LTZ", testTime, types.Value{NativeType: "TIMESTAMP_LTZ", Type: types.Timestamp, Value: testTime}, false},

		{"TIMESTAMP_NTZ nil", "TIMESTAMP_NTZ", nil, types.Value{NativeType: "TIMESTAMP_NTZ", Type: types.Timestamp, Value: nil}, false},
		{"TIMESTAMP_NTZ time", "TIMESTAMP_NTZ", testTime, types.Value{NativeType: "TIMESTAMP_NTZ", Type: types.Timestamp, Value: testTime}, false},

		{"TIMESTAMP_TZ nil", "TIMESTAMP_TZ", nil, types.Value{NativeType: "TIMESTAMP_TZ", Type: types.Timestamp, Value: nil}, false},
		{"TIMESTAMP_TZ time", "TIMESTAMP_TZ", testTime, types.Value{NativeType: "TIMESTAMP_TZ", Type: types.Timestamp, Value: testTime}, false},

		// Unsupported type
		{"UNSUPPORTED nil", "UNSUPPORTED", nil, types.Value{}, true},
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
	utcResult, err := converter.ConvertValue("TIMESTAMP", utcTime)
	assert.NoError(t, err)
	nycResult, err := converter.ConvertValue("TIMESTAMP", nycTime)
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
		value, err := converter.ConvertValue("INTEGER", 2147483647)
		assert.NoError(t, err)
		assert.Equal(t, int32(2147483647), value.Value)

		// Min int32
		value, err = converter.ConvertValue("INTEGER", -2147483648)
		assert.NoError(t, err)
		assert.Equal(t, int32(-2147483648), value.Value)

		// Max int64
		value, err = converter.ConvertValue("BIGINT", 9223372036854775807)
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), value.Value)

		// Min int64
		value, err = converter.ConvertValue("BIGINT", -9223372036854775808)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9223372036854775808), value.Value)
	})

	t.Run("String edge cases", func(t *testing.T) {
		// Empty string
		value, err := converter.ConvertValue("VARCHAR", "")
		assert.NoError(t, err)
		assert.Equal(t, "", value.Value)

		// Very long string
		longString := string(make([]byte, 1000))
		value, err = converter.ConvertValue("VARCHAR", longString)
		assert.NoError(t, err)
		assert.Equal(t, longString, value.Value)
	})
}
