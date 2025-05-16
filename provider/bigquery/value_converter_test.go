// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package bigquery

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
		expected  types.NativeType
		expectErr bool
	}{
		// Integer types
		{"INT64", "INT64", INT64, false},
		{"INTEGER", "INTEGER", INTEGER, false},
		{"BIGINT", "BIGINT", BIGINT, false},

		// Floating point types
		{"FLOAT64", "FLOAT64", FLOAT64, false},
		{"DECIMAL", "DECIMAL", DECIMAL, false},

		// String type
		{"STRING", "STRING", STRING, false},

		// Boolean type
		{"BOOL", "BOOL", BOOL, false},

		// Date/Time types
		{"DATE", "DATE", DATE, false},
		{"DATETIME", "DATETIME", DATETIME, false},
		{"TIME", "TIME", TIME, false},
		{"TIMESTAMP", "TIMESTAMP", TIMESTAMP, false},

		// Unsupported type
		{"UNSUPPORTED", "UNSUPPORTED", nil, true},
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
		nativeType types.NativeType
		expected   types.ValueType
		expectErr  bool
	}{
		// Integer types
		{"INT64", INT64, types.Int64, false},
		{"INTEGER", INTEGER, types.Int64, false},
		{"BIGINT", BIGINT, types.Int64, false},

		// Floating point types
		{"FLOAT64", FLOAT64, types.Float64, false},
		{"DECIMAL", DECIMAL, types.Float64, false},

		// String type
		{"STRING", STRING, types.String, false},

		// Boolean type
		{"BOOL", BOOL, types.Bool, false},

		// Date/Time types
		{"DATE", DATE, types.Datetime, false},
		{"DATETIME", DATETIME, types.Datetime, false},
		{"TIME", TIME, types.Datetime, false},
		{"TIMESTAMP", TIMESTAMP, types.Timestamp, false},

		// Unsupported type
		{"UNSUPPORTED", types.NativeTypeLiteral("UNSUPPORTED"), nil, true},
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
		{"INT64 nil", INT64, nil, types.Value{NativeType: INT64, Type: types.Int64, Value: nil}, false},
		{"INT64 int", INT64, 123, types.Value{NativeType: INT64, Type: types.Int64, Value: int64(123)}, false},
		{"INT64 large", INT64, int64(9223372036854775807), types.Value{NativeType: INT64, Type: types.Int64, Value: int64(9223372036854775807)}, false},
		{"INT64 float", INT64, 123.45, types.Value{NativeType: INT64, Type: types.Int64, Value: int64(123)}, false},
		{"INT64 string", INT64, "123", types.Value{NativeType: INT64, Type: types.Int64, Value: int64(123)}, false},
		{"INT64 invalid", INT64, "abc", types.Value{}, true},

		{"INTEGER nil", INTEGER, nil, types.Value{NativeType: INTEGER, Type: types.Int64, Value: nil}, false},
		{"INTEGER int", INTEGER, 123, types.Value{NativeType: INTEGER, Type: types.Int64, Value: int64(123)}, false},

		{"BIGINT nil", BIGINT, nil, types.Value{NativeType: BIGINT, Type: types.Int64, Value: nil}, false},
		{"BIGINT int", BIGINT, 123, types.Value{NativeType: BIGINT, Type: types.Int64, Value: int64(123)}, false},

		// Float tests
		{"FLOAT64 nil", FLOAT64, nil, types.Value{NativeType: FLOAT64, Type: types.Float64, Value: nil}, false},
		{"FLOAT64 float", FLOAT64, 123.45, types.Value{NativeType: FLOAT64, Type: types.Float64, Value: float64(123.45)}, false},
		{"FLOAT64 int", FLOAT64, 123, types.Value{NativeType: FLOAT64, Type: types.Float64, Value: float64(123)}, false},
		{"FLOAT64 string", FLOAT64, "123.45", types.Value{NativeType: FLOAT64, Type: types.Float64, Value: float64(123.45)}, false},
		{"FLOAT64 invalid", FLOAT64, "abc", types.Value{}, true},

		{"DECIMAL nil", DECIMAL, nil, types.Value{NativeType: DECIMAL, Type: types.Float64, Value: nil}, false},
		{"DECIMAL float", DECIMAL, 123.45, types.Value{NativeType: DECIMAL, Type: types.Float64, Value: float64(123.45)}, false},

		// String tests
		{"STRING nil", STRING, nil, types.Value{NativeType: STRING, Type: types.String, Value: nil}, false},
		{"STRING string", STRING, "test", types.Value{NativeType: STRING, Type: types.String, Value: "test"}, false},
		{"STRING int", STRING, 123, types.Value{NativeType: STRING, Type: types.String, Value: "123"}, false},
		{"STRING float", STRING, 123.45, types.Value{NativeType: STRING, Type: types.String, Value: "123.45"}, false},

		// Boolean tests
		{"BOOL nil", BOOL, nil, types.Value{NativeType: BOOL, Type: types.Bool, Value: nil}, false},
		{"BOOL true", BOOL, true, types.Value{NativeType: BOOL, Type: types.Bool, Value: true}, false},
		{"BOOL false", BOOL, false, types.Value{NativeType: BOOL, Type: types.Bool, Value: false}, false},
		{"BOOL string true", BOOL, "true", types.Value{NativeType: BOOL, Type: types.Bool, Value: true}, false},
		{"BOOL string false", BOOL, "false", types.Value{NativeType: BOOL, Type: types.Bool, Value: false}, false},
		{"BOOL int 1", BOOL, 1, types.Value{NativeType: BOOL, Type: types.Bool, Value: true}, false},
		{"BOOL int 0", BOOL, 0, types.Value{NativeType: BOOL, Type: types.Bool, Value: false}, false},
		{"BOOL invalid", BOOL, "abc", types.Value{}, true},

		// Date/Time tests
		{"DATE nil", DATE, nil, types.Value{NativeType: DATE, Type: types.Datetime, Value: nil}, false},
		{"DATE time", DATE, testTime, types.Value{NativeType: DATE, Type: types.Datetime, Value: testTime}, false},

		{"DATETIME nil", DATETIME, nil, types.Value{NativeType: DATETIME, Type: types.Datetime, Value: nil}, false},
		{"DATETIME time", DATETIME, testTime, types.Value{NativeType: DATETIME, Type: types.Datetime, Value: testTime}, false},

		{"TIME nil", TIME, nil, types.Value{NativeType: TIME, Type: types.Datetime, Value: nil}, false},
		{"TIME time", TIME, testTime, types.Value{NativeType: TIME, Type: types.Datetime, Value: testTime}, false},

		// Timestamp tests
		{"TIMESTAMP nil", TIMESTAMP, nil, types.Value{NativeType: TIMESTAMP, Type: types.Timestamp, Value: nil}, false},
		{"TIMESTAMP time", TIMESTAMP, testTime, types.Value{NativeType: TIMESTAMP, Type: types.Timestamp, Value: testTime}, false},

		// Unsupported type
		{"UNSUPPORTED nil", types.NativeTypeLiteral("UNSUPPORTED"), nil, types.Value{}, true},
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
	utcResult, err := converter.ConvertValue(TIMESTAMP, utcTime)
	assert.NoError(t, err)
	nycResult, err := converter.ConvertValue(TIMESTAMP, nycTime)
	assert.NoError(t, err)

	// Compare the unix timestamps (which are timezone agnostic)
	utcVal, ok := utcResult.Value.(time.Time)
	assert.True(t, ok)
	nycVal, ok := nycResult.Value.(time.Time)
	assert.True(t, ok)

	assert.Equal(t, utcVal.Unix(), nycVal.Unix(), "Times should represent the same instant")
}

func TestEdgeCases(t *testing.T) {
	converter := Converter{}

	t.Run("Integer boundaries", func(t *testing.T) {
		// Max int64
		value, err := converter.ConvertValue(INT64, 9223372036854775807)
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), value.Value)

		// Min int64
		value, err = converter.ConvertValue(INT64, -9223372036854775808)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9223372036854775808), value.Value)
	})

	t.Run("String edge cases", func(t *testing.T) {
		// Empty string
		value, err := converter.ConvertValue(STRING, "")
		assert.NoError(t, err)
		assert.Equal(t, "", value.Value)

		// Very long string
		longString := string(make([]byte, 1000))
		value, err = converter.ConvertValue(STRING, longString)
		assert.NoError(t, err)
		assert.Equal(t, longString, value.Value)
	})
}
