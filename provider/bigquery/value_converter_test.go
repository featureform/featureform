// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"

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
		{"INT64", "INT64", types.Int64, false},
		{"INTEGER", "INTEGER", types.Int64, false},
		{"BIGINT", "BIGINT", types.Int64, false},
		{"FLOAT64", "FLOAT64", types.Float64, false},
		{"DECIMAL", "DECIMAL", types.Float64, false},
		{"STRING", "STRING", types.String, false},
		{"BOOL", "BOOL", types.Bool, false},
		{"DATE", "DATE", types.Datetime, false},
		{"DATETIME", "DATETIME", types.Datetime, false},
		{"TIME", "TIME", types.Datetime, false},
		{"TIMESTAMP", "TIMESTAMP", types.Timestamp, false},
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

	tests := []struct {
		name       string
		nativeType types.NativeType
		value      interface{}
		expected   types.Value
		expectErr  bool
	}{
		// Integer tests
		{"INT64 nil", "INT64", nil, types.Value{NativeType: "INT64", Type: types.Int64, Value: nil}, false},
		{"INT64 int", "INT64", 123, types.Value{NativeType: "INT64", Type: types.Int64, Value: int64(123)}, false},
		{"INT64 float", "INT64", 123.45, types.Value{NativeType: "INT64", Type: types.Int64, Value: int64(123)}, false},
		{"INT64 string", "INT64", "123", types.Value{NativeType: "INT64", Type: types.Int64, Value: int64(123)}, false},
		{"INT64 invalid", "INT64", "abc", types.Value{}, true},

		// Float tests
		{"FLOAT64 nil", "FLOAT64", nil, types.Value{NativeType: "FLOAT64", Type: types.Float64, Value: nil}, false},
		{"FLOAT64 float", "FLOAT64", 123.45, types.Value{NativeType: "FLOAT64", Type: types.Float64, Value: float64(123.45)}, false},
		{"FLOAT64 int", "FLOAT64", 123, types.Value{NativeType: "FLOAT64", Type: types.Float64, Value: float64(123)}, false},
		{"FLOAT64 string", "FLOAT64", "123.45", types.Value{NativeType: "FLOAT64", Type: types.Float64, Value: float64(123.45)}, false},
		{"FLOAT64 invalid", "FLOAT64", "abc", types.Value{}, true},

		// String tests
		{"STRING nil", "STRING", nil, types.Value{NativeType: "STRING", Type: types.String, Value: nil}, false},
		{"STRING string", "STRING", "test", types.Value{NativeType: "STRING", Type: types.String, Value: "test"}, false},
		{"STRING int", "STRING", 123, types.Value{NativeType: "STRING", Type: types.String, Value: "123"}, false},
		{"STRING float", "STRING", 123.45, types.Value{NativeType: "STRING", Type: types.String, Value: "123.45"}, false},

		// Bool tests
		{"BOOL nil", "BOOL", nil, types.Value{NativeType: "BOOL", Type: types.Bool, Value: nil}, false},
		{"BOOL true", "BOOL", true, types.Value{NativeType: "BOOL", Type: types.Bool, Value: true}, false},
		{"BOOL false", "BOOL", false, types.Value{NativeType: "BOOL", Type: types.Bool, Value: false}, false},
		{"BOOL string true", "BOOL", "true", types.Value{NativeType: "BOOL", Type: types.Bool, Value: true}, false},
		{"BOOL string false", "BOOL", "false", types.Value{NativeType: "BOOL", Type: types.Bool, Value: false}, false},
		{"BOOL int 1", "BOOL", 1, types.Value{NativeType: "BOOL", Type: types.Bool, Value: true}, false},
		{"BOOL int 0", "BOOL", 0, types.Value{NativeType: "BOOL", Type: types.Bool, Value: false}, false},
		{"BOOL invalid", "BOOL", "abc", types.Value{}, true},

		// Datetime tests - these depend on the implementation of ConvertDatetime which we're not testing here
		{"DATE nil", "DATE", nil, types.Value{NativeType: "DATE", Type: types.Datetime, Value: nil}, false},
		{"DATETIME nil", "DATETIME", nil, types.Value{NativeType: "DATETIME", Type: types.Datetime, Value: nil}, false},
		{"TIME nil", "TIME", nil, types.Value{NativeType: "TIME", Type: types.Datetime, Value: nil}, false},

		// Timestamp tests - similar to datetime
		{"TIMESTAMP nil", "TIMESTAMP", nil, types.Value{NativeType: "TIMESTAMP", Type: types.Timestamp, Value: nil}, false},
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

func TestGetBigQueryType(t *testing.T) {
	tests := []struct {
		name      string
		valueType types.ValueType
		expected  bigquery.FieldType
		expectErr bool
	}{
		{"Int", types.Int, bigquery.IntegerFieldType, false},
		{"Int32", types.Int32, bigquery.IntegerFieldType, false},
		{"Int64", types.Int64, bigquery.IntegerFieldType, false},
		{"Float32", types.Float32, "FLOAT64", false},
		{"Float64", types.Float64, "FLOAT64", false},
		{"String", types.String, bigquery.StringFieldType, false},
		{"Bool", types.Bool, bigquery.BooleanFieldType, false},
		{"Timestamp", types.Timestamp, bigquery.TimestampFieldType, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldType, err := GetBigQueryType(tt.valueType)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, fieldType)
			}
		})
	}
}
