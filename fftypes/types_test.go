package types_test

import (
	"testing"
	"time"

	types "github.com/featureform/fftypes"
	"github.com/stretchr/testify/assert"
)

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		value    types.Value
		expected string
	}{
		{"String value", types.Value{Type: types.String, Value: "do not redeem"}, "do not redeem"},
		{"Integer value", types.Value{Type: types.Int, Value: 42}, "42"},
		{"Boolean value", types.Value{Type: types.Bool, Value: true}, "true"},
		{"Null value", types.Value{Type: types.String, Value: nil, IsNull: true}, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			str, err := tc.value.ToString()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, str)
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		name       string
		value      types.Value
		expected   int
		expectsErr bool
	}{
		{"Valid int", types.Value{Type: types.Int, Value: 100}, 100, false},
		{"String to int", types.Value{Type: types.String, Value: "200"}, 200, false},
		{"Invalid string", types.Value{Type: types.String, Value: "abc"}, 0, true},
		{"Null value", types.Value{Type: types.Int, Value: nil, IsNull: true}, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.value.ToInt()
			if tc.expectsErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, res)
			}
		})
	}
}

func TestToFloat(t *testing.T) {
	tests := []struct {
		name       string
		value      types.Value
		expected   float64
		expectsErr bool
	}{
		{"Valid float", types.Value{Type: types.Float64, Value: 99.99}, 99.99, false},
		{"Integer to float", types.Value{Type: types.Int, Value: 42}, 42.0, false},
		{"String to float", types.Value{Type: types.String, Value: "3.14"}, 3.14, false},
		{"Invalid string", types.Value{Type: types.String, Value: "xyz"}, 0, true},
		{"Null value", types.Value{Type: types.Float64, Value: nil, IsNull: true}, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.value.ToFloat()
			if tc.expectsErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.InDelta(t, tc.expected, res, 0.0001)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		name       string
		value      types.Value
		expected   bool
		expectsErr bool
	}{
		{"Valid bool", types.Value{Type: types.Bool, Value: true}, true, false},
		{"String true", types.Value{Type: types.String, Value: "true"}, true, false},
		{"String false", types.Value{Type: types.String, Value: "false"}, false, false},
		{"Invalid string", types.Value{Type: types.String, Value: "yes"}, false, true},
		{"Null value", types.Value{Type: types.Bool, Value: nil, IsNull: true}, false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.value.ToBool()
			if tc.expectsErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, res)
			}
		})
	}
}

func TestToTime(t *testing.T) {
	timeStr := "2023-12-25T15:04:05Z"
	expectedTime, _ := time.Parse(time.RFC3339, timeStr)

	tests := []struct {
		name       string
		value      types.Value
		expected   time.Time
		expectsErr bool
	}{
		{"Valid time", types.Value{Type: types.Timestamp, Value: expectedTime}, expectedTime, false},
		{"String to time", types.Value{Type: types.String, Value: timeStr}, expectedTime, false},
		{"Invalid string", types.Value{Type: types.String, Value: "invalid"}, time.Time{}, true},
		{"Null value", types.Value{Type: types.Timestamp, Value: nil, IsNull: true}, time.Time{}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.value.ToTime()
			if tc.expectsErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, res)
			}
		})
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		name     string
		value    types.Value
		expected bool
	}{
		{"String value", types.Value{Type: types.String, Value: "do not redeem"}, false},
		{"Integer value", types.Value{Type: types.Int, Value: 0}, true},
		{"Int64 value", types.Value{Type: types.Int64, Value: int64(40)}, true},
		{"Float64 value", types.Value{Type: types.Float64, Value: 17.38}, true},
		{"Boolean value", types.Value{Type: types.Bool, Value: true}, false},
		{"Timestamp value", types.Value{Type: types.Timestamp, Value: "2024-01-01T12:00:00Z"}, false},
		{"Datetime value", types.Value{Type: types.Datetime, Value: "2024-01-01 12:00:00"}, false},
		{"Null value", types.Value{Type: types.String, Value: nil, IsNull: true}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.value.IsNumeric())
		})
	}
}

func TestIsText(t *testing.T) {
	tests := []struct {
		name     string
		value    types.Value
		expected bool
	}{
		{"String value", types.Value{Type: types.String, Value: "do not redeem"}, true},
		{"Integer value", types.Value{Type: types.Int, Value: 0}, false},
		{"Int64 value", types.Value{Type: types.Int64, Value: int64(40)}, false},
		{"Float64 value", types.Value{Type: types.Float64, Value: 17.38}, false},
		{"Boolean value", types.Value{Type: types.Bool, Value: true}, false},
		{"Timestamp value", types.Value{Type: types.Timestamp, Value: "2024-01-01T12:00:00Z"}, false},
		{"Datetime value", types.Value{Type: types.Datetime, Value: "2024-01-01 12:00:00"}, false},
		{"Null string value", types.Value{Type: types.String, Value: nil, IsNull: true}, true},
		{"Null int value", types.Value{Type: types.Int, Value: nil, IsNull: true}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.value.IsText())
		})
	}
}
