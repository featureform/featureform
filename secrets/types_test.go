package secrets

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertStringToType(t *testing.T) {
	t.Run("string conversions", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected string
			wantErr  bool
		}{
			{
				name:     "normal string",
				input:    "hello",
				expected: "hello",
			},
			{
				name:     "empty string",
				input:    "",
				expected: "",
			},
			{
				name:     "numeric string",
				input:    "123",
				expected: "123",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := convertStringToType[string](tt.input)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	t.Run("int conversions", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected int
			wantErr  bool
		}{
			{
				name:     "valid integer",
				input:    "123",
				expected: 123,
			},
			{
				name:     "zero",
				input:    "0",
				expected: 0,
			},
			{
				name:     "negative integer",
				input:    "-123",
				expected: -123,
			},
			{
				name:    "invalid integer",
				input:   "not an int",
				wantErr: true,
			},
			{
				name:    "decimal string",
				input:   "123.45",
				wantErr: true,
			},
			{
				name:    "empty string",
				input:   "",
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := convertStringToType[int](tt.input)
				if tt.wantErr {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), "error parsing")
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	t.Run("bool conversions", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected bool
			wantErr  bool
		}{
			{
				name:     "true value",
				input:    "true",
				expected: true,
			},
			{
				name:     "false value",
				input:    "false",
				expected: false,
			},
			{
				name:     "1 as true",
				input:    "1",
				expected: true,
			},
			{
				name:     "0 as false",
				input:    "0",
				expected: false,
			},
			{
				name:     "t as true",
				input:    "t",
				expected: true,
			},
			{
				name:     "f as false",
				input:    "f",
				expected: false,
			},
			{
				name:    "invalid bool",
				input:   "not a bool",
				wantErr: true,
			},
			{
				name:    "empty string",
				input:   "",
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := convertStringToType[bool](tt.input)
				if tt.wantErr {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), "error parsing")
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	t.Run("edge cases", func(t *testing.T) {
		t.Run("max int", func(t *testing.T) {
			input := "2147483647" // max int32
			result, err := convertStringToType[int](input)
			assert.NoError(t, err)
			assert.Equal(t, 2147483647, result)
		})

		t.Run("min int", func(t *testing.T) {
			input := "-2147483648" // min int32
			result, err := convertStringToType[int](input)
			assert.NoError(t, err)
			assert.Equal(t, -2147483648, result)
		})

		t.Run("bool case insensitive", func(t *testing.T) {
			tests := []string{"TRUE", "True", "true"}
			for _, input := range tests {
				result, err := convertStringToType[bool](input)
				assert.NoError(t, err)
				assert.True(t, result)
			}
		})
	})
}
