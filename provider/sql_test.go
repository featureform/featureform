package provider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultCastTableItemType(t *testing.T) {
	q := defaultOfflineSQLQueries{}

	type testCase struct {
		name        string
		input       interface{}
		targetType  interface{}
		expected    interface{}
		expectPanic bool
	}

	testCases := []testCase{
		{
			name:        "Nil input returns nil",
			input:       nil,
			targetType:  sfInt,
			expected:    nil,
			expectPanic: false,
		},
		{
			name:        "Valid integer casting",
			input:       "42",
			targetType:  sfInt,
			expected:    42,
			expectPanic: false,
		},
		{
			name:        "Valid integer casting with sfNumber",
			input:       "42",
			targetType:  sfNumber,
			expected:    42,
			expectPanic: false,
		},
		{
			name:        "Invalid integer casting returns original",
			input:       "not_an_int",
			targetType:  sfInt,
			expected:    "not_an_int",
			expectPanic: false,
		},
		{
			name:        "Valid float casting from string",
			input:       "3.14",
			targetType:  sfFloat,
			expected:    3.14,
			expectPanic: false,
		},
		{
			name:        "Invalid float casting returns original",
			input:       "not_a_float",
			targetType:  sfFloat,
			expected:    "not_a_float",
			expectPanic: false,
		},
		{
			name:        "Float casting when input is already float64",
			input:       2.71,
			targetType:  sfFloat,
			expected:    2.71,
			expectPanic: false,
		},
		{
			name:        "String casting",
			input:       "hello",
			targetType:  sfString,
			expected:    "hello",
			expectPanic: false,
		},
		{
			name:        "Valid boolean casting",
			input:       true,
			targetType:  sfBool,
			expected:    true,
			expectPanic: false,
		},
		{
			name:        "Invalid boolean casting should panic",
			input:       "true",
			targetType:  sfBool,
			expected:    nil, // Not used because we expect a panic.
			expectPanic: true,
		},
		{
			name:       "Timestamp casting returns UTC time",
			input:      time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC),
			targetType: sfTimestamp,
			expected:   time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC),
		},
		{
			name:        "Default case for unknown type",
			input:       "unknown",
			targetType:  "unknown_type",
			expected:    "unknown",
			expectPanic: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for input %v with type %v", tc.input, tc.targetType)
					}
				}()
			}
			result := q.castTableItemType(tc.input, tc.targetType)
			if !tc.expectPanic {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
