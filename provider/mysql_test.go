package provider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMySQLCastTableItemType(t *testing.T) {
	q := mySQLQueries{}

	// Prepare a fixed time value for testing.
	testTime := time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC)

	// Define test cases.
	testCases := []struct {
		name     string
		input    interface{}
		typeSpec interface{}
		expected interface{}
	}{
		{
			name:     "Nil input returns nil",
			input:    nil,
			typeSpec: mySqlInt,
			expected: nil,
		},
		{
			name:     "mySqlInt conversion",
			input:    int64(42),
			typeSpec: mySqlInt,
			expected: int32(42),
		},
		{
			name:     "mySqlBigInt conversion",
			input:    int64(42),
			typeSpec: mySqlBigInt,
			expected: 42,
		},
		{
			name:     "mySqlFloat conversion",
			input:    3.14,
			typeSpec: mySqlFloat,
			expected: 3.14,
		},
		{
			name:     "mySqlString conversion",
			input:    "hello",
			typeSpec: mySqlString,
			expected: "hello",
		},
		{
			name:     "mySqlBool conversion",
			input:    true,
			typeSpec: mySqlBool,
			expected: true,
		},
		{
			name:     "mySqlTimestamp conversion",
			input:    testTime,
			typeSpec: mySqlTimestamp,
			expected: testTime,
		},
		{
			name:     "Default case returns input unchanged",
			input:    "unchanged",
			typeSpec: "unknown",
			expected: "unchanged",
		},
	}

	// Run each test case as a subtest.
	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			result := q.castTableItemType(tc.input, tc.typeSpec)
			assert.Equal(t, tc.expected, result)
		})
	}
}
