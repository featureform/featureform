package provider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/provider/bigquery"
)

func NewBigQueryTestData(t *testing.T) TestColumnData {
	t.Helper()
	now := time.Now().UTC()
	formattedTime := now.Format("2006-01-02 15:04:05")

	return TestColumnData{
		Columns: []TestColumn{
			{
				Name:           "int_col",
				NativeType:     bigquery.INTEGER,
				ExpectedGoType: fftypes.Int64,
				TestValue:      int64(42),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int64(42), actual.(int64), "INTEGER value mismatch")
				},
			},
			{
				Name:           "bigint_col",
				NativeType:     bigquery.BIGINT,
				ExpectedGoType: fftypes.Int64,
				TestValue:      int64(9223372036854775807),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int64(9223372036854775807), actual.(int64), "BIGINT value mismatch")
				},
			},
			{
				Name:           "float_col",
				NativeType:     bigquery.DECIMAL,
				ExpectedGoType: fftypes.Float64,
				TestValue:      float64(3.14159),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.InDelta(t, float64(3.14159), actual.(float64), 0.0001, "DECIMAL value mismatch")
				},
			},
			{
				Name:           "string_col",
				NativeType:     bigquery.STRING,
				ExpectedGoType: fftypes.String,
				TestValue:      "string value",
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, "string value", actual.(string), "STRING value mismatch")
				},
			},
			{
				Name:           "bool_col",
				NativeType:     bigquery.BOOL,
				ExpectedGoType: fftypes.Bool,
				TestValue:      true,
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, true, actual.(bool), "BOOL value mismatch")
				},
			},
			{
				Name:           "timestamp_col",
				NativeType:     bigquery.TIMESTAMP,
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual any) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP not converted to time.Time")
				},
			},
		},
	}
}

func TestBigQueryNativeTypeConversions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Configure test environment
	test := getConfiguredBigQueryTester(t)
	writableTester, ok := test.storeTester.(OfflineSqlStoreWriteableDatasetTester)
	require.True(t, ok, "Store tester does not support writable datasets")

	// Initialize our test data structure
	bqTestData := NewBigQueryTestData(t)

	// Run the common type conversion test
	TestDatabaseTypeConversions(t, writableTester, bqTestData)
}
