package provider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/provider/clickhouse"
)

func NewClickHouseTestData(t *testing.T) TestColumnData {
	t.Helper()
	now := time.Now().UTC()
	formattedTime := now.Format("2006-01-02 15:04:05")

	return TestColumnData{
		Columns: []TestColumn{
			{
				Name:           "int8_col",
				NativeType:     clickhouse.INT8,
				ExpectedGoType: fftypes.Int8,
				TestValue:      int8(42),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int8(42), actual.(int8), "Int8 value mismatch")
				},
			},
			{
				Name:           "int16_col",
				NativeType:     clickhouse.INT16,
				ExpectedGoType: fftypes.Int16,
				TestValue:      int16(16384),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int16(16384), actual.(int16), "Int16 value mismatch")
				},
			},
			{
				Name:           "int32_col",
				NativeType:     clickhouse.INT32,
				ExpectedGoType: fftypes.Int32,
				TestValue:      int32(2147483647),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int32(2147483647), actual.(int32), "Int32 value mismatch")
				},
			},
			{
				Name:           "int64_col",
				NativeType:     clickhouse.INT64,
				ExpectedGoType: fftypes.Int64,
				TestValue:      int64(9223372036854775807),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int64(9223372036854775807), actual.(int64), "Int64 value mismatch")
				},
			},
			{
				Name:           "uint8_col",
				NativeType:     clickhouse.UINT8,
				ExpectedGoType: fftypes.UInt8,
				TestValue:      uint8(255),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, uint8(255), actual.(uint8), "UInt8 value mismatch")
				},
			},
			{
				Name:           "uint16_col",
				NativeType:     clickhouse.UINT16,
				ExpectedGoType: fftypes.UInt16,
				TestValue:      uint16(65535),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, uint16(65535), actual.(uint16), "UInt16 value mismatch")
				},
			},
			{
				Name:           "uint32_col",
				NativeType:     clickhouse.UINT32,
				ExpectedGoType: fftypes.UInt32,
				TestValue:      uint32(4294967295),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, uint32(4294967295), actual.(uint32), "UInt32 value mismatch")
				},
			},
			{
				Name:           "uint64_col",
				NativeType:     clickhouse.UINT64,
				ExpectedGoType: fftypes.UInt64,
				TestValue:      uint64(18446744073709551615),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, uint64(18446744073709551615), actual.(uint64), "UInt64 value mismatch")
				},
			},
			{
				Name:           "float32_col",
				NativeType:     clickhouse.FLOAT32,
				ExpectedGoType: fftypes.Float32,
				TestValue:      float32(3.14159),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.InDelta(t, float32(3.14159), actual.(float32), 0.0001, "Float32 value mismatch")
				},
			},
			{
				Name:           "float64_col",
				NativeType:     clickhouse.FLOAT64,
				ExpectedGoType: fftypes.Float64,
				TestValue:      float64(2.71828182845904),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.InDelta(t, float64(2.71828182845904), actual.(float64), 0.0001, "Float64 value mismatch")
				},
			},
			{
				Name:           "string_col",
				NativeType:     clickhouse.STRING,
				ExpectedGoType: fftypes.String,
				TestValue:      "string value",
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, "string value", actual.(string), "String value mismatch")
				},
			},
			{
				Name:           "datetime64_col",
				NativeType:     clickhouse.DATETIME64, // Millisecond precision
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual any) {
					datetimeFmt := "2006-01-02 15:04:05.999999999"

					actualTime, ok := actual.(time.Time)
					assert.True(t, ok, "DateTime64 not converted to time.Time")
					expectedTime, err := time.Parse(datetimeFmt, formattedTime)
					assert.NoError(t, err)
					assert.Equal(t, expectedTime.Unix(), actualTime.Unix(), "DateTime64 value mismatch")
				},
			},
			{
				Name:           "nullable_int_col",
				NativeType:     clickhouse.NewNullableType(clickhouse.INT32),
				ExpectedGoType: fftypes.Int32,
				TestValue:      42,
				VerifyFunc: func(t *testing.T, actual any) {
					if actual == nil {
						assert.Fail(t, "Expected non-nil value for nullable column")
					} else {
						assert.Equal(t, int32(42), actual.(int32), "Nullable(Int32) value mismatch")
					}
				},
			},
			{
				Name:           "nullable_string_col",
				NativeType:     clickhouse.NewNullableType(clickhouse.STRING),
				ExpectedGoType: fftypes.String,
				TestValue:      "nullable string",
				VerifyFunc: func(t *testing.T, actual any) {
					if actual == nil {
						assert.Fail(t, "Expected non-nil value for nullable column")
					} else {
						assert.Equal(t, "nullable string", actual.(string), "Nullable(String) value mismatch")
					}
				},
			},
		},
	}
}

func TestNativeTypeConversions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Configure test environment
	test := getConfiguredClickHouseTester(t)
	writableTester, ok := test.storeTester.(OfflineSqlStoreWriteableDatasetTester)
	require.True(t, ok, "Store tester does not support writable datasets")

	// Initialize our test data structure and convert it to new format
	chTestData := NewClickHouseTestData(t)

	// Run the common type conversion test
	TestDatabaseTypeConversions(t, writableTester, chTestData)
}
