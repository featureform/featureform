package provider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/provider/snowflake"
)

// NewSnowflakeTestData creates test column data for Snowflake type conversions
// using the common TestColumn format
func NewSnowflakeTestData(t *testing.T) TestColumnData {
	t.Helper()
	now := time.Now().UTC()
	formattedTime := now.Format("2006-01-02 15:04:05")

	return TestColumnData{
		Columns: []TestColumn{
			{
				Name:           "int_col",
				NativeType:     snowflake.INTEGER,
				ExpectedGoType: fftypes.Int32,
				TestValue:      42,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int32(42), actual.(int32), "Integer value mismatch")
				},
			},
			{
				Name:           "bigint_col",
				NativeType:     snowflake.BIGINT,
				ExpectedGoType: fftypes.Int64,
				TestValue:      int64(9223372036854775807),
				VerifyFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int64(9223372036854775807), actual.(int64), "Bigint value mismatch")
				},
			},
			{
				Name:           "smallint_col",
				NativeType:     snowflake.SMALLINT,
				ExpectedGoType: fftypes.Int32,
				TestValue:      int16(32767),
				VerifyFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int32(32767), actual.(int32), "Smallint value mismatch")
				},
			},
			//{
			//	Name:           "float_col",
			//	NativeType:     snowflake.FLOAT,
			//	ExpectedGoType: fftypes.Float32,
			//	TestValue:      3.14159,
			//	VerifyFunc: func(t *testing.T, actual interface{}) {
			//		assert.InDelta(t, 3.14159, actual.(float32), 0.0001, "Float value mismatch")
			//	},
			//},
			//{
			//	Name:           "float4_col",
			//	NativeType:     snowflake.FLOAT4,
			//	ExpectedGoType: fftypes.Float32,
			//	TestValue:      2.71828,
			//	VerifyFunc: func(t *testing.T, actual interface{}) {
			//		assert.InDelta(t, 2.71828, actual.(float32), 0.0001, "Float4 value mismatch")
			//	},
			//},
			//{
			//	Name:           "float8_col",
			//	NativeType:     snowflake.FLOAT8,
			//	ExpectedGoType: fftypes.Float64,
			//	TestValue:      1.61803,
			//	VerifyFunc: func(t *testing.T, actual interface{}) {
			//		assert.InDelta(t, 1.61803, actual.(float64), 0.0001, "Float8 value mismatch")
			//	},
			//},
			//{
			//	Name:           "double_col",
			//	NativeType:     snowflake.DOUBLE,
			//	ExpectedGoType: fftypes.Float64,
			//	TestValue:      2.99792458,
			//	VerifyFunc: func(t *testing.T, actual interface{}) {
			//		assert.InDelta(t, 2.99792458, actual.(float64), 0.0001, "Double value mismatch")
			//	},
			//},
			//{
			//	Name:           "real_col",
			//	NativeType:     snowflake.REAL,
			//	ExpectedGoType: fftypes.Float32,
			//	TestValue:      1.41421,
			//	VerifyFunc: func(t *testing.T, actual interface{}) {
			//		assert.InDelta(t, 1.41421, actual.(float32), 0.0001, "Real value mismatch")
			//	},
			//},
			{
				Name:           "text_col",
				NativeType:     snowflake.TEXT,
				ExpectedGoType: fftypes.String,
				TestValue:      "text string",
				VerifyFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, "text string", actual.(string), "Text value mismatch")
				},
			},
			{
				Name:           "bool_col",
				NativeType:     snowflake.BOOLEAN,
				ExpectedGoType: fftypes.Bool,
				TestValue:      true,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, true, actual.(bool), "Boolean value mismatch")
				},
			},
			{
				Name:           "date_col",
				NativeType:     snowflake.DATE,
				ExpectedGoType: fftypes.Datetime,
				// Use simple date format instead of CURRENT_DATE() function
				TestValue: now.Format("2006-01-02"),
				VerifyFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "DATE not converted to time.Time")
				},
			},
			{
				Name:           "datetime_col",
				NativeType:     snowflake.DATETIME,
				ExpectedGoType: fftypes.Datetime,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "DATETIME not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_col",
				NativeType:     snowflake.TIMESTAMP,
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_ltz_col",
				NativeType:     snowflake.TIMESTAMP_LTZ,
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP_LTZ not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_ntz_col",
				NativeType:     snowflake.TIMESTAMP_NTZ,
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP_NTZ not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_tz_col",
				NativeType:     snowflake.TIMESTAMP_TZ,
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP_TZ not converted to time.Time")
				},
			},
			//{
			//	Name:           "number_col",
			//	NativeType:     snowflake.NUMBER,
			//	ExpectedGoType: fftypes.Float64,
			//	TestValue:      123.456,
			//	VerifyFunc: func(t *testing.T, actual interface{}) {
			//		assert.InDelta(t, 123.456, actual.(float64), 0.0001, "NUMBER value mismatch")
			//	},
			//},
		},
	}
}

func TestSnowflakeTypeConversions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	sfOfflineSqlTest := getConfiguredSnowflakeTester(t)
	sfStoreTester := sfOfflineSqlTest.storeTester.(OfflineSqlStoreWriteableDatasetTester)

	// Initialize our test data structure
	sfTestData := NewSnowflakeTestData(t)

	// Run the common type conversion test
	TestDatabaseTypeConversions(t, sfStoreTester, sfTestData)
}
