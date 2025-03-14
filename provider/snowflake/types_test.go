package snowflake

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	harness "github.com/featureform/fftypes/testing"
	"github.com/featureform/logging"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
)

func TestSnowflakeTypeConversions(t *testing.T) {
	// Define a common timestamp for datetime-related fields
	utcTime := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)

	// Define all test cases
	testCases := []harness.TypeConversionTestCase{
		{"INTEGER", types.Int32, int32(42), nil},
		{"BIGINT", types.Int64, int64(9223372036854775807), nil},
		{"SMALLINT", types.Int32, int32(32767), nil},
		{"NUMBER", types.Float64, float64(3.14159), nil},
		{"DECIMAL", types.Float64, float64(123.456), nil},
		{"NUMERIC", types.Float64, float64(99.9999), nil},
		{"FLOAT", types.Float32, float32(2.71828), nil},
		{"FLOAT4", types.Float32, float32(1.234), nil},
		{"FLOAT8", types.Float64, float64(5.678), nil},
		{"DOUBLE", types.Float64, float64(9.876), nil},
		{"DOUBLE PRECISION", types.Float64, float64(10.1010), nil},
		{"REAL", types.Float32, float32(6.283), nil},
		{"VARCHAR", types.String, "test string", nil},
		{"STRING", types.String, "another test string", nil},
		{"TEXT", types.String, "this is some longer text for testing", nil},
		{"CHAR", types.String, "X", nil},
		{"CHARACTER", types.String, "Y", nil},
		{"BOOLEAN", types.Bool, true, nil},
		{"DATE", types.Datetime, utcTime, nil},
		{"DATETIME", types.Datetime, utcTime, nil},
		{"TIME", types.Datetime, utcTime, nil},
		{"TIMESTAMP", types.Timestamp, utcTime, nil},     // Changed to Timestamp
		{"TIMESTAMP_LTZ", types.Timestamp, utcTime, nil}, // Changed to Timestamp
		{"TIMESTAMP_NTZ", types.Timestamp, utcTime, nil}, // Changed to Timestamp
		{"TIMESTAMP_TZ", types.Timestamp, utcTime, nil},  // Changed to Timestamp
	}

	// Create a function to build a dataset with all test cases
	createDataset := func(testCases []harness.TypeConversionTestCase) (dataset.Dataset, error) {
		// Create mock database
		db, mock, err := sqlmock.New()
		if err != nil {
			return nil, err
		}

		// First collect all column names and create schema
		var columns []string
		var schemaFields []types.ColumnSchema

		for _, tc := range testCases {
			columns = append(columns, tc.TypeName)
			schemaFields = append(schemaFields, types.ColumnSchema{
				Name:       types.ColumnName(tc.TypeName),
				NativeType: types.NativeType(tc.TypeName),
				Type:       tc.ExpectedType, // Important: Set the expected type in schema
			})
		}

		// Create mock rows with all columns defined
		rows := sqlmock.NewRows(columns)

		// Extract values for the row
		rowValues := make([]driver.Value, len(testCases))
		for i, tc := range testCases {
			rowValues[i] = tc.SampleDBValue
		}

		// Add values to the row
		rows.AddRow(rowValues...)

		// Set up the mock query expectation
		mock.ExpectQuery("^SELECT").WillReturnRows(rows)

		// Create schema and location
		schema := types.Schema{Fields: schemaFields}
		location := pl.NewSQLLocation("test_table")

		// Create the dataset with schema and sfConverter (not TypeMap)
		return dataset.NewSqlDataset(db, *location, schema, sfConverter, 1)
	}

	// Create a type map for the test harness
	// This maps Snowflake types to FFTypes value types
	typeMap := make(types.NativeToValueTypeMapper)
	for _, tc := range testCases {
		typeMap[types.NativeType(tc.TypeName)] = tc.ExpectedType
	}

	// Run the test suite with the correct type map
	harness.TypeMapTestSuite(t, typeMap, testCases, createDataset)
}

func TestTimestampTypeHandling(t *testing.T) {
	// Define times with different timezones
	utcTime := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)

	nycLoc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err, "Loading NYC timezone should not fail")
	nycTime := time.Date(2025, 3, 10, 8, 0, 0, 0, nycLoc) // Same instant as utcTime

	tokyoLoc, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err, "Loading Tokyo timezone should not fail")
	tokyoTime := time.Date(2025, 3, 10, 21, 0, 0, 0, tokyoLoc) // Same instant as utcTime

	// Create test cases
	testCases := []struct {
		name          string
		typeName      string
		inputTime     time.Time
		expectedEqual bool
	}{
		{"TIMESTAMP_UTC", "TIMESTAMP", utcTime, true},
		{"TIMESTAMP_NYC", "TIMESTAMP", nycTime, true},
		{"TIMESTAMP_TOKYO", "TIMESTAMP", tokyoTime, true},
		{"TIMESTAMP_LTZ_UTC", "TIMESTAMP_LTZ", utcTime, true},
		{"TIMESTAMP_LTZ_NYC", "TIMESTAMP_LTZ", nycTime, true},
		{"TIMESTAMP_NTZ_UTC", "TIMESTAMP_NTZ", utcTime, true},
		{"TIMESTAMP_TZ_UTC", "TIMESTAMP_TZ", utcTime, true},
		{"TIMESTAMP_TZ_NYC", "TIMESTAMP_TZ", nycTime, true},
	}

	ctx := logging.NewTestContext(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock database connection
			db, mock, err := sqlmock.New()
			require.NoError(t, err, "Creating mock DB should not fail")
			defer db.Close()

			// Create a schema with timestamp column
			schema := types.Schema{
				Fields: []types.ColumnSchema{
					{
						Name:       "timestamp_col",
						NativeType: types.NativeType(tc.typeName),
						Type:       types.Timestamp, // Changed to Timestamp
					},
				},
			}

			// Create location
			location := pl.NewSQLLocation("timestamp_test")

			// Setup mock response
			columns := []string{"timestamp_col"}
			rows := sqlmock.NewRows(columns).AddRow(tc.inputTime)
			mock.ExpectQuery("^SELECT").WillReturnRows(rows)

			// Create dataset
			ds, err := dataset.NewSqlDataset(
				db,
				*location,
				schema,
				sfConverter,
				1,
			)
			require.NoError(t, err, "Dataset creation should not fail")

			// Get iterator
			iter, err := ds.Iterator(ctx)
			require.NoError(t, err, "Iterator creation should not fail")

			// Get value
			require.True(t, iter.Next(), "Iterator should have at least one row")
			row := iter.Values()
			require.NoError(t, iter.Err(), "Iterator should not have errors")
			require.NotEmpty(t, row, "Row should not be empty")

			// Verify the timestamp value
			resultValue, ok := row[0].Value.(time.Time)
			require.True(t, ok, "Value should be a time.Time")

			// Compare with expected result
			if tc.expectedEqual {
				// First verify that the times are truly identical when compared in UTC
				utcFormat := time.RFC3339
				require.Equal(t, utcTime.UTC().Format(utcFormat), resultValue.UTC().Format(utcFormat),
					"Time values should be equivalent when compared in UTC")

				// Additional check for the timestamp itself
				require.Equal(t, utcTime.Unix(), resultValue.Unix(),
					"Unix timestamps should be equal")
			} else {
				require.NotEqual(t, utcTime.UTC(), resultValue.UTC(),
					"Time values should not be equivalent when compared in UTC")
			}

			// Check that there are no more rows
			require.False(t, iter.Next(), "Iterator should not have more rows")
		})
	}
}
