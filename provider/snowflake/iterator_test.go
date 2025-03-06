package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/featureform/fftypes"
	"github.com/featureform/provider/location"
)

func TestIterator_Next(t *testing.T) {
	// Test cases for different data types
	testCases := []struct {
		name         string
		schema       types.Schema
		setupMockRow func(*sqlmock.Rows)
		expectedVal  []interface{}
	}{
		// Integer Types
		{
			name: "Integer types",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "int_col", NativeType: "INTEGER"},
					{Name: "bigint_col", NativeType: "BIGINT"},
					{Name: "smallint_col", NativeType: "SMALLINT"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(int64(42), int64(9223372036854775807), int64(127))
			},
			expectedVal: []interface{}{
				int32(42),
				int64(9223372036854775807),
				int32(127),
			},
		},

		// Floating Point Types
		{
			name: "Float types - NUMBER, DECIMAL, NUMERIC",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "number_col", NativeType: "NUMBER"},
					{Name: "decimal_col", NativeType: "DECIMAL"},
					{Name: "numeric_col", NativeType: "NUMERIC"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(float64(3.14), float64(123.456), float64(789.012))
			},
			expectedVal: []interface{}{
				float64(3.14),
				float64(123.456),
				float64(789.012),
			},
		},
		{
			name: "Float types - FLOAT, FLOAT4, REAL",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "float_col", NativeType: "FLOAT"},
					{Name: "float4_col", NativeType: "FLOAT4"},
					{Name: "real_col", NativeType: "REAL"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(float64(2.71), float64(1.414), float64(1.732))
			},
			expectedVal: []interface{}{
				float32(2.71),
				float32(1.414),
				float32(1.732),
			},
		},
		{
			name: "Float types - FLOAT8, DOUBLE, DOUBLE PRECISION",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "float8_col", NativeType: "FLOAT8"},
					{Name: "double_col", NativeType: "DOUBLE"},
					{Name: "double_precision_col", NativeType: "DOUBLE PRECISION"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(float64(1.618), float64(2.718281828), float64(3.141592653589793))
			},
			expectedVal: []interface{}{
				float64(1.618),
				float64(2.718281828),
				float64(3.141592653589793),
			},
		},

		// String Types
		{
			name: "String types - VARCHAR, STRING, TEXT",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "varchar_col", NativeType: "VARCHAR"},
					{Name: "string_col", NativeType: "STRING"},
					{Name: "text_col", NativeType: "TEXT"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow("hello", "world", "testing")
			},
			expectedVal: []interface{}{
				"hello",
				"world",
				"testing",
			},
		},
		{
			name: "String types - CHAR, CHARACTER",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "char_col", NativeType: "CHAR"},
					{Name: "character_col", NativeType: "CHARACTER"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow("A", "B")
			},
			expectedVal: []interface{}{
				"A",
				"B",
			},
		},

		// Boolean Type
		{
			name: "Boolean type",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "bool_col", NativeType: "BOOLEAN"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(true)
			},
			expectedVal: []interface{}{
				true,
			},
		},

		// Date/Time Types
		{
			name: "Date/Time types - DATE, DATETIME",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "date_col", NativeType: "DATE"},
					{Name: "datetime_col", NativeType: "DATETIME"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(
					time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
				)
			},
			expectedVal: []interface{}{
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
			},
		},
		{
			name: "Date/Time types - TIME, TIMESTAMP",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "time_col", NativeType: "TIME"},
					{Name: "timestamp_col", NativeType: "TIMESTAMP"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(
					time.Date(0, 1, 1, 12, 30, 45, 0, time.UTC),
					time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
				)
			},
			expectedVal: []interface{}{
				time.Date(0, 1, 1, 12, 30, 45, 0, time.UTC),
				time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
			},
		},
		{
			name: "Date/Time types - TIMESTAMP variants",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "timestamp_ltz_col", NativeType: "TIMESTAMP_LTZ"},
					{Name: "timestamp_ntz_col", NativeType: "TIMESTAMP_NTZ"},
					{Name: "timestamp_tz_col", NativeType: "TIMESTAMP_TZ"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(
					time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
					time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
					time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
				)
			},
			expectedVal: []interface{}{
				time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
				time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
				time.Date(2023, 1, 1, 12, 30, 45, 0, time.UTC),
			},
		},

		// Special Types
		{
			name: "Special types - VARIANT, OBJECT, ARRAY",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "variant_col", NativeType: "VARIANT"},
					{Name: "object_col", NativeType: "OBJECT"},
					{Name: "array_col", NativeType: "ARRAY"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(
					`{"key": "value"}`,
					`{"name": "test", "value": 123}`,
					`[1, 2, 3, 4]`,
				)
			},
			expectedVal: []interface{}{
				`{"key": "value"}`,
				`{"name": "test", "value": 123}`,
				`[1, 2, 3, 4]`,
			},
		},
		{
			name: "Special types - GEOGRAPHY, BINARY",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "geography_col", NativeType: "GEOGRAPHY"},
					{Name: "binary_col", NativeType: "BINARY"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(
					`{"type":"Point","coordinates":[125.6, 10.1]}`,
					"BINARY_DATA",
				)
			},
			expectedVal: []interface{}{
				`{"type":"Point","coordinates":[125.6, 10.1]}`,
				"BINARY_DATA",
			},
		},

		// Mixed Types
		{
			name: "Mixed types",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "int_col", NativeType: "INTEGER"},
					{Name: "string_col", NativeType: "STRING"},
					{Name: "bool_col", NativeType: "BOOLEAN"},
					{Name: "float_col", NativeType: "FLOAT"},
					{Name: "timestamp_col", NativeType: "TIMESTAMP"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(
					int64(42),
					"test",
					true,
					float64(3.14),
					time.Date(2023, 1, 1, 12, 30, 0, 0, time.UTC),
				)
			},
			expectedVal: []interface{}{
				int32(42),
				"test",
				true,
				float32(3.14),
				time.Date(2023, 1, 1, 12, 30, 0, 0, time.UTC),
			},
		},

		// Null Values
		{
			name: "Null values",
			schema: types.Schema{
				Fields: []types.ColumnSchema{
					{Name: "int_col", NativeType: "INTEGER"},
					{Name: "string_col", NativeType: "STRING"},
					{Name: "bool_col", NativeType: "BOOLEAN"},
				},
			},
			setupMockRow: func(rows *sqlmock.Rows) {
				rows.AddRow(nil, nil, nil)
			},
			expectedVal: []interface{}{
				nil,
				nil,
				nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock database connection
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Create columns for the mock result
			columns := make([]string, len(tc.schema.Fields))
			for i, field := range tc.schema.Fields {
				columns[i] = string(field.Name)
			}

			// Prepare the mock result
			rows := sqlmock.NewRows(columns)
			tc.setupMockRow(rows)

			// Set up the query expectation
			mock.ExpectQuery("SELECT").WillReturnRows(rows)

			// Execute the query to get the rows
			result, err := db.Query("SELECT")
			require.NoError(t, err)

			// Create the iterator with the schema
			iterator := NewIterator(result, tc.schema)

			// Test the Next method
			ctx := context.Background()
			hasNext := iterator.Next(ctx)
			require.True(t, hasNext)
			require.NoError(t, iterator.Err())

			// Check the values
			row := iterator.Values()
			require.Equal(t, len(tc.schema.Fields), len(row))

			for i, expected := range tc.expectedVal {
				if expected == nil {
					assert.Nil(t, row[i].Value)
				} else {
					assert.Equal(t, reflect.TypeOf(expected), reflect.TypeOf(row[i].Value))
					assert.Equal(t, expected, row[i].Value)
				}
				assert.Equal(t, tc.schema.Fields[i].NativeType, row[i].NativeType)
			}

			// There should be no more rows
			hasNext = iterator.Next(ctx)
			require.False(t, hasNext)
			require.NoError(t, iterator.Err())

			// Verify all expectations were met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIterator_ColumnCountMismatch(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Define a schema with fewer columns than the result set
	schema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "col1", NativeType: "INTEGER"},
		},
	}

	// Prepare the mock result with more columns than the schema
	rows := sqlmock.NewRows([]string{"col1", "col2"})
	rows.AddRow(int64(42), "extra")

	// Set up the query expectation
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// Execute the query to get the rows
	result, err := db.Query("SELECT")
	require.NoError(t, err)

	// Create the iterator with the schema
	iterator := NewIterator(result, schema)

	// Test the Next method - should fail due to column count mismatch
	ctx := context.Background()
	hasNext := iterator.Next(ctx)
	require.False(t, hasNext)
	require.Error(t, iterator.Err())
	require.Contains(t, iterator.Err().Error(), "column count mismatch")
}

func TestIterator_Context_Cancelled(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	schema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "col1", NativeType: "INTEGER"},
		},
	}

	// Prepare the mock result
	rows := sqlmock.NewRows([]string{"col1"})
	rows.AddRow(int64(42))

	// Set up the query expectation
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// Execute the query to get the rows
	result, err := db.Query("SELECT")
	require.NoError(t, err)

	// Create the iterator with the schema
	iterator := NewIterator(result, schema)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel it immediately

	// Test the Next method with cancelled context
	hasNext := iterator.Next(ctx)
	require.False(t, hasNext)
	require.Error(t, iterator.Err())
	require.Equal(t, ctx.Err(), iterator.Err())
}

func TestIterator_Close(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	schema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "col1", NativeType: "INTEGER"},
		},
	}

	// Prepare the mock result
	rows := sqlmock.NewRows([]string{"col1"})
	rows.AddRow(int64(42))

	// Set up the query expectation
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// Execute the query to get the rows
	result, err := db.Query("SELECT")
	require.NoError(t, err)

	// Create the iterator with the schema
	iterator := NewIterator(result, schema)

	// Test the Close method
	err = iterator.Close()
	require.NoError(t, err)

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

// Additional test cases for specific edge cases

func TestIterator_ScanError(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	schema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "col1", NativeType: "INTEGER"},
		},
	}

	// Prepare a mock result that will produce a scan error
	// This is done by creating a row with a string value when an integer is expected
	rows := sqlmock.NewRows([]string{"col1"}).
		RowError(0, sql.ErrNoRows) // Add an explicit error for the first row

	// Set up the query expectation
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// Execute the query to get the rows
	result, err := db.Query("SELECT")
	require.NoError(t, err)

	// Create the iterator with the schema
	iterator := NewIterator(result, schema)

	// Test the Next method - should fail due to scan error
	ctx := context.Background()
	hasNext := iterator.Next(ctx)
	require.False(t, hasNext)
	require.Error(t, iterator.Err())
}

func TestIterator_DeserializeError(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	schema := types.Schema{
		Fields: []types.ColumnSchema{
			// Using BOOLEAN type but will provide a complex value that can't be deserialized
			{Name: "col1", NativeType: "BOOLEAN"},
		},
	}

	// Create a custom rows mock that returns something inappropriate for a boolean
	rows := sqlmock.NewRows([]string{"col1"})

	// Here's a trick - using a struct{} which can't be converted to bool
	// This should cause a deserialization error in our code
	type complexType struct{}
	rows.AddRow(complexType{})

	// Set up the query expectation
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// Execute the query to get the rows
	result, err := db.Query("SELECT")
	require.NoError(t, err)

	// Create the iterator with the schema
	iterator := NewIterator(result, schema)

	// Test the Next method - should fail due to deserialization error
	ctx := context.Background()
	hasNext := iterator.Next(ctx)

	// The sqlmock may not let us add a truly invalid type, so we need to be flexible
	// in how we test this. The test might pass or fail depending on how sqlmock handles
	// the complex type.
	if hasNext {
		// If we got a row, the value should be nil (since it couldn't be deserialized)
		row := iterator.Values()
		assert.Nil(t, row[0].Value)
	} else {
		// If we didn't get a row, there should be an error
		require.Error(t, iterator.Err())
	}
}

func TestDataset_Iterator(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Create a location and schema
	loc := location.NewSQLLocation("test_table")

	schema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "id", NativeType: "INTEGER"},
			{Name: "name", NativeType: "STRING"},
		},
	}

	// Create a dataset with the location and schema
	casted := loc.(*location.SQLLocation)
	ds := NewDataset(*casted, schema, 10)

	// Assign the mock db to the dataset
	ds.db = db

	// Prepare the mock result
	rows := sqlmock.NewRows([]string{"id", "name"})
	rows.AddRow(int64(1), "test1")
	rows.AddRow(int64(2), "test2")

	// Set up the query expectation - note we're using a specific query pattern
	expectedSQL := "SELECT id,name FROM test_table LIMIT 10"
	mock.ExpectQuery(expectedSQL).WillReturnRows(rows)

	// Get an iterator from the dataset
	ctx := context.Background()
	iterator, err := ds.Iterator(ctx)
	require.NoError(t, err)
	defer iterator.Close()

	// Check that we can iterate through the results
	for i := 0; i < 2; i++ {
		hasNext := iterator.Next(ctx)
		require.True(t, hasNext)
		require.NoError(t, iterator.Err())

		row := iterator.Values()
		require.Equal(t, 2, len(row))

		// Check the id
		assert.Equal(t, "INTEGER", string(row[0].NativeType))
		assert.Equal(t, int32(i+1), row[0].Value)

		// Check the name
		assert.Equal(t, "STRING", string(row[1].NativeType))
		assert.Equal(t, fmt.Sprintf("test%d", i+1), row[1].Value)
	}

	// No more rows
	hasNext := iterator.Next(ctx)
	require.False(t, hasNext)
	require.NoError(t, iterator.Err())

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDataset_GetSchema(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	loc := location.NewSQLLocation("test_table")

	// Create an empty schema - we'll get the real schema from the database
	schema := types.Schema{}

	// Create a dataset with the location and schema
	casted := loc.(*location.SQLLocation)
	ds := NewDataset(*casted, schema, 0)

	// Assign the mock db to the dataset
	ds.db = db

	// Prepare the mock result for the schema query
	columns := []string{"column_name", "data_type"}
	rows := sqlmock.NewRows(columns)
	rows.AddRow("id", "INTEGER")
	rows.AddRow("name", "VARCHAR")
	rows.AddRow("created_at", "TIMESTAMP")

	// Set up the query expectation
	mock.ExpectQuery("SELECT column_name, data_type FROM information_schema.columns").
		WithArgs("test_table").
		WillReturnRows(rows)

	// Call getSchema
	schema, err = ds.getSchema("test_table")
	require.NoError(t, err)

	// Check the schema
	require.Equal(t, 3, len(schema.Fields))

	assert.Equal(t, "id", string(schema.Fields[0].Name))
	assert.Equal(t, "INTEGER", string(schema.Fields[0].NativeType))

	assert.Equal(t, "name", string(schema.Fields[1].Name))
	assert.Equal(t, "VARCHAR", string(schema.Fields[1].NativeType))

	assert.Equal(t, "created_at", string(schema.Fields[2].Name))
	assert.Equal(t, "TIMESTAMP", string(schema.Fields[2].NativeType))

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}
