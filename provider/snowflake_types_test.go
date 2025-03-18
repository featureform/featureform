package provider

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/snowflake"
)

// SnowflakeTestColumn represents a single test column with its Snowflake type,
// expected Go type, and test value
type SnowflakeTestColumn struct {
	Name             string
	SnowflakeType    string
	ExpectedGoType   types.ValueType
	TestValue        interface{}
	ExpectedTestFunc func(t *testing.T, actual interface{})
}

// SnowflakeTestData encapsulates all test columns with their types and expected values
type SnowflakeTestData struct {
	Columns []SnowflakeTestColumn
}

// NewSnowflakeTestData creates a test data structure with all Snowflake types
func NewSnowflakeTestData(t *testing.T) *SnowflakeTestData {
	t.Helper()
	now := time.Now().UTC()
	formattedTime := now.Format("2006-01-02 15:04:05")

	return &SnowflakeTestData{
		Columns: []SnowflakeTestColumn{
			{
				Name:           "int_col",
				SnowflakeType:  "INTEGER",
				ExpectedGoType: types.Int32,
				TestValue:      42,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, float64(42), actual.(float64), "Integer value mismatch")
				},
			},
			{
				Name:           "bigint_col",
				SnowflakeType:  "BIGINT",
				ExpectedGoType: types.Int64,
				TestValue:      int64(9223372036854775807),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, float64(9223372036854775807), actual.(float64), "Bigint value mismatch")
				},
			},
			{
				Name:           "smallint_col",
				SnowflakeType:  "SMALLINT",
				ExpectedGoType: types.Int32,
				TestValue:      int16(32767),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, float64(32767), actual.(float64), "Smallint value mismatch")
				},
			},
			{
				Name:           "num_col",
				SnowflakeType:  "NUMBER(10,2)",
				ExpectedGoType: types.Float64,
				TestValue:      123.45,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 123.45, actual.(float64), 0.0001, "Number value mismatch")
				},
			},
			{
				Name:           "decimal_col",
				SnowflakeType:  "DECIMAL(18,6)",
				ExpectedGoType: types.Float64,
				TestValue:      123456.789012,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 123456.789012, actual.(float64), 0.0001, "Decimal value mismatch")
				},
			},
			{
				Name:           "numeric_col",
				SnowflakeType:  "NUMERIC(12,4)",
				ExpectedGoType: types.Float64,
				TestValue:      9876.5432,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 9876.5432, actual.(float64), 0.0001, "Numeric value mismatch")
				},
			},
			{
				Name:           "float_col",
				SnowflakeType:  "FLOAT",
				ExpectedGoType: types.Float32,
				TestValue:      3.14159,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 3.14159, actual.(float32), 0.0001, "Float value mismatch")
				},
			},
			{
				Name:           "float4_col",
				SnowflakeType:  "FLOAT4",
				ExpectedGoType: types.Float32,
				TestValue:      2.71828,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 2.71828, actual.(float32), 0.0001, "Float4 value mismatch")
				},
			},
			{
				Name:           "float8_col",
				SnowflakeType:  "FLOAT8",
				ExpectedGoType: types.Float32,
				TestValue:      1.61803,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 1.61803, actual.(float32), 0.0001, "Float8 value mismatch")
				},
			},
			{
				Name:           "double_col",
				SnowflakeType:  "DOUBLE",
				ExpectedGoType: types.Float32,
				TestValue:      2.99792458,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 2.99792458, actual.(float32), 0.0001, "Double value mismatch")
				},
			},
			{
				Name:           "real_col",
				SnowflakeType:  "REAL",
				ExpectedGoType: types.Float32,
				TestValue:      1.41421,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, 1.41421, actual.(float32), 0.0001, "Real value mismatch")
				},
			},
			{
				Name:           "string_col",
				SnowflakeType:  "VARCHAR(100)",
				ExpectedGoType: types.String,
				TestValue:      "varchar string",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, "varchar string", actual.(string), "String value mismatch")
				},
			},
			{
				Name:           "text_col",
				SnowflakeType:  "TEXT",
				ExpectedGoType: types.String,
				TestValue:      "text string",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, "text string", actual.(string), "Text value mismatch")
				},
			},
			{
				Name:           "char_col",
				SnowflakeType:  "CHAR(10)",
				ExpectedGoType: types.String,
				TestValue:      "char10    ",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, "char10    ", actual.(string), "Char value mismatch")
				},
			},
			{
				Name:           "bool_col",
				SnowflakeType:  "BOOLEAN",
				ExpectedGoType: types.Bool,
				TestValue:      true,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, true, actual.(bool), "Boolean value mismatch")
				},
			},
			{
				Name:           "date_col",
				SnowflakeType:  "DATE",
				ExpectedGoType: types.Datetime,
				TestValue:      "CURRENT_DATE()",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "DATE not converted to time.Time")
				},
			},
			{
				Name:           "datetime_col",
				SnowflakeType:  "DATETIME",
				ExpectedGoType: types.Datetime,
				TestValue:      formattedTime, // Current time formatted as string
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "DATETIME not converted to time.Time")
				},
			},
			{
				Name:           "time_col",
				SnowflakeType:  "TIME",
				ExpectedGoType: types.Datetime,
				TestValue:      "CURRENT_TIME()",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIME not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_col",
				SnowflakeType:  "TIMESTAMP",
				ExpectedGoType: types.Timestamp,
				TestValue:      "CURRENT_TIMESTAMP()",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_ltz_col",
				SnowflakeType:  "TIMESTAMP_LTZ",
				ExpectedGoType: types.Timestamp,
				TestValue:      "CURRENT_TIMESTAMP()",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP_LTZ not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_ntz_col",
				SnowflakeType:  "TIMESTAMP_NTZ",
				ExpectedGoType: types.Timestamp,
				TestValue:      "CURRENT_TIMESTAMP()",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP_NTZ not converted to time.Time")
				},
			},
			{
				Name:           "timestamp_tz_col",
				SnowflakeType:  "TIMESTAMP_TZ",
				ExpectedGoType: types.Timestamp,
				TestValue:      "CURRENT_TIMESTAMP()",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "TIMESTAMP_TZ not converted to time.Time")
				},
			},
		},
	}
}

// GenerateCreateTableSQL creates the SQL statement to create a table with all test columns
func (td *SnowflakeTestData) GenerateCreateTableSQL(tableName string) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	for i, col := range td.Columns {
		builder.WriteString(fmt.Sprintf("\t%s %s", col.Name, col.SnowflakeType))
		if i < len(td.Columns)-1 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}

	builder.WriteString(")")
	return builder.String()
}

// GenerateInsertSQL creates the SQL statement to insert a test row
func (td *SnowflakeTestData) GenerateInsertSQL(tableName string) string {
	var columnNames strings.Builder
	var valuesList strings.Builder

	for i, col := range td.Columns {
		columnNames.WriteString(col.Name)

		// Handle different value types
		switch v := col.TestValue.(type) {
		case string:
			// Check if this is a Snowflake function call (starts without quotes)
			if strings.HasPrefix(v, "CURRENT_") || strings.HasPrefix(v, "ARRAY_") {
				valuesList.WriteString(v)
			} else {
				// Regular string value
				valuesList.WriteString(fmt.Sprintf("'%s'", v))
			}
		case int, int16, int32, int64, float32, float64:
			valuesList.WriteString(fmt.Sprintf("%v", v))
		case bool:
			if v {
				valuesList.WriteString("TRUE")
			} else {
				valuesList.WriteString("FALSE")
			}
		default:
			valuesList.WriteString("NULL")
		}

		if i < len(td.Columns)-1 {
			columnNames.WriteString(", ")
			valuesList.WriteString(", ")
		}
	}

	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s", tableName, columnNames.String(), valuesList.String())
}

// BuildExpectedTypeMapping returns a map of column names to their expected Go types
func (td *SnowflakeTestData) BuildExpectedTypeMapping() map[string]types.ValueType {
	typeMapping := make(map[string]types.ValueType)
	for _, col := range td.Columns {
		typeMapping[strings.ToUpper(col.Name)] = col.ExpectedGoType
	}
	return typeMapping
}

// VerifyResults verifies that the returned values match the expected values
func (td *SnowflakeTestData) VerifyResults(t *testing.T, columnValues map[string]interface{}) {
	for _, col := range td.Columns {
		value, exists := columnValues[strings.ToUpper(col.Name)]
		assert.True(t, exists, fmt.Sprintf("Column %s not found in results", col.Name))
		if exists && col.ExpectedTestFunc != nil {
			col.ExpectedTestFunc(t, value)
		}
	}
}

func TestSnowflakeTypeConversions(t *testing.T) {
	// Skip test if not in integration test environment
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Initialize test database
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Creating Parent Database: %s\n", dbName)
	snowflakeConfig, err := getSnowflakeConfig(t, dbName)
	if err != nil {
		t.Fatalf("could not get snowflake config: %s", err)
	}
	if err := createSnowflakeDatabase(snowflakeConfig); err != nil {
		t.Fatalf("%v", err)
	}

	t.Cleanup(func() {
		t.Logf("Dropping Parent Database: %s\n", dbName)
		err := destroySnowflakeDatabase(snowflakeConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.SnowflakeOffline, snowflakeConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &snowflakeOfflineStoreTester{
		defaultDbName:         dbName,
		snowflakeOfflineStore: store.(*snowflakeOfflineStore),
	}

	err = offlineStoreTester.CreateSchema(snowflakeConfig.Database, "test_types")
	require.NoError(t, err, "Failed to create schema")

	// Initialize our test data structure
	testData := NewSnowflakeTestData(t)

	// Create a test table with all supported Snowflake types
	tableName := fmt.Sprintf("test_types_%s", strings.ToLower(uuid.NewString()[:8]))
	location := pl.NewSQLLocationFromParts(snowflakeConfig.Database, "test_types", tableName)
	sqlLocation := SanitizeSqlLocation(location.TableLocation())

	// Generate the create table SQL from our test data structure
	createTableQuery := testData.GenerateCreateTableSQL(sqlLocation)
	_, err = offlineStoreTester.db.Exec(createTableQuery)
	require.NoError(t, err, "Failed to create test table")

	// Generate the insert SQL from our test data structure
	insertQuery := testData.GenerateInsertSQL(sqlLocation)
	_, err = offlineStoreTester.db.Exec(insertQuery)
	require.NoError(t, err, "Failed to insert test data")

	// Connect to the table using our converter
	ds, err := dataset.NewSqlDatasetWithAutoSchema(
		offlineStoreTester.db,
		*location,
		snowflake.SfConverter,
		1, // Limit to the one row we inserted
	)
	require.NoError(t, err, "Failed to create dataset with auto schema")

	// Verify column types match our expectations
	expectedTypeMapping := testData.BuildExpectedTypeMapping()

	// Get an iterator to verify data conversion
	ctx := logging.NewTestContext(t)
	it, err := ds.Iterator(ctx)
	require.NoError(t, err, "Failed to create iterator")
	defer it.Close()

	// Check that we got our row
	require.True(t, it.Next(), "Failed to get row from iterator", "error", it.Err())

	// Get the values
	row := it.Values()

	// Verify column count
	require.Equal(t, len(expectedTypeMapping), len(row), "Unexpected number of columns in row")

	// Create a map of column names to their values for easier testing
	columnValues := make(map[string]interface{})
	schema := it.Schema()
	require.NoError(t, err, "Failed to get schema")
	for i, col := range schema.ColumnNames() {
		columnValues[col] = row[i].Value
	}

	// Verify each column value matches expected test conditions
	testData.VerifyResults(t, columnValues)

	// Make sure we don't have extra rows
	assert.False(t, it.Next(), "Iterator returned unexpected additional rows")
	assert.NoError(t, it.Err(), "Iterator encountered error")
}
