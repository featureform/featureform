package provider

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/clickhouse"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
)

type ClickHouseTestColumn struct {
	Name             string
	ClickHouseType   string
	ExpectedGoType   fftypes.ValueType
	TestValue        interface{}
	ExpectedTestFunc func(t *testing.T, actual interface{})
}

// ClickHouseTestData encapsulates all test columns with their types and expected values
type ClickHouseTestData struct {
	Columns []ClickHouseTestColumn
}

func (td *ClickHouseTestData) ToSchema() fftypes.Schema {
	schema := fftypes.Schema{}
	for _, col := range td.Columns {
		field := fftypes.ColumnSchema{
			Name:       fftypes.ColumnName(col.Name),
			Type:       col.ExpectedGoType,
			NativeType: fftypes.NativeType(col.ClickHouseType),
		}
		schema.Fields = append(schema.Fields, field)
	}
	return schema
}

func NewClickHouseTestData(t *testing.T) *ClickHouseTestData {
	t.Helper()
	now := time.Now().UTC()
	formattedTime := now.Format("2006-01-02 15:04:05")

	return &ClickHouseTestData{
		Columns: []ClickHouseTestColumn{
			{
				Name:           "int8_col",
				ClickHouseType: "Int8",
				ExpectedGoType: fftypes.Int8,
				TestValue:      int8(42),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int8(42), actual.(int8), "Int8 value mismatch")
				},
			},
			{
				Name:           "int16_col",
				ClickHouseType: "Int16",
				ExpectedGoType: fftypes.Int16,
				TestValue:      int16(16384),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int16(16384), actual.(int16), "Int16 value mismatch")
				},
			},
			{
				Name:           "int32_col",
				ClickHouseType: "Int32",
				ExpectedGoType: fftypes.Int32,
				TestValue:      int32(2147483647),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int32(2147483647), actual.(int32), "Int32 value mismatch")
				},
			},
			{
				Name:           "int64_col",
				ClickHouseType: "Int64",
				ExpectedGoType: fftypes.Int64,
				TestValue:      int64(9223372036854775807),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, int64(9223372036854775807), actual.(int64), "Int64 value mismatch")
				},
			},
			{
				Name:           "uint8_col",
				ClickHouseType: "UInt8",
				ExpectedGoType: fftypes.UInt8,
				TestValue:      uint8(255),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, uint8(255), actual.(uint8), "UInt8 value mismatch")
				},
			},
			{
				Name:           "uint16_col",
				ClickHouseType: "UInt16",
				ExpectedGoType: fftypes.UInt16,
				TestValue:      uint16(65535),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, uint16(65535), actual.(uint16), "UInt16 value mismatch")
				},
			},
			{
				Name:           "uint32_col",
				ClickHouseType: "UInt32",
				ExpectedGoType: fftypes.UInt32,
				TestValue:      uint32(4294967295),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, uint32(4294967295), actual.(uint32), "UInt32 value mismatch")
				},
			},
			{
				Name:           "uint64_col",
				ClickHouseType: "UInt64",
				ExpectedGoType: fftypes.UInt64,
				TestValue:      uint64(18446744073709551615),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, uint64(18446744073709551615), actual.(uint64), "UInt64 value mismatch")
				},
			},
			{
				Name:           "float32_col",
				ClickHouseType: "Float32",
				ExpectedGoType: fftypes.Float32,
				TestValue:      float32(3.14159),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, float32(3.14159), actual.(float32), 0.0001, "Float32 value mismatch")
				},
			},
			{
				Name:           "float64_col",
				ClickHouseType: "Float64",
				ExpectedGoType: fftypes.Float64,
				TestValue:      float64(2.71828182845904),
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.InDelta(t, float64(2.71828182845904), actual.(float64), 0.0001, "Float64 value mismatch")
				},
			},
			{
				Name:           "string_col",
				ClickHouseType: "String",
				ExpectedGoType: fftypes.String,
				TestValue:      "string value",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					assert.Equal(t, "string value", actual.(string), "String value mismatch")
				},
			},
			{
				Name:           "datetime64_col",
				ClickHouseType: "DateTime64(9)", // Millisecond precision
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "DateTime64 not converted to time.Time")
				},
			},
			{
				Name:           "nullable_int_col",
				ClickHouseType: "Nullable(Int32)",
				ExpectedGoType: fftypes.Int32,
				TestValue:      42,
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
					if actual == nil {
						assert.Fail(t, "Expected non-nil value for nullable column")
					} else {
						assert.Equal(t, int32(42), actual.(int32), "Nullable(Int32) value mismatch")
					}
				},
			},
			{
				Name:           "nullable_string_col",
				ClickHouseType: "Nullable(String)",
				ExpectedGoType: fftypes.String,
				TestValue:      "nullable string",
				ExpectedTestFunc: func(t *testing.T, actual interface{}) {
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

// GenerateCreateTableSQL creates the SQL statement to create a table with all test columns
func (td *ClickHouseTestData) GenerateCreateTableSQL(tableName string) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	for i, col := range td.Columns {
		builder.WriteString(fmt.Sprintf("\t%s %s", col.Name, col.ClickHouseType))
		if i < len(td.Columns)-1 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}

	builder.WriteString(") ENGINE = MergeTree() ORDER BY tuple()")
	return builder.String()
}

func (td *ClickHouseTestData) GenerateInsertSQL(tableName string) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName))

	// Column names
	columnNames := make([]string, len(td.Columns))
	for i, col := range td.Columns {
		columnNames[i] = col.Name
	}
	builder.WriteString(strings.Join(columnNames, ", "))
	builder.WriteString(") VALUES (")

	// Values
	values := make([]string, len(td.Columns))
	for i, col := range td.Columns {
		switch v := col.TestValue.(type) {
		case string:
			// Check if this is a function call (like toDate() or array literal)
			if strings.HasPrefix(v, "to") || strings.HasPrefix(v, "[") {
				values[i] = v
			} else {
				values[i] = fmt.Sprintf("'%s'", v)
			}
		case bool:
			if v {
				values[i] = "true"
			} else {
				values[i] = "false"
			}
		case nil:
			values[i] = "NULL"
		default:
			values[i] = fmt.Sprintf("%v", v)
		}
	}
	builder.WriteString(strings.Join(values, ", "))
	builder.WriteString(")")

	return builder.String()
}

// BuildExpectedTypeMapping builds a map of column names to expected Go types
func (td *ClickHouseTestData) BuildExpectedTypeMapping() map[string]fftypes.ValueType {
	mapping := make(map[string]fftypes.ValueType)
	for _, col := range td.Columns {
		mapping[col.Name] = col.ExpectedGoType
	}
	return mapping
}

// VerifyResults verifies that the column values match our expectations
func (td *ClickHouseTestData) VerifyResults(t *testing.T, columnValues map[string]interface{}) {
	for _, col := range td.Columns {
		value, exists := columnValues[col.Name]
		assert.True(t, exists, "Column %s not found in results", col.Name)
		if exists {
			col.ExpectedTestFunc(t, value)
		}
	}
}

func TestClickHouseTypeConversions(t *testing.T) {
	// Skip test if not in integration test environment
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Initialize test database
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Creating Parent Database: %s\n", dbName)
	clickhouseConfig, err := getClickHouseConfig(t)
	if err != nil {
		t.Fatalf("could not get clickhouse config: %s", err)
	}
	clickhouseConfig.Database = dbName
	clickhouseConfig.Port = 9000
	clickhouseConfig.Password = ""
	if err := createClickHouseDatabaseFromConfig(t, clickhouseConfig); err != nil {
		t.Fatalf("%v", err)
	}

	store, err := GetOfflineStore(pt.ClickHouseOffline, clickhouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &clickHouseOfflineStoreTester{
		defaultDbName:          dbName,
		clickHouseOfflineStore: store.(*clickHouseOfflineStore),
	}

	t.Cleanup(func() {
		offlineStoreTester.DropDatabase(dbName)
	})

	// Initialize our test data structure
	testData := NewClickHouseTestData(t)

	// Create a test table with all supported ClickHouse types
	tableName := fmt.Sprintf("test_types_%s", strings.ToLower(uuid.NewString()[:8]))
	location := pl.NewSQLLocationFromParts(clickhouseConfig.Database, "", tableName)
	sanitized := sanitizeClickHouseTableName(location.TableLocation())

	// Generate the create table SQL from our test data structure
	createTableQuery := testData.GenerateCreateTableSQL(sanitized)
	_, err = offlineStoreTester.db.Exec(createTableQuery)
	require.NoError(t, err, "Failed to create test table")

	// Generate the insert SQL from our test data structure
	insertQuery := testData.GenerateInsertSQL(sanitized)
	_, err = offlineStoreTester.db.Exec(insertQuery)
	require.NoError(t, err, "Failed to insert test data")

	// Connect to the table using our converter
	ds, err := dataset.NewSqlDataset(
		offlineStoreTester.db,
		*location,
		testData.ToSchema(),
		clickhouse.ChConverter,
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

	for i, col := range row {
		assert.Equal(t, expectedTypeMapping[it.Schema().ColumnNames()[i]], col.Type, "Column type mismatch")
	}

	// Create a map of column names to their values for easier testing
	columnValues := make(map[string]interface{})
	schema := it.Schema()
	for i, col := range schema.ColumnNames() {
		columnValues[col] = row[i].Value
	}

	// Verify each column value matches expected test conditions
	testData.VerifyResults(t, columnValues)

	// Make sure we don't have extra rows
	assert.False(t, it.Next(), "Iterator returned unexpected additional rows")
	assert.NoError(t, it.Err(), "Iterator encountered error")
}
