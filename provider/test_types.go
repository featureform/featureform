package provider

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
)

type TestColumn struct {
	// Name of the column
	Name string
	// Database-specific type name
	NativeType string
	// Expected Go type after conversion
	ExpectedGoType fftypes.ValueType
	// Test value to be inserted
	TestValue any
	// Function to verify the converted value
	VerifyFunc func(t *testing.T, actual interface{})
}

type TestColumnData struct {
	Columns []TestColumn
}

func (d *TestColumnData) ToSchema() fftypes.Schema {
	schema := fftypes.Schema{}
	for _, col := range d.Columns {
		field := fftypes.ColumnSchema{
			Name:       fftypes.ColumnName(col.Name),
			Type:       col.ExpectedGoType,
			NativeType: fftypes.NativeType(col.NativeType),
		}
		schema.Fields = append(schema.Fields, field)
	}
	return schema
}

func (d *TestColumnData) BuildExpectedTypeMapping() map[string]fftypes.ValueType {
	mapping := make(map[string]fftypes.ValueType)
	for _, col := range d.Columns {
		mapping[col.Name] = col.ExpectedGoType
	}
	return mapping
}

// TestDatabaseTypeConversions is a common test for database type conversions
func TestDatabaseTypeConversions(t *testing.T, tester OfflineSqlStoreWriteableDatasetTester, testData TestColumnData) {

	// Get test database name
	dbName := tester.GetTestDatabase()

	schemaName := fmt.Sprintf("test_types_schema_%s", strings.ToLower(uuid.NewString()[:8]))
	err := tester.CreateSchema(dbName, schemaName)
	require.NoError(t, err, "Failed to create schema")

	// Create table name with random suffix
	tableName := fmt.Sprintf("test_types_%s", strings.ToLower(uuid.NewString()[:8]))
	location := pl.NewSQLLocationFromParts(dbName, schemaName, tableName)

	// Create a dataset with test schema
	schema := testData.ToSchema()

	writeableDS, err := tester.CreateWritableDataset(location, schema)
	require.NoError(t, err, "Failed to create dataset")

	// Create test row data
	testRows := make([]fftypes.Row, 1)
	testRows[0] = make(fftypes.Row, len(testData.Columns))

	// Populate row with test values
	for i, col := range testData.Columns {
		testRows[0][i] = fftypes.Value{
			Type:       col.ExpectedGoType,
			NativeType: fftypes.NativeType(col.NativeType),
			Value:      col.TestValue,
		}
	}

	// Write test data
	ctx := logging.NewTestContext(t)
	err = writeableDS.WriteBatch(ctx, testRows)
	require.NoError(t, err, "Failed to write test data")

	// Get expected type mapping
	expectedTypeMapping := testData.BuildExpectedTypeMapping()

	// Test with iterator
	it, err := writeableDS.Iterator(ctx)
	require.NoError(t, err, "Failed to create iterator")
	defer it.Close()

	// Verify we have a row
	require.True(t, it.Next(), "Failed to get row from iterator with error: %v", it.Err())
	if it.Err() != nil {
		t.Fatalf("Iterator error: %v", it.Err())
	}

	// Get values and verify
	row := it.Values()
	require.Equal(t, len(expectedTypeMapping), len(row), "Unexpected number of columns in row")

	// Create map of column values
	columnValues := make(map[string]interface{})
	sch := it.Schema()
	for i, col := range sch.ColumnNames() {
		columnValues[col] = row[i].Value
	}

	// Verify each column value matches expectations
	for _, col := range testData.Columns {
		value, exists := columnValues[col.Name]
		require.True(t, exists, "Column %s not found in results", col.Name)
		if exists && col.VerifyFunc != nil {
			col.VerifyFunc(t, value)
		}
	}

	// Ensure no extra rows
	assert.False(t, it.Next(), "Iterator returned unexpected additional rows")
	assert.NoError(t, it.Err(), "Iterator encountered error")
}
