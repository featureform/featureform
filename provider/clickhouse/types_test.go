package clickhouse

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	types "github.com/featureform/fftypes"
	typestesting "github.com/featureform/fftypes/testing"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
)

func TestClickHouseTypeConversions(t *testing.T) {
	utcTime := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)

	testCases := []typestesting.TypeConversionTestCase{
		// Integer types (int32 group)
		//{
		//	TypeName:      "Int8",
		//	ExpectedType:  types.Int32,
		//	SampleDBValue: int8(42),
		//},
		//{
		//	TypeName:      "Int16",
		//	ExpectedType:  types.Int32,
		//	SampleDBValue: int16(1000),
		//},
		{
			TypeName:      "Int32",
			ExpectedType:  types.Int32,
			SampleDBValue: int32(100000),
		},
		//{
		//	TypeName:      "UInt8",
		//	ExpectedType:  types.Int32,
		//	SampleDBValue: uint8(200),
		//},
		//{
		//	TypeName:      "UInt16",
		//	ExpectedType:  types.Int32,
		//	SampleDBValue: uint16(50000),
		//},

		// Integer types (int64 group)
		{
			TypeName:      "Int64",
			ExpectedType:  types.Int64,
			SampleDBValue: int64(9223372036854775807), // Max int64
		},
		{
			TypeName:      "UInt32",
			ExpectedType:  types.Int64,
			SampleDBValue: uint32(4294967295), // Max uint32
		},
		{
			TypeName:      "UInt64",
			ExpectedType:  types.Int64,
			SampleDBValue: uint64(9223372036854775807), // Max int64 (max we can convert safely)
		},

		// Floating point types
		{
			TypeName:      "Float32",
			ExpectedType:  types.Float32,
			SampleDBValue: float32(3.14159),
		},
		{
			TypeName:      "Float64",
			ExpectedType:  types.Float64,
			SampleDBValue: float64(2.71828182845904),
		},

		// String types
		{
			TypeName:      "String",
			ExpectedType:  types.String,
			SampleDBValue: "test string",
		},
		{
			TypeName:      "FixedString",
			ExpectedType:  types.String,
			SampleDBValue: "fixed-width",
		},

		// Boolean type
		//{
		//	TypeName:      "Bool",
		//	ExpectedType:  types.Bool,
		//	SampleDBValue: uint8(1), // ClickHouse typically uses UInt8 for booleans
		//},
		//{
		//	TypeName:      "Bool",
		//	ExpectedType:  types.Bool,
		//	SampleDBValue: uint8(0),
		//},

		// Date/Time types
		{
			TypeName:      "Date",
			ExpectedType:  types.Datetime,
			SampleDBValue: utcTime,
		},
		{
			TypeName:      "DateTime",
			ExpectedType:  types.Datetime,
			SampleDBValue: utcTime,
		},
		{
			TypeName:      "DateTime64",
			ExpectedType:  types.Datetime,
			SampleDBValue: utcTime,
		},
	}

	// Create a dataset creator function that builds a multi-column dataset
	createDataset := func(testCases []typestesting.TypeConversionTestCase) (dataset.Dataset, error) {
		// Create a mock database connection
		db, mock, err := sqlmock.New()
		if err != nil {
			return nil, err
		}

		// Build schema fields and columns
		var columns []string
		var schemaFields []types.ColumnSchema

		// Create a unique name for each column even if type is the same
		for i, tc := range testCases {
			colName := tc.TypeName
			if i > 0 && tc.TypeName == testCases[i-1].TypeName {
				// Add a suffix for duplicate type names
				colName = tc.TypeName + "_" + string('A'+byte(i))
			}
			columns = append(columns, colName)

			// Important: Set both NativeType and Type in schema
			schemaFields = append(schemaFields, types.ColumnSchema{
				Name:       types.ColumnName(colName),
				NativeType: types.NativeType(tc.TypeName),
			})
		}

		// Create a row with all column values
		rows := sqlmock.NewRows(columns)
		rowValues := make([]driver.Value, len(testCases))

		// Populate row values
		for i, tc := range testCases {
			rowValues[i] = tc.SampleDBValue
		}

		// Add the row to the mock result
		rows.AddRow(rowValues...)

		// Create SQL location
		location := pl.NewSQLLocation("test_table")

		// Setup mock query expectations
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		// Create schema
		schema := types.Schema{
			Fields: schemaFields,
		}

		// Create dataset with the schema and converter
		ds, err := dataset.NewSqlDataset(
			db,
			*location,
			schema,
			chConverter,
			1, // Limit to 1 row
		)

		return ds, err
	}

	// Run the test suite with the multi-column approach
	typestesting.TypeMapTestSuite(t, testCases, createDataset)
}
