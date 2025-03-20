// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package bigquery

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

func TestBigQueryTypeConversions(t *testing.T) {
	utcTime := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)

	testCases := []typestesting.TypeConversionTestCase{
		{
			TypeName:      "INT64",
			ExpectedType:  types.Int64,
			SampleDBValue: int64(42),
		},
		{
			TypeName:      "INT64",
			ExpectedType:  types.Int64,
			SampleDBValue: int64(9223372036854775807), // Max int64
		},
		{
			TypeName:      "FLOAT64",
			ExpectedType:  types.Float64,
			SampleDBValue: float64(3.14159),
		},
		{
			TypeName:      "NUMERIC",
			ExpectedType:  types.Float64,
			SampleDBValue: float64(123.456),
		},
		{
			TypeName:      "BIGNUMERIC",
			ExpectedType:  types.Float64,
			SampleDBValue: float64(9999999.9999),
		},
		{
			TypeName:      "BOOL",
			ExpectedType:  types.Bool,
			SampleDBValue: true,
		},
		{
			TypeName:      "BOOL",
			ExpectedType:  types.Bool,
			SampleDBValue: false,
		},
		{
			TypeName:      "STRING",
			ExpectedType:  types.String,
			SampleDBValue: "test string",
		},
		{
			TypeName:      "STRING",
			ExpectedType:  types.String,
			SampleDBValue: "Special characters: àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ !@#$%^&*()",
		},
		{
			TypeName:      "DATE",
			ExpectedType:  types.Datetime,
			SampleDBValue: utcTime,
		},
		{
			TypeName:      "DATETIME",
			ExpectedType:  types.Datetime,
			SampleDBValue: utcTime,
		},
		{
			TypeName:      "TIME",
			ExpectedType:  types.Datetime,
			SampleDBValue: utcTime,
		},
		{
			TypeName:      "TIMESTAMP",
			ExpectedType:  types.Timestamp,
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
				//Type:       tc.ExpectedType,
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
		mock.ExpectQuery("^SELECT").WillReturnRows(rows)

		// Create schema
		schema := types.Schema{
			Fields: schemaFields,
		}

		// Create dataset with the schema and converter
		ds, err := dataset.NewSqlDataset(
			db,
			*location,
			schema,
			bqConverter,
			1, // Limit to 1 row
		)

		return ds, err
	}

	typestesting.TypeMapTestSuite(t, testCases, createDataset)
}
