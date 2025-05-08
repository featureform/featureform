package dataset

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/location"
)

func TestSqlDatasetQueryConstruction(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Setup test data
	testSchema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "id", NativeType: "integer", Type: types.Int},
			{Name: "name", NativeType: "varchar", Type: types.String},
			{Name: "special; column--", NativeType: "varchar", Type: types.String}, // Test sanitization
		},
	}

	// Mock converter - can be replaced with actual implementation or a more detailed mock
	mockConverter := &mockIntegerValueConverter{}

	testCases := []struct {
		name           string
		limit          int
		expectedQuery  string
		setupMockFn    func(mock sqlmock.Sqlmock)
		locationSchema string
		locationTable  string
	}{
		{
			name:           "No Limit Query",
			limit:          -1,
			locationSchema: "test_schema",
			locationTable:  "test_table",
			setupMockFn: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name", "special; column--"}
				mock.ExpectQuery(`SELECT "id", "name", "special; column--" FROM "test_schema"."test_table"`).
					WillReturnRows(sqlmock.NewRows(columns))
			},
		},
		{
			name:           "With Limit Query",
			limit:          10,
			locationSchema: "test_schema",
			locationTable:  "test_table",
			setupMockFn: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name", "special; column--"}
				mock.ExpectQuery(`SELECT "id", "name", "special; column--" FROM "test_schema"."test_table" LIMIT 10`).
					WillReturnRows(sqlmock.NewRows(columns))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup location
			loc := location.NewSQLLocationFromParts("", tc.locationSchema, tc.locationTable)

			// Setup mock expectations
			tc.setupMockFn(mock)

			// Create dataset
			ds, err := NewSqlDataset(db, loc, testSchema, mockConverter, tc.limit)
			require.NoError(t, err)

			// Call the method that builds and executes the query
			ctx := context.Background()
			iterator, err := ds.Iterator(ctx, 0)

			// Verify results
			assert.NoError(t, err)
			assert.NotNil(t, iterator)

			// Verify all expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Simple mock converter
type mockIntegerValueConverter struct{}

func (m *mockIntegerValueConverter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	return types.Value{
		NativeType: nativeType,
		Type:       types.Int,
		Value:      value,
	}, nil
}

func (m *mockIntegerValueConverter) GetType(nativeType types.NativeType) (types.ValueType, error) {
	switch nativeType {
	case "integer":
		return types.Int, nil
	default:
		return types.String, nil
	}
}
