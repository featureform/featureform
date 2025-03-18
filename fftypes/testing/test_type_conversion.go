package testing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/dataset"
)

type TypeConversionTestCase struct {
	TypeName      string          // Database native type name
	ExpectedType  types.ValueType // Expected ValueType after conversion
	SampleDBValue interface{}     // Sample value from the database
	ExpectedValue interface{}     // Expected value after conversion (if different from SampleDBValue)
}

// DatasetCreator creates a dataset with all test columns
type DatasetCreator func(testCases []TypeConversionTestCase) (dataset.Dataset, error)

func TypeMapTestSuite(
	t *testing.T,
	testCases []TypeConversionTestCase,
	createDataset DatasetCreator,
) {
	ctx := logging.NewTestContext(t)

	//Test all columns at once
	t.Run("AllColumnsTest", func(t *testing.T) {
		// Create dataset with all columns
		ds, err := createDataset(testCases)
		require.NoError(t, err, "Dataset creation should not fail")

		// Get iterator
		iter, err := ds.Iterator(ctx)
		require.NoError(t, err, "Iterator creation should not fail")

		// Verify we have a row
		require.True(t, iter.Next(), "Iterator should return at least one row")
		row := iter.Values()
		require.NoError(t, iter.Err(), "Iterator should not have errors")

		// Get schema
		schema := iter.Schema()
		require.NoError(t, err, "Schema retrieval should not fail")

		// Verify number of columns matches number of test cases
		require.Equal(t, len(testCases), len(schema.Fields), "Schema should have the expected number of fields")
		require.Equal(t, len(testCases), len(row), "Row should have the expected number of columns")

		// Validate each column
		for i, tc := range testCases {
			t.Run(tc.TypeName, func(t *testing.T) {
				// Validate schema field
				assert.Equal(t, tc.TypeName, string(schema.Fields[i].NativeType),
					"Column %d: Native type in schema should match test case", i)

				// Validate row value
				value := row[i]

				// Make sure the value is not nil
				require.NotNil(t, value, "Column %d: Row value should not be nil", i)

				assert.Equal(t, tc.TypeName, string(value.NativeType),
					"Column %d: Native type in row should match test case", i)
				assert.Equal(t, tc.ExpectedType, value.Type,
					"Column %d: Value type in row should match expected type", i)

				if tc.ExpectedValue != nil {
					assert.Equal(t, tc.ExpectedValue, value.Value,
						"Column %d: Converted value should match expected value", i)
				} else if tc.ExpectedType == types.Datetime {
					// Special handling for datetime values
					expectedTime, ok1 := tc.SampleDBValue.(time.Time)
					actualTime, ok2 := value.Value.(time.Time)

					require.True(t, ok1, "Column %d: Expected value should be a time.Time", i)
					require.True(t, ok2, "Column %d: Actual value should be a time.Time", i)

					assert.Equal(t, expectedTime.UTC().Format(time.RFC3339), actualTime.UTC().Format(time.RFC3339),
						"Column %d: Time values should be equivalent when compared in UTC", i)
				} else {
					assert.Equal(t, tc.SampleDBValue, value.Value,
						"Column %d: Converted value should match sample value", i)
				}
			})
		}

		// Verify no more rows
		assert.False(t, iter.Next(), "Iterator should not return more than one row")
	})

	// Test NULL values for each type
	t.Run("NullValuesTest", func(t *testing.T) {
		// Create a copy of the test cases with NULL values
		nullTestCases := make([]TypeConversionTestCase, len(testCases))
		for i, tc := range testCases {
			nullTestCases[i] = TypeConversionTestCase{
				TypeName:      tc.TypeName,
				ExpectedType:  tc.ExpectedType,
				SampleDBValue: nil, // NULL value
			}
		}

		// Create dataset with NULL values
		ds, err := createDataset(nullTestCases)
		require.NoError(t, err, "NULL dataset creation should not fail")

		// Get iterator
		iter, err := ds.Iterator(ctx)
		require.NoError(t, err, "NULL iterator creation should not fail")

		// Verify we have a row
		require.True(t, iter.Next(), "NULL iterator should return at least one row", "error", iter.Err())
		row := iter.Values()
		require.NoError(t, iter.Err(), "NULL iterator should not have errors")

		// Verify number of columns matches number of test cases
		require.Equal(t, len(testCases), len(row), "NULL row should have the expected number of columns")

		// Validate each NULL column
		for i, tc := range testCases {
			t.Run("NULL_"+tc.TypeName, func(t *testing.T) {
				value := row[i]
				require.NotNil(t, value, "Column %d: NULL row value should not be nil", i)

				assert.Equal(t, tc.TypeName, string(value.NativeType),
					"Column %d: Native type for NULL should match original type", i)
				assert.Nil(t, value.Value,
					"Column %d: Value should be nil for NULL input", i)
			})
		}

		// Verify no more rows
		assert.False(t, iter.Next(), "NULL iterator should not return more rows")
	})
}
