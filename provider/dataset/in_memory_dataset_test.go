package dataset

import (
	"testing"

	"github.com/stretchr/testify/require"

	pl "github.com/featureform/provider/location"
	"github.com/featureform/types"
)

func TestInMemoryDataset(t *testing.T) {
	// Create test data
	data := []types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 3}},
	}
	schema := types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)

	// Create dataset
	ds := NewInMemoryDataset(data, schema, location)

	// Create test case with factory for creating fresh datasets
	tc := DatasetTestCase{
		Dataset:        ds,
		ExpectedData:   data,
		ExpectedSchema: schema,
		Location:       location,
		DatasetFactory: func() Dataset {
			// Create a fresh copy of the dataset for tests that modify it
			return NewInMemoryDataset(data, schema, location)
		},
	}

	// Run the generic test suite
	RunDatasetTestSuite(t, tc)
}
