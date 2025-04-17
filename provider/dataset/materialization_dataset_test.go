package dataset

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
)

// createTestData creates test data with varied types
func createTestData() ([]types.Row, types.Schema) {
	now := time.Now()

	// Create schema
	schema := types.Schema{
		Fields: []types.ColumnSchema{
			{Name: "entity_id", NativeType: "string", Type: types.String},
			{Name: "feature_value", NativeType: "float64", Type: types.Float64},
			{Name: "timestamp", NativeType: "timestamp", Type: types.Timestamp},
		},
	}

	// Create data rows
	data := []types.Row{
		{
			types.Value{NativeType: "string", Type: types.String, Value: "entity1"},
			types.Value{NativeType: "float64", Type: types.Float64, Value: 123.45},
			types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: now.Add(-3 * time.Hour)},
		},
		{
			types.Value{NativeType: "string", Type: types.String, Value: "entity2"},
			types.Value{NativeType: "float64", Type: types.Float64, Value: 67.89},
			types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: now.Add(-2 * time.Hour)},
		},
		{
			types.Value{NativeType: "string", Type: types.String, Value: "entity3"},
			types.Value{NativeType: "float64", Type: types.Float64, Value: 42.0},
			types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: now.Add(-1 * time.Hour)},
		},
		{
			types.Value{NativeType: "string", Type: types.String, Value: "entity4"},
			types.Value{NativeType: "float64", Type: types.Float64, Value: 99.99},
			types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: now},
		},
		{
			types.Value{NativeType: "string", Type: types.String, Value: "entity5"},
			types.Value{NativeType: "float64", Type: types.Float64, Value: 101.5},
			types.Value{NativeType: "timestamp", Type: types.Timestamp, Value: now.Add(1 * time.Hour)},
		},
	}

	return data, schema
}

// createFeatureSchema creates a feature schema from a standard schema
func createFeatureSchema(schema types.Schema) types.FeaturesSchema {
	// Assume first column is entity, second is feature, third is timestamp
	return types.FeaturesSchema{
		EntityCol: schema.Fields[0],
		FeatureCols: []types.FeatureCol{
			{
				FeatureCol:   schema.Fields[1],
				TimestampCol: schema.Fields[2],
			},
		},
	}
}

func TestMaterializationDataset(t *testing.T) {
	testID := MaterializationID("test-materialization")
	testLocation := pl.NewSQLLocation("test-location")

	// Create test data
	data, schema := createTestData()
	featureSchema := createFeatureSchema(schema)

	testCases := []struct {
		name           string
		chunkSize      int64
		expectedChunks int
	}{
		{"SingleChunk", 10, 1},
		{"MultipleChunks", 2, 3},
		{"ExactlyDivisible", 5, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create base dataset with configurable chunk size
			baseDS := NewInMemoryDatasetWithChunkSize(data, schema, testLocation, tc.chunkSize)

			// Create materialization dataset
			matDS := NewMaterialization(baseDS, testID, featureSchema)

			// Test basic properties
			t.Run("BasicProperties", func(t *testing.T) {
				// Test ID
				assert.Equal(t, testID, matDS.ID())

				// Test Location
				assert.Equal(t, testLocation, matDS.Location())

				// Test FeatureSchema
				assert.Equal(t, featureSchema, matDS.FeatureSchema())

				// Test Schema
				assert.Equal(t, schema, matDS.Schema())
			})

			// Test size methods
			t.Run("Size", func(t *testing.T) {
				// Test Len
				size, err := matDS.Len()
				require.NoError(t, err)
				assert.Equal(t, int64(len(data)), size)

				// Test NumChunks
				chunks, err := matDS.NumChunks()
				require.NoError(t, err)
				assert.Equal(t, tc.expectedChunks, chunks)
			})

			// Test iterator
			t.Run("Iterator", func(t *testing.T) {
				ctx := logging.NewTestContext(t)
				iter, err := matDS.Iterator(ctx, 0)
				require.NoError(t, err)

				rows := collectRows(t, iter)
				assert.Equal(t, len(data), len(rows))

				// Verify first and last row content
				assert.Equal(t, "entity1", rows[0][0].Value)
				assert.Equal(t, "entity5", rows[len(rows)-1][0].Value)
			})

			// Test segment iterator
			t.Run("IterateSegment", func(t *testing.T) {
				ctx := logging.NewTestContext(t)
				iter, err := matDS.IterateSegment(ctx, 1, 4)
				require.NoError(t, err)

				rows := collectRows(t, iter)
				assert.Equal(t, 3, len(rows))
				assert.Equal(t, "entity2", rows[0][0].Value)
				assert.Equal(t, "entity4", rows[2][0].Value)
			})

			// Test chunk iterator
			t.Run("ChunkIterator", func(t *testing.T) {
				ctx := logging.NewTestContext(t)

				// Test each chunk
				numChunks, err := matDS.NumChunks()
				require.NoError(t, err)

				for i := 0; i < numChunks; i++ {
					t.Run(fmt.Sprintf("Chunk%d", i), func(t *testing.T) {
						iter, err := matDS.ChunkIterator(ctx, i)
						require.NoError(t, err)

						rows := collectRows(t, iter)

						// Calculate expected chunk size
						expectedSize := tc.chunkSize
						if i == numChunks-1 && int64(len(data))%tc.chunkSize != 0 {
							expectedSize = int64(len(data)) - int64(i)*tc.chunkSize
						}

						assert.Equal(t, int(expectedSize), len(rows))
					})
				}
			})

			// Test feature iterators
			t.Run("FeatureIterators", func(t *testing.T) {
				ctx := logging.NewTestContext(t)

				// Test FeatureIterator
				t.Run("FeatureIterator", func(t *testing.T) {
					iter, err := matDS.FeatureIterator(ctx, 0)
					require.NoError(t, err)

					featureRows := collectFeatureRows(t, iter)
					assert.Equal(t, len(data), len(featureRows))

					// Verify feature schema and content
					for i, fr := range featureRows {
						assert.Equal(t, featureSchema, fr.Schema)
						assert.Equal(t, data[i][0].Value, fr.Row[0].Value) // Entity value
						assert.Equal(t, data[i][1].Value, fr.Row[1].Value) // Feature value
					}
				})

				// Test FeatureIterateSegment
				t.Run("FeatureIterateSegment", func(t *testing.T) {
					iter, err := matDS.FeatureIterateSegment(ctx, 1, 4)
					require.NoError(t, err)

					featureRows := collectFeatureRows(t, iter)
					assert.Equal(t, 3, len(featureRows))
					assert.Equal(t, "entity2", featureRows[0].Row[0].Value)
					assert.Equal(t, "entity4", featureRows[2].Row[0].Value)
				})

				// Test FeatureChunkIterator
				t.Run("FeatureChunkIterator", func(t *testing.T) {
					numChunks, err := matDS.NumChunks()
					require.NoError(t, err)

					for i := 0; i < numChunks; i++ {
						t.Run(fmt.Sprintf("Chunk%d", i), func(t *testing.T) {
							iter, err := matDS.FeatureChunkIterator(ctx, i)
							require.NoError(t, err)

							featureRows := collectFeatureRows(t, iter)

							// Calculate expected chunk size
							expectedSize := tc.chunkSize
							if i == numChunks-1 && int64(len(data))%tc.chunkSize != 0 {
								expectedSize = int64(len(data)) - int64(i)*tc.chunkSize
							}

							assert.Equal(t, int(expectedSize), len(featureRows))
						})
					}
				})
			})
		})
	}

	// Test empty dataset
	t.Run("EmptyDataset", func(t *testing.T) {
		emptyData := []types.Row{}
		emptyDS := NewInMemoryDatasetWithChunkSize(emptyData, schema, testLocation, 10)
		emptyMatDS := NewMaterialization(emptyDS, testID, featureSchema)

		// Test size
		size, err := emptyMatDS.Len()
		require.NoError(t, err)
		assert.Equal(t, int64(0), size)

		// Test chunks
		chunks, err := emptyMatDS.NumChunks()
		require.NoError(t, err)
		assert.Equal(t, 0, chunks)

		// Test iteration
		ctx := logging.NewTestContext(t)
		iter, err := emptyMatDS.Iterator(ctx, 0)
		require.NoError(t, err)

		rows := collectRows(t, iter)
		assert.Equal(t, 0, len(rows))
	})
}

// Helper function to collect rows from an iterator
func collectRows(t *testing.T, iter Iterator) []types.Row {
	var rows []types.Row
	for iter.Next() {
		rows = append(rows, iter.Values())
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return rows
}

// Helper function to collect feature rows from a feature iterator
func collectFeatureRows(t *testing.T, iter *FeatureIterator) []types.FeatureRow {
	var rows []types.FeatureRow
	for iter.Next() {
		rows = append(rows, iter.FeatureValues())
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return rows
}
