package provider

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
)

func TestLegacyMaterializationAdapter(t *testing.T) {
	// Setup test data
	testID := MaterializationID("test-materialization")
	testLocation := pl.NewSQLLocation("test-location")

	now := time.Now()
	testRecords := []ResourceRecord{
		{
			Entity: "entity1",
			Value:  123.45,
			TS:     now.Add(-time.Hour),
		},
		{
			Entity: "entity2",
			Value:  67.89,
			TS:     now,
		},
		{
			Entity: "entity3",
			Value:  "string-value",
			TS:     now.Add(time.Hour),
		},
		{
			Entity: "entity4",
			Value:  true,
			TS:     now.Add(2 * time.Hour),
		},
		{
			Entity: "entity5",
			Value:  []float64{1.0, 2.0, 3.0}, // Test array values
			TS:     now.Add(3 * time.Hour),
		},
	}

	// Test with different chunk sizes
	testCases := []struct {
		name           string
		rowsPerChunk   int64
		expectedChunks int
	}{
		{"SingleChunk", 10, 1},
		{"EvenChunks", 2, 3},
		{"UnevenChunks", 3, 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create enhanced memory materialization
			memoryMat := newMemoryMaterialization(
				testID,
				testRecords,
				testLocation,
				tc.rowsPerChunk,
			)

			// Create test schema
			testSchema := ResourceSchema{
				Entity: "entity",
				Value:  "value",
				TS:     "timestamp",
			}

			// Create the adapter
			adapter := NewLegacyMaterializationAdapter(memoryMat, testSchema)

			// Test basic properties
			t.Run("BasicProperties", func(t *testing.T) {
				// Test ID
				id := adapter.ID()
				assert.Equal(t, dataset.MaterializationID(testID), id)

				// Test Location
				location := adapter.Location()
				assert.Equal(t, testLocation, location)

				// Test Schema
				schema := adapter.Schema()
				assert.Equal(t, 3, len(schema.Fields))
				assert.Equal(t, "entity", string(schema.Fields[0].Name))
				assert.Equal(t, "value", string(schema.Fields[1].Name))
				assert.Equal(t, "timestamp", string(schema.Fields[2].Name))

				// Test FeatureSchema
				featureSchema := adapter.FeatureSchema()
				assert.Equal(t, "entity", string(featureSchema.EntityColumn.Name))
				assert.Equal(t, 1, len(featureSchema.FeatureColumns))
				assert.Equal(t, "value", string(featureSchema.FeatureColumns[0].FeatureColumn.Name))
				assert.Equal(t, "timestamp", string(featureSchema.FeatureColumns[0].TimestampColumn.Name))
			})

			// Test Len and NumChunks
			t.Run("SizeAndChunks", func(t *testing.T) {
				// Test Len
				length, err := adapter.Len()
				require.NoError(t, err)
				assert.Equal(t, int64(len(testRecords)), length)

				// Test NumChunks
				numChunks, err := adapter.NumChunks()
				require.NoError(t, err)
				assert.Equal(t, tc.expectedChunks, numChunks)
			})

			// Test Iterator
			t.Run("Iterator", func(t *testing.T) {
				ctx := logging.NewTestContext(t)

				// Test full iteration
				t.Run("Full", func(t *testing.T) {
					iter, err := adapter.Iterator(ctx, 0)
					require.NoError(t, err)

					rows := collectRows(t, iter)
					assert.Equal(t, len(testRecords), len(rows))

					// Check data consistency for a few records
					assert.Equal(t, "entity1", rows[0][0].Value)
					assert.Equal(t, 123.45, rows[0][1].Value)
					assert.Equal(t, testRecords[0].TS, rows[0][2].Value)

					assert.Equal(t, "entity5", rows[4][0].Value)
					assert.Equal(t, []float64{1.0, 2.0, 3.0}, rows[4][1].Value)
				})

				// Test with limit
				t.Run("WithLimit", func(t *testing.T) {
					iter, err := adapter.Iterator(ctx, 3)
					require.NoError(t, err)

					rows := collectRows(t, iter)
					assert.Equal(t, 3, len(rows))

					// Check first three records only
					assert.Equal(t, "entity1", rows[0][0].Value)
					assert.Equal(t, "entity2", rows[1][0].Value)
					assert.Equal(t, "entity3", rows[2][0].Value)
				})
			})

			// Test IterateSegment
			t.Run("IterateSegment", func(t *testing.T) {
				ctx := logging.NewTestContext(t)

				// Test middle segment
				t.Run("MiddleSegment", func(t *testing.T) {
					iter, err := adapter.IterateSegment(ctx, 1, 4)
					require.NoError(t, err)

					rows := collectRows(t, iter)
					assert.Equal(t, 3, len(rows))
					assert.Equal(t, "entity2", rows[0][0].Value)
					assert.Equal(t, "entity3", rows[1][0].Value)
					assert.Equal(t, "entity4", rows[2][0].Value)
				})

				// Test out-of-bounds segment
				t.Run("OutOfBoundsSegment", func(t *testing.T) {
					iter, err := adapter.IterateSegment(ctx, 3, 10)
					require.NoError(t, err)

					rows := collectRows(t, iter)
					assert.Equal(t, 2, len(rows)) // Just the last 2 records
				})

				// Test empty segment
				t.Run("EmptySegment", func(t *testing.T) {
					iter, err := adapter.IterateSegment(ctx, 5, 5)
					require.NoError(t, err)

					rows := collectRows(t, iter)
					assert.Equal(t, 0, len(rows))
				})
			})

			// Test ChunkIterator
			t.Run("ChunkIterator", func(t *testing.T) {
				ctx := logging.NewTestContext(t)

				// Test all chunks
				for i := 0; i < tc.expectedChunks; i++ {
					t.Run(fmt.Sprintf("Chunk%d", i), func(t *testing.T) {
						iter, err := adapter.ChunkIterator(ctx, i)
						require.NoError(t, err)

						// Verify it's a SizedIterator
						_, ok := iter.(dataset.SizedIterator)
						require.True(t, ok, "ChunkIterator should return a SizedIterator")

						// Collect rows and verify count
						rows := collectRows(t, iter)

						if i < tc.expectedChunks-1 {
							// All chunks except the last should be full
							assert.Equal(t, int(tc.rowsPerChunk), len(rows))
						} else {
							// Last chunk might not be full
							expectedSize := len(testRecords) - int(tc.rowsPerChunk)*i
							assert.Equal(t, expectedSize, len(rows))
						}
					})
				}
			})

			// Test FeatureIterator methods
			t.Run("FeatureIterator", func(t *testing.T) {
				ctx := logging.NewTestContext(t)

				// Test FeatureIterator
				t.Run("Full", func(t *testing.T) {
					iter, err := adapter.FeatureIterator(ctx, 0)
					require.NoError(t, err)

					featureRows := collectFeatureRows(t, iter)
					assert.Equal(t, len(testRecords), len(featureRows))

					// Check schema
					assert.Equal(t, "entity", string(featureRows[0].Schema.EntityColumn.Name))
					assert.Equal(t, "value", string(featureRows[0].Schema.FeatureColumns[0].FeatureColumn.Name))
				})

				// Test FeatureIterateSegment
				t.Run("Segment", func(t *testing.T) {
					iter, err := adapter.FeatureIterateSegment(ctx, 2, 5)
					require.NoError(t, err)

					featureRows := collectFeatureRows(t, iter)
					assert.Equal(t, 3, len(featureRows))
					assert.Equal(t, "entity3", featureRows[0].Row[0].Value)
				})

				// Test FeatureChunkIterator
				t.Run("Chunk", func(t *testing.T) {
					iter, err := adapter.FeatureChunkIterator(ctx, 0)
					require.NoError(t, err)

					featureRows := collectFeatureRows(t, iter)
					if tc.rowsPerChunk >= int64(len(testRecords)) {
						assert.Equal(t, len(testRecords), len(featureRows))
					} else {
						assert.Equal(t, int(tc.rowsPerChunk), len(featureRows))
					}
				})
			})
		})
	}

	// Test with empty dataset
	t.Run("EmptyDataset", func(t *testing.T) {
		ctx := logging.NewTestContext(t)
		emptyMat := newMemoryMaterialization(
			testID,
			[]ResourceRecord{},
			testLocation,
			10,
		)

		testSchema := ResourceSchema{
			Entity: "entity",
			Value:  "value",
			TS:     "timestamp",
		}

		adapter := NewLegacyMaterializationAdapter(emptyMat, testSchema)

		// Test Len
		length, err := adapter.Len()
		require.NoError(t, err)
		assert.Equal(t, int64(0), length)

		// Test NumChunks
		numChunks, err := adapter.NumChunks()
		require.NoError(t, err)
		assert.Equal(t, 0, numChunks)

		// Test Iterator
		iter, err := adapter.Iterator(ctx, 0)
		require.NoError(t, err)

		rows := collectRows(t, iter)
		assert.Equal(t, 0, len(rows))
		assert.NoError(t, iter.Err())
	})
}

// Helper function to collect all rows from an iterator
func collectRows(t *testing.T, iter dataset.Iterator) []types.Row {
	var rows []types.Row
	for iter.Next() {
		rows = append(rows, iter.Values())
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return rows
}

// Helper function to collect all feature rows from a feature iterator
func collectFeatureRows(t *testing.T, iter *dataset.FeatureIterator) []types.FeatureRow {
	var rows []types.FeatureRow
	for iter.Next() {
		rows = append(rows, iter.FeatureValues())
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return rows
}
