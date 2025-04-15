// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package dataset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
)

type DatasetTestCase struct {
	Dataset        Dataset
	ExpectedData   []types.Row
	ExpectedSchema types.Schema
	Location       pl.Location

	// DatasetFactory creates a new dataset with the same configuration as Dataset
	// Used for tests that modify the dataset (e.g., write tests)
	DatasetFactory func() Dataset
}

// RunDatasetTestSuite runs a suite of tests against a pre-created dataset
func RunDatasetTestSuite(t *testing.T, tc DatasetTestCase) {
	t.Run("BasicProperties", func(t *testing.T) {
		testBasicProperties(t, tc)
	})

	t.Run("Iterator", func(t *testing.T) {
		testIterator(t, tc)
	})

	t.Run("SegmentIterator", func(t *testing.T) {
		testSegmentIterator(t, tc)
	})

	if writeable, ok := tc.Dataset.(WriteableDataset); ok {
		t.Run("Writeable", func(t *testing.T) {
			// Use a fresh dataset for write tests if a factory is provided
			var datasetToTest WriteableDataset
			if tc.DatasetFactory != nil {
				freshDataset := tc.DatasetFactory()
				var ok bool
				datasetToTest, ok = freshDataset.(WriteableDataset)
				if !ok {
					t.Fatalf("Dataset created by factory does not implement WriteableDataset")
				}
			} else {
				// Otherwise, use the original but warn about it
				t.Log("No dataset factory provided. The original dataset will be modified.")
				datasetToTest = writeable
			}
			testWriteableDataset(t, datasetToTest, tc)
		})
	}

	if sized, ok := tc.Dataset.(SizedDataset); ok {
		t.Run("Sized", func(t *testing.T) {
			testSizedDataset(t, sized, tc)
		})
	}

	if segmentable, ok := tc.Dataset.(SegmentableDataset); ok {
		t.Run("InvalidSegment", func(t *testing.T) {
			testInvalidSegment(t, segmentable)
		})
	}

	if sizedSegmentable, ok := tc.Dataset.(SizedSegmentableDataset); ok {
		t.Run("ChunkedAdapter", func(t *testing.T) {
			testChunkedDatasetAdapter(t, sizedSegmentable, tc)
		})
	}
}

func testIteratorSize(t *testing.T, iter Iterator, expectedSize int64) (isSized bool) {
	sizedIter, ok := iter.(SizedIterator)
	if !ok {
		return false
	}

	t.Logf("Iterator of type %T implements SizedIterator", iter)
	size, err := sizedIter.Len()
	require.NoError(t, err)
	assert.Equal(t, expectedSize, size, "Iterator size doesn't match expected length")
	return true
}

func testBasicProperties(t *testing.T, tc DatasetTestCase) {
	assert.Equal(t, tc.Location, tc.Dataset.Location())
	schema := tc.Dataset.Schema()
	assert.Equal(t, tc.ExpectedSchema, schema)
}

func testIterator(t *testing.T, tc DatasetTestCase) {
	ctx := logging.NewTestContext(t)

	iter, err := tc.Dataset.Iterator(ctx, -1)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Check if the iterator implements SizedIterator
	if isSized := testIteratorSize(t, iter, int64(len(tc.ExpectedData))); isSized {
		// Get a fresh iterator since we might have consumed it
		iter, err = tc.Dataset.Iterator(ctx, -1)
		require.NoError(t, err)
	}

	i := 0
	for iter.Next() {
		require.Less(t, i, len(tc.ExpectedData), "Iterator returned more rows than expected")
		assert.Equal(t, tc.ExpectedData[i], iter.Values())
		i++
	}
	assert.Equal(t, len(tc.ExpectedData), i, "Iterator did not return all expected rows")
}

func testSegmentIterator(t *testing.T, tc DatasetTestCase) {
	ctx := logging.NewTestContext(t)

	// Skip test if not enough data
	if len(tc.ExpectedData) < 2 {
		t.Skip("Not enough data for segment test")
		return
	}

	// Test segment from 1 to end
	segmentable, ok := tc.Dataset.(SegmentableDataset)
	if !ok {
		t.Skip("Dataset is not segmentable")
		return
	}

	start := int64(1)
	end := int64(len(tc.ExpectedData))
	expectedSize := end - start

	iter, err := segmentable.IterateSegment(ctx, start, end)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Check if the segment iterator implements SizedIterator
	if isSized := testIteratorSize(t, iter, expectedSize); isSized {
		// Get a fresh iterator since we might have consumed it
		iter, err = segmentable.IterateSegment(ctx, start, end)
		require.NoError(t, err)
	}

	// Verify iterator returns expected segment
	for i := start; i < end; i++ {
		require.True(t, iter.Next(), "Iterator ended prematurely")
		assert.Equal(t, tc.ExpectedData[i], iter.Values())
	}
	assert.False(t, iter.Next(), "Iterator did not end as expected")
}

func testWriteableDataset(t *testing.T, ds WriteableDataset, tc DatasetTestCase) {
	// This test will modify the dataset by writing new rows to it
	t.Run("WriteBatch", func(t *testing.T) {
		ctx := logging.NewTestContext(t)

		// Create new rows to write
		newRows := []types.Row{
			{types.Value{NativeType: "int", Type: types.Int, Value: 100}},
			{types.Value{NativeType: "int", Type: types.Int, Value: 101}},
		}

		// Get initial length
		var initialLen int64
		if sized, ok := ds.(SizedDataset); ok {
			var err error
			initialLen, err = sized.Len()
			require.NoError(t, err)
		}

		// Keep track of expected data after write
		expectedDataAfterWrite := append([]types.Row{}, tc.ExpectedData...)
		expectedDataAfterWrite = append(expectedDataAfterWrite, newRows...)

		// Write batch
		err := ds.WriteBatch(ctx, newRows)
		require.NoError(t, err)

		// Verify length increased if possible
		if sized, ok := ds.(SizedDataset); ok {
			newLen, err := sized.Len()
			require.NoError(t, err)
			assert.Equal(t, initialLen+int64(len(newRows)), newLen)
		}

		// Verify all rows (original + new) are available through iteration
		iter, err := ds.Iterator(ctx, -1)
		require.NoError(t, err)

		rowIdx := 0
		for iter.Next() {
			if rowIdx < len(expectedDataAfterWrite) {
				assert.Equal(t, expectedDataAfterWrite[rowIdx], iter.Values())
			} else {
				t.Errorf("Iterator returned more rows than expected")
			}
			rowIdx++
		}

		assert.Equal(t, len(expectedDataAfterWrite), rowIdx,
			"Iterator did not return all expected rows")
	})
}

func testSizedDataset(t *testing.T, ds SizedDataset, tc DatasetTestCase) {
	size, err := ds.Len()
	require.NoError(t, err)
	assert.Equal(t, int64(len(tc.ExpectedData)), size)
}

func testInvalidSegment(t *testing.T, ds SegmentableDataset) {
	ctx := logging.NewTestContext(t)

	// Try an invalid segment range
	iter, err := ds.IterateSegment(ctx, -1, 3)
	assert.Error(t, err)
	assert.Nil(t, iter)
}

func testChunkedDatasetAdapter(t *testing.T, ds SizedSegmentableDataset, tc DatasetTestCase) {
	ctx := logging.NewTestContext(t)

	// Create adapter with chunk size of 3
	adapter := ChunkedDatasetAdapter{
		SizedSegmentableDataset: ds,
		ChunkSize:               3,
	}

	// Calculate expected number of chunks
	totalRows := int64(len(tc.ExpectedData))
	expectedChunks := (totalRows + adapter.ChunkSize - 1) / adapter.ChunkSize

	// Test 1: Verify correct number of chunks
	t.Run("NumChunks", func(t *testing.T) {
		numChunks, err := adapter.NumChunks()
		require.NoError(t, err)
		assert.Equal(t, int(expectedChunks), numChunks)
	})

	// Test 2: Verify first chunk (should have up to ChunkSize elements)
	if totalRows > 0 {
		t.Run("FirstChunk", func(t *testing.T) {
			// Create test context for this subtest
			iter, err := adapter.ChunkIterator(ctx, 0)
			require.NoError(t, err)
			require.NotNil(t, iter)

			// Calculate expected size for first chunk
			expectedSize := adapter.ChunkSize
			if totalRows < expectedSize {
				expectedSize = totalRows
			}

			// We expect chunk iterators to always be sized
			isSized := testIteratorSize(t, iter, expectedSize)
			require.True(t, isSized, "Chunk iterator should implement SizedIterator")

			// Get a fresh iterator since we might have consumed it
			iter, err = adapter.ChunkIterator(ctx, 0)
			require.NoError(t, err)

			// Verify chunk contents
			i := 0
			for iter.Next() {
				val := iter.Values()
				assert.Equal(t, tc.ExpectedData[i], val)
				i++
			}
			assert.Equal(t, int(expectedSize), i, "Should have iterated through all elements")
		})
	}

	// Test 3: Verify last chunk if there are multiple chunks
	if int(expectedChunks) > 1 {
		t.Run("LastChunk", func(t *testing.T) {
			lastChunkIndex := int(expectedChunks) - 1
			iter, err := adapter.ChunkIterator(ctx, lastChunkIndex)
			require.NoError(t, err)
			require.NotNil(t, iter)

			// Calculate expected size for last chunk
			lastChunkSize := int(totalRows) - (lastChunkIndex * int(adapter.ChunkSize))

			// We expect chunk iterators to always be sized
			isSized := testIteratorSize(t, iter, int64(lastChunkSize))
			require.True(t, isSized, "Chunk iterator should implement SizedIterator")

			// Get a fresh iterator since we might have consumed it
			iter, err = adapter.ChunkIterator(ctx, lastChunkIndex)
			require.NoError(t, err)

			// Verify chunk contents
			startIdx := lastChunkIndex * int(adapter.ChunkSize)
			i := 0
			for iter.Next() {
				val := iter.Values()
				assert.Equal(t, tc.ExpectedData[startIdx+i], val)
				i++
			}
			assert.Equal(t, lastChunkSize, i, "Should have iterated through all elements")
		})
	}

	// Test 4: Verify error handling for out-of-bounds chunk
	t.Run("OutOfBoundsChunk", func(t *testing.T) {
		iter, err := adapter.ChunkIterator(ctx, int(expectedChunks))
		assert.Error(t, err)
		assert.Nil(t, iter)
	})
}
