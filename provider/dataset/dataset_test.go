package dataset

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
)

type InMemoryDataset struct {
	data     []types.Row
	schema   types.Schema
	location pl.Location
}

func NewInMemoryDataset(data []types.Row, schema types.Schema, location pl.Location) *InMemoryDataset {
	return &InMemoryDataset{data: data, schema: schema, location: location}
}

func (ds *InMemoryDataset) Location() pl.Location {
	return ds.location
}

func (ds *InMemoryDataset) Iterator(limit int, ctx context.Context) (Iterator, error) {
	return &InMemoryIterator{data: ds.data, index: -1}, nil
}

func (ds *InMemoryDataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}

func (ds *InMemoryDataset) WriteBatch(ctx context.Context, rows []types.Row) error {
	ds.data = append(ds.data, rows...)
	return nil
}

func (ds *InMemoryDataset) Len() (int64, error) {
	return int64(len(ds.data)), nil
}

func (ds *InMemoryDataset) IterateSegment(ctx context.Context, begin, end int64) (Iterator, error) {
	size, err := ds.Len()
	if err != nil {
		return nil, err
	}
	if begin < 0 || end > size || begin > end {
		return nil, errors.New("invalid segment range")
	}
	return &InMemoryIterator{data: ds.data[begin:end], index: -1}, nil
}

type InMemoryIterator struct {
	data  []types.Row
	index int
}

func (it *InMemoryIterator) Next(ctx context.Context) bool {
	if it.index+1 < len(it.data) {
		it.index++
		return true
	}
	return false
}

func (it *InMemoryIterator) Values() types.Row {
	return it.data[it.index]
}

func (it *InMemoryIterator) Schema() (types.Schema, error) {
	return types.Schema{}, nil
}

func (it *InMemoryIterator) Err() error {
	return nil
}

func (it *InMemoryIterator) Close() error {
	return nil
}

type SizedInMemoryIterator struct {
	InMemoryIterator
}

func (it *SizedInMemoryIterator) Len() (int64, error) {
	return int64(len(it.data)), nil
}

func TestNewDataset(t *testing.T) {
	ctx := logging.NewTestContext(t)

	data := []types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 3}}}
	schema := types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset(data, schema, location)

	assert.Equal(t, location, ds.Location())
	sch, err := ds.Schema()
	require.NoError(t, err)
	assert.Equal(t, schema, sch)

	iter, err := ds.Iterator(ctx)
	require.NoError(t, err)
	require.NotNil(t, iter)
}

func TestWriteableDataset(t *testing.T) {
	ctx := logging.NewTestContext(t)

	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset([]types.Row{}, types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}, location)
	rows := []types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 4}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 5}},
	}
	err = ds.WriteBatch(ctx, rows)
	require.NoError(t, err)
	length, err := ds.Len()
	require.NoError(t, err)
	assert.Equal(t, int64(2), length)
}

func TestSizedDataset(t *testing.T) {
	data := []types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
	}
	schema := types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset(data, schema, location)

	size, err := ds.Len()
	require.NoError(t, err)
	assert.Equal(t, int64(2), size)
}

func TestSegmentableDataset(t *testing.T) {
	ctx := logging.NewTestContext(t)

	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset([]types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 3}},
	}, types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}, location)

	iter, err := ds.IterateSegment(ctx, 1, 3)
	require.NoError(t, err)
	require.NotNil(t, iter)

	require.True(t, iter.Next(ctx))
	assert.Equal(t, types.Row{types.Value{NativeType: "int", Type: types.Int, Value: 2}}, iter.Values())
	require.True(t, iter.Next(ctx))
	assert.Equal(t, types.Row{types.Value{NativeType: "int", Type: types.Int, Value: 3}}, iter.Values())
	assert.False(t, iter.Next(ctx))
}

func TestInvalidSegmentableDataset(t *testing.T) {
	ctx := logging.NewTestContext(t)

	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset([]types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
	}, types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}, location)

	iter, err := ds.IterateSegment(ctx, -1, 3)
	assert.Error(t, err)
	assert.Nil(t, iter)
}

func TestChunkedDatasetAdapter(t *testing.T) {
	ctx := logging.NewTestContext(t)

	// Setup: Create a dataset with 10 rows
	data := make([]types.Row, 10)
	for i := 0; i < 10; i++ {
		data[i] = types.Row{types.Value{NativeType: "int", Type: types.Int, Value: i + 1}}
	}
	schema := types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset(data, schema, location)

	// Create adapter with chunk size of 3
	adapter := ChunkedDatasetAdapter{
		SizedSegmentableDataset: ds,
		ChunkSize:               3,
	}

	// Test 1: Verify correct number of chunks
	t.Run("NumChunks", func(t *testing.T) {
		numChunks, err := adapter.NumChunks()
		require.NoError(t, err)
		// 10 rows with chunk size 3 should give 4 chunks (3,3,3,1)
		assert.Equal(t, 4, numChunks)
	})

	// Test 2: Verify first chunk (should have 3 elements)
	t.Run("FirstChunk", func(t *testing.T) {
		// Create test context for this subtest
		iter, err := adapter.ChunkIterator(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, iter)

		// Verify chunk size
		size, err := iter.Len()
		require.NoError(t, err)
		assert.Equal(t, int64(3), size)

		// Verify chunk contents
		expectedValues := []int{1, 2, 3}
		i := 0
		// Pass context to Next
		for iter.Next(ctx) {
			val := iter.Values()
			assert.Equal(t, expectedValues[i], val[0].Value)
			i++
		}
		assert.Equal(t, 3, i, "Should have iterated through 3 elements")
	})

	// Test 3: Verify last chunk (should have only 1 element)
	t.Run("LastChunk", func(t *testing.T) {
		iter, err := adapter.ChunkIterator(ctx, 3)
		require.NoError(t, err)
		require.NotNil(t, iter)

		// Verify chunk size
		size, err := iter.Len()
		require.NoError(t, err)
		assert.Equal(t, int64(1), size)

		// Verify chunk contents
		require.True(t, iter.Next(ctx))
		val := iter.Values()
		assert.Equal(t, 10, val[0].Value)
		assert.False(t, iter.Next(ctx), "Should have only one element")
	})

	// Test 4: Verify error handling for out-of-bounds chunk
	t.Run("OutOfBoundsChunk", func(t *testing.T) {
		iter, err := adapter.ChunkIterator(ctx, 4)
		assert.Error(t, err)
		assert.Nil(t, iter)
	})
}

func TestSizedIterator(t *testing.T) {
	ctx := logging.NewTestContext(t)

	data := []types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 3}},
	}

	iter := &SizedInMemoryIterator{
		InMemoryIterator: InMemoryIterator{data: data, index: -1},
	}

	length, err := iter.Len()
	require.NoError(t, err)
	assert.Equal(t, int64(3), length)

	count := 0
	for iter.Next(ctx) {
		count++
		val := iter.Values()
		assert.Equal(t, count, val[0].Value)
	}
	assert.Equal(t, 3, count)
}
