package provider

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/featureform/fftypes"
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

func (ds *InMemoryDataset) Iterator() (NewIterator, error) {
	return &InMemoryIterator{data: ds.data, index: -1}, nil
}

func (ds *InMemoryDataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}

func (ds *InMemoryDataset) WriteBatch(rows []types.Row) error {
	ds.data = append(ds.data, rows...)
	return nil
}

func (ds *InMemoryDataset) Len() (int64, error) {
	return int64(len(ds.data)), nil
}

func (ds *InMemoryDataset) IterateSegment(begin, end int64) (NewIterator, error) {
	if begin < 0 || end > int64(len(ds.data)) || begin > end {
		return nil, errors.New("invalid segment range")
	}
	return &InMemoryIterator{data: ds.data[begin:end], index: -1}, nil
}

type InMemoryIterator struct {
	data  []types.Row
	index int
}

func (it *InMemoryIterator) Next() bool {
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
	return types.Schema{}, nil // Mock implementation
}

func (it *InMemoryIterator) Err() error {
	return nil
}

func (it *InMemoryIterator) Close() error {
	return nil
}

func TestNewDataset(t *testing.T) {
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
	assert.NoError(t, err)
	assert.Equal(t, schema, sch)
}

func TestWriteableDataset(t *testing.T) {
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset([]types.Row{}, types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}, location)
	rows := []types.Row{{types.Value{NativeType: "int", Type: types.Int, Value: 4}}, {types.Value{NativeType: "int", Type: types.Int, Value: 5}}}
	err = ds.WriteBatch(rows)
	require.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), int64(len(ds.data)))
}

func TestSizedDataset(t *testing.T) {
	data := []types.Row{{types.Value{NativeType: "int", Type: types.Int, Value: 1}}, {types.Value{NativeType: "int", Type: types.Int, Value: 2}}}
	schema := types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset(data, schema, location)

	size, err := ds.Len()
	require.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), size)
}

func TestSegmentableDataset(t *testing.T) {
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset([]types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 3}},
	}, types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}, location)

	iter, err := ds.IterateSegment(1, 3)
	assert.NoError(t, err)
	assert.NotNil(t, iter)

	assert.True(t, iter.Next())
	assert.Equal(t, types.Row{types.Value{NativeType: "int", Type: types.Int, Value: 2}}, iter.Values())
	assert.True(t, iter.Next())
	assert.Equal(t, types.Row{types.Value{NativeType: "int", Type: types.Int, Value: 3}}, iter.Values())
	assert.False(t, iter.Next())
}

func TestInvalidSegmentableDataset(t *testing.T) {
	location, err := pl.NewFileLocationFromURI("file://test")
	require.NoError(t, err)
	ds := NewInMemoryDataset([]types.Row{
		{types.Value{NativeType: "int", Type: types.Int, Value: 1}},
		{types.Value{NativeType: "int", Type: types.Int, Value: 2}},
	}, types.Schema{Fields: []types.ColumnSchema{{Name: "id", NativeType: "int"}}}, location)

	iter, err := ds.IterateSegment(-1, 3)
	assert.Error(t, err)
	assert.Nil(t, iter)
}
