// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package dataset

import (
	"context"
	"errors"
	"fmt"

	types "github.com/featureform/fftypes"
	pl "github.com/featureform/provider/location"
)

type InMemoryDataset struct {
	data      []types.Row
	schema    types.Schema
	location  pl.Location
	chunkSize int64
}

func NewInMemoryDataset(data []types.Row, schema types.Schema, location pl.Location) *InMemoryDataset {
	return &InMemoryDataset{
		data:      data,
		schema:    schema,
		location:  location,
		chunkSize: 1, // Default chunk size
	}
}

func NewInMemoryDatasetWithChunkSize(data []types.Row, schema types.Schema, location pl.Location, chunkSize int64) *InMemoryDataset {
	if chunkSize <= 0 {
		chunkSize = 100 // Fallback to default if invalid
	}
	return &InMemoryDataset{
		data:      data,
		schema:    schema,
		location:  location,
		chunkSize: chunkSize,
	}
}

func (ds *InMemoryDataset) Location() pl.Location {
	return ds.location
}

func (ds *InMemoryDataset) Iterator(ctx context.Context, limit int64) (Iterator, error) {
	return NewInMemoryIterator(ds.data, ds.schema, limit), nil
}

func (ds *InMemoryDataset) Schema() types.Schema {
	return ds.schema
}

func (ds *InMemoryDataset) WriteBatch(ctx context.Context, rows []types.Row) error {
	ds.data = append(ds.data, rows...)
	return nil
}

func (ds *InMemoryDataset) NumChunks() (int, error) {
	size, err := ds.Len()
	if err != nil {
		return 0, err
	}

	numChunks := size / ds.chunkSize
	if size%ds.chunkSize > 0 {
		numChunks++
	}
	return int(numChunks), nil
}

func (ds *InMemoryDataset) ChunkIterator(ctx context.Context, idx int) (SizedIterator, error) {
	numChunks, err := ds.NumChunks()
	if err != nil {
		return nil, err
	}

	if idx < 0 || idx >= numChunks {
		return nil, fmt.Errorf("chunk index out of range: %d (num chunks: %d)", idx, numChunks)
	}

	begin := int64(idx) * ds.chunkSize
	end := begin + ds.chunkSize

	size, err := ds.Len()
	if err != nil {
		return nil, err
	}

	if end > size {
		end = size
	}

	iter, err := ds.IterateSegment(ctx, begin, end)
	if err != nil {
		return nil, err
	}

	sizedIter, ok := iter.(SizedIterator)
	if ok {
		return sizedIter, nil
	}

	return &GenericSizedIterator{
		Iterator: iter,
		Length:   end - begin,
	}, nil
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
	it := InMemoryIterator{data: ds.data[begin:end], index: -1}
	return &SizedInMemoryIterator{it}, nil
}

type InMemoryIterator struct {
	data   []types.Row
	schema types.Schema
	index  int
	limit  int64 // Add a limit field
}

func NewInMemoryIterator(data []types.Row, schema types.Schema, limit int64) *InMemoryIterator {
	// If limit is <= 0 or greater than data length, use full data length
	if limit <= 0 || limit > int64(len(data)) {
		limit = int64(len(data))
	}

	// Create a limited view of the data if needed
	limitedData := data
	if limit < int64(len(data)) {
		limitedData = data[:limit]
	}

	return &InMemoryIterator{
		data:   limitedData,
		schema: schema,
		index:  -1, // Start at -1 so first Next() will return index 0
		limit:  limit,
	}
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

func (it *InMemoryIterator) Schema() types.Schema {
	return it.schema
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
