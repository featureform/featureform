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

func (ds *InMemoryDataset) Iterator(ctx context.Context, limit int64) (Iterator, error) {
	return &InMemoryIterator{data: ds.data, schema: ds.schema, index: int(limit)}, nil
}

func (ds *InMemoryDataset) Schema() types.Schema {
	return ds.schema
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
	it := InMemoryIterator{data: ds.data[begin:end], index: -1}
	return &SizedInMemoryIterator{it}, nil
}

type InMemoryIterator struct {
	data   []types.Row
	schema types.Schema
	index  int
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
