// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package dataset

import (
	. "context"
	"sync"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	pl "github.com/featureform/provider/location"
)

// Dataset is the base interface required by most of the
// OfflineStore interface. It provides a location, iterator,
// and schema
type Dataset interface {
	Location() pl.Location
	Iterator(ctx Context) (Iterator, error)
	Schema() (types.Schema, error)
}

// WriteableDataset is a Dataset that you can write to. For some datasets,
// especially file based one, writing single rows is expensive. It's
// preferable to write as many things as possible in a single batch.
type WriteableDataset interface {
	Dataset
	WriteBatch(Context, []types.Row) error
}

// SizedDataset is a Dataset where the size can be cheaply calculated.
type SizedDataset interface {
	Dataset
	Len() (int64, error)
}

// SegmentableDataset allows a user to easily iterate
// an arbitrary segment of the dataset. Note that it
// doesn't guarantee the end of the iterator is actually
// in range of the dataset. You can try casting Iterator
// to SizedIterator to see if the length is available.
type SegmentableDataset interface {
	Dataset
	IterateSegment(ctx Context, begin, end int64) (Iterator, error)
}

type ChunkedDataset interface {
	NumChunks() (int, error)
	ChunkIterator(ctx Context, idx int) (SizedIterator, error)
}

type SizedSegmentableDataset interface {
	SizedDataset
	SegmentableDataset
}

// ChunkedDatasetAdapter takes a SizedSegmentableDataset
// and adapts it to include the ChunkedDataset methods
// by using Len() and IterateSegment() to create
// chunk iterators of size ChunkSize (last chunk will be
// <= ChunkSize).
type ChunkedDatasetAdapter struct {
	SizedSegmentableDataset
	ChunkSize int64

	size     int64
	sizeErr  error
	sizeOnce sync.Once
}

func (adapter *ChunkedDatasetAdapter) getSize() (int64, error) {
	if adapter.ChunkSize <= 0 {
		return 0, fferr.NewInternalErrorf("chunk size must be > 0")
	}
	adapter.sizeOnce.Do(func() {
		adapter.size, adapter.sizeErr = adapter.SizedSegmentableDataset.Len()
	})
	return adapter.size, adapter.sizeErr
}

func (adapter *ChunkedDatasetAdapter) NumChunks() (int, error) {
	size, err := adapter.getSize()
	if err != nil {
		return -1, err
	}

	numChunks := size / adapter.ChunkSize
	if size%adapter.ChunkSize > 0 {
		numChunks++
	}

	return int(numChunks), nil
}

func (adapter *ChunkedDatasetAdapter) ChunkIterator(ctx Context, idx int) (SizedIterator, error) {
	numChunks, err := adapter.NumChunks()
	if err != nil {
		return nil, err
	}

	if idx < 0 || idx >= numChunks {
		return nil, fferr.NewInternalErrorf("chunk index out of range")
	}

	begin := int64(idx) * adapter.ChunkSize
	end := begin + adapter.ChunkSize

	size, err := adapter.SizedSegmentableDataset.Len()
	if err != nil {
		return nil, err
	}

	if end > size {
		end = size
	}

	iter, err := adapter.SizedSegmentableDataset.IterateSegment(ctx, begin, end)
	if err != nil {
		return nil, err
	}

	// Create a wrapper that adds the Len method to any iterator
	return &GenericSizedIterator{
		Iterator: iter,
		length:   end - begin,
	}, nil
}

// Iterator is the generic interface to loop through any dataset in Featureform.
type Iterator interface {
	Next() bool
	Values() types.Row
	Schema() (types.Schema, error)
	Columns() []string
	Err() error
	Close() error
}

// SizedIterator is an Iterator where we can cheaply check the full length.
type SizedIterator interface {
	Iterator
	Len() (int64, error)
}

type GenericSizedIterator struct {
	Iterator
	length int64
}

func (it *GenericSizedIterator) Len() (int64, error) {
	return it.length, nil
}
