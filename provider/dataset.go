// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package provider

import (
	types "github.com/featureform/fftypes"
	pl "github.com/featureform/provider/location"
)

// NewDataset is the base interface required by most of the
// OfflineStore interface. It provides a location, iterator,
// and schema
type NewDataset interface {
	Location() pl.Location
	Iterator() (NewIterator, error)
	Schema() (types.Schema, error)
}

// WriteableDataset is a Dataset that you can write to. For some datasets,
// especially file based one, writing single rows is expensive. It's
// preferable to write as many things as possible in a single batch.
type WriteableDataset interface {
	NewDataset
	WriteBatch([]types.Row) error
}

// SizedDataset is a Dataset where the size can be cheaply calculated.
type SizedDataset interface {
	NewDataset
	Len() (int64, error)
}

// SegmentableDataset allows a user to easily iterate
// an arbitrary segment of the dataset. Note that it
// doesn't guarantee the end of the iterator is actually
// in range of the dataset. You can try casting Iterator
// to SizedIterator to see if the length is available.
type SegmentableDataset interface {
	NewDataset
	IterateSegment(begin, end int64) (NewIterator, error)
}

type ChunkedDataset interface {
	NumChunks() (int, error)
	ChunkIterator(idx int) (SizedIterator, error)
}

// ChunkedDatasetAdapter takes a SizedSegmentableDataset
// and adapts it to include the ChunkedDataset methods
// by using Len() and IterateSegment() to create
// chunk iterators of size ChunkSize (last chunk will be
// <= ChunkSize).
type ChunkedDatasetAdapter struct {
	SizedDataset
	SegmentableDataset
	ChunkSize int64
}

// NewIterator is the generic interface to loop through any dataset in
// Featureform.
type NewIterator interface {
	Next() bool
	Values() types.Row
	Schema() (types.Schema, error)
	Err() error
	Close() error
}

// SizedIterator is an Iterator where we can cheaply check
// the full length.
type SizedIterator interface {
	NewIterator
	Len() (int64, error)
}
