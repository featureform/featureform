// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package arrow

import (
	"context"
	"sync"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/types"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
)

// *ipc.Reader implements this interface, and this
// allows for easier testing/dependency injection.
type reader interface {
	Next() bool
	Err() error
	Record() arrowlib.Record
	Release()
	Schema() *arrowlib.Schema
}

type Iterator struct {
	reader     reader
	schema     types.Schema
	curRows    types.Rows
	curRowsIdx int
	closed     bool
	lastErr    error
	mtx        sync.RWMutex
}

func newIterator(reader reader, schema types.Schema) (*Iterator, error) {
	if reader == nil {
		return nil, fferr.NewInternalErrorf("Cannot create arrow iterator with nil reader")
	}
	return &Iterator{
		reader: reader,
		schema: schema,
		mtx:    sync.RWMutex{},
	}, nil
}

func (iter *Iterator) Next(ctx context.Context) bool {
	iter.mtx.RLock()
	defer iter.mtx.RUnlock()
	if iter.closed {
		return false
	}
	iter.curRowsIdx++
	if iter.curRowsIdx >= len(iter.curRows) {
		return iter.parseNextBatch(ctx)
	}
	return true
}

func (iter *Iterator) parseNextBatch(ctx context.Context) bool {
	contin := iter.reader.Next()
	if !contin {
		return false
	}
	rec := iter.reader.Record()
	defer rec.Release()
	rows, err := ConvertRecordToRows(rec, iter.schema)
	if err != nil {
		logger := logging.GetLoggerFromContext(ctx)
		logger.Errorw("Failed to convert record to row", "err", err)
		iter.lastErr = err
		return false
	}
	iter.curRows = rows
	iter.curRowsIdx = 0
	return true
}

func (iter *Iterator) Values() types.Row {
	iter.mtx.RLock()
	defer iter.mtx.RUnlock()
	if len(iter.curRows) == 0 {
		return nil
	}
	return iter.curRows[iter.curRowsIdx]
}

func (iter *Iterator) Close() error {
	iter.mtx.Lock()
	defer iter.mtx.Unlock()
	iter.reader.Release()
	iter.closed = true
	return nil
}

func (iter *Iterator) Err() error {
	iter.mtx.RLock()
	defer iter.mtx.RUnlock()
	if iter.lastErr != nil {
		return iter.lastErr
	}
	return iter.reader.Err()
}

func (iter *Iterator) Schema() (types.Schema, error) {
	iter.mtx.RLock()
	defer iter.mtx.RUnlock()
	return iter.schema, nil
}
