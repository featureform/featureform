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
	"fmt"
	"testing"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
)

// mockReader implements the reader interface below.
type mockReader struct {
	records               []arrowlib.Record
	schema                *arrowlib.Schema
	shouldErr             bool
	NumTimesReleaseCalled int
	NumTimesNextCalled    int
}

// mockReader methods to implement the reader interface
func (mr *mockReader) Next() bool {
	mr.NumTimesNextCalled++
	if mr.shouldErr {
		return false
	}
	if mr.NumTimesNextCalled > len(mr.records) {
		return false
	}
	return true
}

func (mr *mockReader) Err() error {
	if mr.shouldErr {
		return fmt.Errorf("mock error")
	}
	return nil
}

func (mr *mockReader) Record() arrowlib.Record {
	return mr.records[mr.NumTimesNextCalled-1]
}

func (mr *mockReader) Release() {
	mr.NumTimesReleaseCalled++
	mr.records = nil
}

func (mr *mockReader) Schema() *arrowlib.Schema {
	return mr.schema
}

func TestIteratorNilReader(t *testing.T) {
	if _, err := newIterator(nil, types.Schema{}); err == nil {
		t.Fatalf("Successfully created iterator with nil reader")
	}
}

func TestIteratorWithErr(t *testing.T) {
	ctx := logging.NewTestContext(t)
	schema, err := ConvertSchema(newTestSchema())
	if err != nil {
		t.Fatalf("Conversion failed: %v", err)
	}
	reader := &mockReader{
		shouldErr: true,
	}
	iter, err := newIterator(reader, schema)
	if err != nil {
		t.Fatalf("Failed to create iterator: %s", err)
	}
	if iter.Next(ctx) {
		t.Fatalf("Next succeeded, err expected")
	}
	if err := iter.Err(); err == nil {
		t.Fatalf("Iteration did not return err")
	}
	if reader.NumTimesNextCalled != 1 {
		t.Fatalf("Next called %d times", reader.NumTimesNextCalled)
	}
}

func TestIterator(t *testing.T) {
	ctx := logging.NewTestContext(t)
	schema, err := ConvertSchema(newTestSchema())
	if err != nil {
		t.Fatalf("Conversion failed: %v", err)
	}
	rec, expRows := newTestRecordAndRows(t)
	reader := &mockReader{
		records: []arrowlib.Record{rec},
		schema:  newTestSchema(),
	}
	iter, err := newIterator(reader, schema)
	if err != nil {
		t.Fatalf("Failed to create iterator: %s", err)
	}
	gotSchema := iter.Schema()
	if !gotSchema.Equals(schema) {
		t.Fatalf("Iterator schema doesn't match constructor")
	}
	rows := make(types.Rows, 0)
	for iter.Next(ctx) {
		rows = append(rows, iter.Values())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Iteratation returned error: %v", err)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	assertRowsEqual(t, rows, expRows)
	if reader.NumTimesReleaseCalled != 1 {
		t.Fatalf("Release called %d times", reader.NumTimesReleaseCalled)
	}
	if reader.NumTimesNextCalled != 2 {
		t.Fatalf("Next called %d times", reader.NumTimesNextCalled)
	}
}
