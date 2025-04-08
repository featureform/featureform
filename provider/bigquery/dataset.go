// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
	ptype "github.com/featureform/provider/provider_type"
)

// Dataset implements the Dataset interface for BigQuery
type Dataset struct {
	client    *bigquery.Client
	location  *location.SQLLocation
	schema    types.Schema
	converter types.ValueConverter[any]
	limit     int
}

// NewDataset creates a new BigQuery dataset
func NewDataset(
	client *bigquery.Client,
	location *location.SQLLocation,
	schema types.Schema,
	converter types.ValueConverter[any],
	limit int,
) (*Dataset, error) {
	lmt := limit
	if limit <= 0 {
		lmt = -1
	}

	return &Dataset{
		client:    client,
		location:  location,
		schema:    schema,
		converter: converter,
		limit:     lmt,
	}, nil
}

// Location returns the dataset location
func (ds *Dataset) Location() location.Location {
	return ds.location
}

// Iterator returns an iterator over the dataset
func (ds *Dataset) Iterator(ctx context.Context) (dataset.Iterator, error) {
	logger := logging.GetLoggerFromContext(ctx)

	schema := ds.Schema()
	schema.SetColumnSanitizer(func(col string) string {
		return fmt.Sprintf("`%s`", col)
	})

	columnNames := schema.SanitizedColumnNames()
	cols := strings.Join(columnNames, ", ")
	loc := ds.location.Sanitized()

	var query string
	if ds.limit == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", cols, loc)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", cols, loc, ds.limit)
	}

	bqQ := ds.client.Query(query)
	it, err := bqQ.Read(ctx)
	if err != nil {
		logger.Errorw("Failed to execute query", "query", query, "error", err)
		return nil, fferr.NewInternalErrorf("Failed to execute query: %v", err)
	}

	return NewIterator(ctx, it, ds.converter, ds.schema), nil
}

// Schema returns the dataset schema
func (ds *Dataset) Schema() types.Schema {
	return ds.schema
}

// Iterator implements the Iterator interface for BigQuery
type Iterator struct {
	iter          *bigquery.RowIterator
	currentValues types.Row
	converter     types.ValueConverter[any]
	schema        types.Schema
	err           error
	closed        bool
	ctx           context.Context
}

func NewIterator(ctx context.Context, iter *bigquery.RowIterator, converter types.ValueConverter[any], schema types.Schema) *Iterator {
	return &Iterator{
		iter:      iter,
		converter: converter,
		schema:    schema,
		ctx:       ctx,
	}
}

func (it *Iterator) Values() types.Row {
	return it.currentValues
}

func (it *Iterator) Schema() types.Schema {
	return it.schema
}

func (it *Iterator) Columns() []string {
	return it.schema.ColumnNames()
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	// Big Query doesn't have a close method for iterators, but we can set the closed flag
	it.closed = true
	return nil
}

// Next advances the iterator to the next row
func (it *Iterator) Next() bool {
	select {
	case <-it.ctx.Done():
		it.err = fferr.NewInternalErrorf("context is done")
	}

	if it.closed {
		it.err = fferr.NewInternalErrorf("iterator is closed")
		return false
	}

	var rowValues []bigquery.Value
	err := it.iter.Next(&rowValues)
	if errors.Is(err, iterator.Done) {
		it.Close()
		return false
	}
	if err != nil {
		it.err = fferr.NewExecutionError(ptype.BigQueryOffline.String(), err)
		it.Close()
		return false
	}

	// Verify we have the expected number of columns
	if len(rowValues) != len(it.schema.Fields) {
		it.err = fferr.NewInternalErrorf("column count mismatch: schema has %d, query returned %d",
			len(it.schema.Fields), len(rowValues))
		it.Close()
		return false
	}

	// Create a new row for each iteration
	row := make(types.Row, len(it.schema.Fields))

	// Convert values according to schema
	for i, val := range rowValues {
		nativeType := it.schema.Fields[i].NativeType
		convertedVal, err := it.converter.ConvertValue(nativeType, val)
		if err != nil {
			it.err = err
			it.Close()
			return false
		}
		row[i] = convertedVal
	}

	it.currentValues = row
	return true
}
