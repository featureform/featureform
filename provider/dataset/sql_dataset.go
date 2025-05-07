// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package dataset

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	"github.com/featureform/provider/location"
)

type SqlDataset struct {
	db        *sql.DB
	location  *location.SQLLocation
	schema    types.Schema
	converter types.ValueConverter[any]
	limit     int

	sanitizer func(obj location.FullyQualifiedObject) string // remove this and create specific datasets
}

func NewSqlDataset(
	db *sql.DB,
	location *location.SQLLocation,
	schema types.Schema,
	converter types.ValueConverter[any],
	limit int,
) (*SqlDataset, error) {
	lmt := limit
	if limit <= 0 {
		lmt = -1
	}

	return &SqlDataset{
		db:        db,
		location:  location,
		schema:    schema,
		converter: converter,
		limit:     lmt,
	}, nil
}

func (ds *SqlDataset) SetSanitizer(sanitizer func(obj location.FullyQualifiedObject) string) {
	ds.sanitizer = sanitizer
}

func (ds *SqlDataset) Location() location.Location {
	return ds.location
}

func (ds *SqlDataset) Iterator(ctx context.Context, limit int64) (Iterator, error) {
	logger := logging.GetLoggerFromContext(ctx)
	schema := ds.Schema()

	columnNames := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		columnNames[i] = postgres.Sanitize(string(field.Name))
	}

	if len(columnNames) == 0 {
		return nil, fferr.NewInternalErrorf("No columns found in schema")
	}

	cols := strings.Join(columnNames, ", ")
	var loc string
	if ds.sanitizer != nil {
		loc = ds.sanitizer(ds.location.TableLocation())
	} else {
		loc = location.SanitizeFullyQualifiedObject(ds.location.TableLocation())
	}
	var query string

	effectiveLimit := -1
	if ds.limit > 0 && limit > 0 {
		effectiveLimit = min(ds.limit, int(limit))
	} else if ds.limit > 0 {
		effectiveLimit = ds.limit
	} else if limit > 0 {
		effectiveLimit = int(limit)
	}

	if effectiveLimit == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", cols, loc)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", cols, loc, effectiveLimit)
	}

	rows, err := ds.db.QueryContext(ctx, query)
	if err != nil {
		logger.Errorw("Failed to execute query", "query", query, "error", err)
		return nil, fferr.NewInternalErrorf("Failed to execute query: %v", err)
	}

	return NewSqlIterator(ctx, rows, ds.converter, ds.schema), nil
}

func (ds *SqlDataset) Schema() types.Schema {
	return ds.schema
}

func (ds *SqlDataset) Len() (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", location.SanitizeFullyQualifiedObject(ds.location.TableLocation()))
	row := ds.db.QueryRow(query)

	var count int64
	if err := row.Scan(&count); err != nil {
		return -1, fferr.NewInternalErrorf("Failed to count rows: %v", err)
	}

	return count, nil
}

type SqlIterator struct {
	rows          *sql.Rows
	converter     types.ValueConverter[any]
	schema        types.Schema
	currentValues types.Row
	scanTargets   []any
	err           error
	ctx           context.Context
}

func NewSqlIterator(ctx context.Context, rows *sql.Rows, converter types.ValueConverter[any], schema types.Schema) *SqlIterator {
	columnCount := len(schema.Fields)
	scanTargets := make([]any, columnCount)

	for i := range scanTargets {
		var v any
		scanTargets[i] = &v
	}

	return &SqlIterator{
		rows:        rows,
		converter:   converter,
		schema:      schema,
		scanTargets: scanTargets,
		ctx:         ctx,
	}
}

func (it *SqlIterator) Values() types.Row {
	return it.currentValues
}

func (it *SqlIterator) Schema() types.Schema {
	return it.schema
}

func (it *SqlIterator) Columns() []string {
	return it.schema.ColumnNames()
}

func (it *SqlIterator) Err() error {
	return it.err
}

func (it *SqlIterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return fferr.NewInternalErrorf("Failed to close SQL rows: %v", err)
	}
	return nil
}

func (it *SqlIterator) Next() bool {
	// Check for context cancellation
	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
		it.Close()
		return false
	default:
	}

	if !it.rows.Next() {
		it.Close()
		return false
	}

	if it.scanTargets == nil {
		it.scanTargets = make([]any, len(it.schema.Fields))
		for i := range it.scanTargets {
			var v any
			it.scanTargets[i] = &v
		}
	}

	// Scan row data into scan targets
	if err := it.rows.Scan(it.scanTargets...); err != nil {
		it.err = fferr.NewExecutionError("SQL", err)
		it.Close()
		return false
	}

	// Allocate a new row for the result
	row := make(types.Row, len(it.scanTargets))

	// Convert values according to schema
	for i, rawPtr := range it.scanTargets {
		valPtr, ok := rawPtr.(*any)
		if !ok {
			it.err = fferr.NewInternalErrorf("unexpected scan target type at index %d: %T", i, rawPtr)
			it.Close()
			return false
		}
		val := *valPtr

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
