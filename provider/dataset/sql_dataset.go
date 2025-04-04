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
	"github.com/featureform/logging"
	"github.com/featureform/provider/location"
)

type SqlDataset struct {
	db        *sql.DB
	location  *location.SQLLocation
	schema    types.Schema
	converter types.ValueConverter[any]
	limit     int
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

func NewSqlDatasetWithAutoSchema(
	db *sql.DB,
	location *location.SQLLocation,
	converter types.ValueConverter[any],
	limit int,
) (*SqlDataset, error) {
	schema, err := getSchema(db, converter, location)
	if err != nil {
		return nil, err
	}
	return NewSqlDataset(db, location, schema, converter, limit)
}

func (ds *SqlDataset) WithLimit(limit int) *SqlDataset {
	ds.limit = limit
	return ds
}

func (ds *SqlDataset) Location() location.Location {
	return ds.location
}

func (ds *SqlDataset) Iterator(ctx context.Context) (Iterator, error) {
	logger := logging.GetLoggerFromContext(ctx)
	schema := ds.Schema()
	columnNames := schema.SanitizedColumnNames()
	cols := strings.Join(columnNames, ", ")
	loc := ds.location.Sanitized()

	var query string
	if ds.limit == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", cols, loc)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", cols, loc, ds.limit)
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

// getSchema extracts schema information from the database
func getSchema(db *sql.DB, converter types.ValueConverter[any], tableName *location.SQLLocation) (types.Schema, error) {
	// Extract schema and table name
	tblName := tableName.GetTable()
	schema := tableName.GetSchema()

	// Corrected Query: Ensure both `table_name` and `table_schema` are matched
	qry := `SELECT column_name, data_type 
	        FROM information_schema.columns 
	        WHERE table_name = ? 
	        AND table_schema = ? 
	        ORDER BY ordinal_position`

	// Execute query with both parameters
	rows, err := db.Query(qry, tblName, schema)
	if err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("schema", schema)
		wrapped.AddDetail("table_name", tblName)
		return types.Schema{}, wrapped
	}
	defer rows.Close()

	// Process result set
	fields := make([]types.ColumnSchema, 0)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			wrapped := fferr.NewExecutionError("SQL", err)
			wrapped.AddDetail("schema", schema)
			wrapped.AddDetail("table_name", tblName)
			return types.Schema{}, wrapped
		}

		// Ensure the type is supported
		valueType, err := converter.GetType(types.NativeType(dataType))
		if err != nil {
			wrapped := fferr.NewInternalErrorf("could not convert native type to value type: %v", err)
			wrapped.AddDetail("schema", schema)
			wrapped.AddDetail("table_name", tblName)
			return types.Schema{}, wrapped
		}

		// Append column details
		column := types.ColumnSchema{
			Name:       types.ColumnName(columnName),
			NativeType: types.NativeType(dataType),
			Type:       valueType,
		}
		fields = append(fields, column)
	}

	// Check for row iteration errors
	if err := rows.Err(); err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("schema", schema)
		wrapped.AddDetail("table_name", tblName)
		return types.Schema{}, wrapped
	}

	return types.Schema{Fields: fields}, nil
}

type SqlIterator struct {
	rows          *sql.Rows
	currentValues types.Row
	converter     types.ValueConverter[any]
	schema        types.Schema
	err           error
	closed        bool
	scanBuffers   []any     // Reusable buffer for SQL Scan
	valueBuffers  []any     // Holds scanned values before conversion
	rowBuffer     types.Row // Reusable row buffer to avoid allocations on each Next call

	ctx context.Context
}

func NewSqlIterator(ctx context.Context, rows *sql.Rows, converter types.ValueConverter[any], schema types.Schema) *SqlIterator {
	// Pre-allocate buffers for scanning
	columnCount := len(schema.Fields)
	valueBuffers := make([]interface{}, columnCount)
	scanBuffers := make([]interface{}, columnCount)
	for i := range valueBuffers {
		scanBuffers[i] = &valueBuffers[i]
	}

	// Pre-allocate the row buffer to avoid allocation on each Next() call
	rowBuffer := make(types.Row, columnCount)

	return &SqlIterator{
		rows:         rows,
		converter:    converter,
		schema:       schema,
		scanBuffers:  scanBuffers,
		valueBuffers: valueBuffers,
		rowBuffer:    rowBuffer,
		ctx:          ctx,
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
	if it.closed {
		return false
	}

	if !it.rows.Next() {
		it.Close()
		return false
	}

	// Verify the column count
	columns, err := it.rows.Columns()
	if err != nil {
		it.err = fferr.NewExecutionError("SQL", err)
		it.Close()
		return false
	}

	if len(columns) != len(it.schema.Fields) {
		it.err = fferr.NewInternalErrorf("column count mismatch: schema has %d, query returned %d",
			len(it.schema.Fields), len(columns))
		it.Close()
		return false
	}

	// Scan row data into pre-allocated buffers
	if err := it.rows.Scan(it.scanBuffers...); err != nil {
		it.err = fferr.NewExecutionError("SQL", err)
		it.Close()
		return false
	}

	// Convert values according to schema, reusing the pre-allocated row buffer
	// This avoids allocating a new slice on each Next() call, reducing GC pressure
	// especially important for large datasets with many rows
	for i, val := range it.valueBuffers {
		nativeType := it.schema.Fields[i].NativeType
		convertedVal, err := it.converter.ConvertValue(nativeType, val)
		if err != nil {
			it.err = err
			it.Close()
			return false
		}
		it.rowBuffer[i] = convertedVal
	}

	it.currentValues = it.rowBuffer
	return true
}
