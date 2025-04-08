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
}

func NewSqlDataset(
	db *sql.DB,
	location *location.SQLLocation,
	schema types.Schema,
	converter types.ValueConverter[any],
	limit int,
) (SqlDataset, error) {
	lmt := limit
	if limit <= 0 {
		lmt = -1
	}

	return SqlDataset{
		db:        db,
		location:  location,
		schema:    schema,
		converter: converter,
		limit:     lmt,
	}, nil
}

// NewSqlDatasetWithAutoSchema creates a new SQL dataset with auto-detected schema
func NewSqlDatasetWithAutoSchema(
	db *sql.DB,
	location *location.SQLLocation,
	converter types.ValueConverter[any],
	limit int,
) (SqlDataset, error) {
	schema, err := getSchema(db, converter, location)
	if err != nil {
		return SqlDataset{}, err
	}

	return NewSqlDataset(db, location, schema, converter, limit)
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

func (ds SqlDataset) Location() location.Location {
	return ds.location
}

func (ds SqlDataset) Iterator(ctx context.Context) (Iterator, error) {
	logger := logging.GetLoggerFromContext(ctx)
	schema := ds.Schema()

	columnNames := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		columnNames[i] = postgres.Sanitize(string(field.Name))
	}

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

func (ds SqlDataset) Schema() types.Schema {
	return ds.schema
}

type SqlIterator struct {
	rows          *sql.Rows
	converter     types.ValueConverter[any]
	schema        types.Schema
	currentValues types.Row
	err           error
	closed        bool
	ctx           context.Context
}

func NewSqlIterator(ctx context.Context, rows *sql.Rows, converter types.ValueConverter[any], schema types.Schema) *SqlIterator {
	return &SqlIterator{
		rows:      rows,
		converter: converter,
		schema:    schema,
		ctx:       ctx,
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
	it.closed = true
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

	// Scan directly into new interfaces
	rawValues := make([]any, len(columns))
	for i := range rawValues {
		var v interface{}
		rawValues[i] = &v
	}

	if err := it.rows.Scan(rawValues...); err != nil {
		it.err = fferr.NewExecutionError("SQL", err)
		it.Close()
		return false
	}

	row := make(types.Row, len(rawValues))
	for i, rawPtr := range rawValues {
		val := *(rawPtr.(*interface{}))
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
