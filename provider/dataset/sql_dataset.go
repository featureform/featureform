package dataset

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/location"
)

type SqlDataset struct {
	db        *sql.DB
	location  location.SQLLocation
	schema    types.Schema
	converter types.ValueConverter[any]
	limit     int
}

func NewSqlDataset(
	db *sql.DB,
	location location.SQLLocation,
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
	location location.SQLLocation,
	converter types.ValueConverter[any],
	limit int,
) (SqlDataset, error) {
	schema, err := getSchema(db, converter, location.GetTable())
	if err != nil {
		return SqlDataset{}, err
	}

	return NewSqlDataset(db, location, schema, converter, limit)
}

// getSchema extracts schema information from the database
func getSchema(db *sql.DB, converter types.ValueConverter[any], tableName string) (types.Schema, error) {
	// This query is Snowflake-specific - might need adapter pattern for other DBs
	qry := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? AND table_schema = CURRENT_SCHEMA() ORDER BY ordinal_position"

	rows, err := db.Query(qry, tableName)
	if err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("table_name", tableName)
		return types.Schema{}, wrapped
	}
	defer rows.Close()

	fields := make([]types.ColumnSchema, 0)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			wrapped := fferr.NewExecutionError("SQL", err)
			wrapped.AddDetail("table_name", tableName)
			return types.Schema{}, wrapped
		}

		ok := converter.IsSupportedType(types.NativeType(dataType))
		if !ok {
			return types.Schema{}, fferr.NewInternalErrorf("Unknown native type: %v", dataType)
		}

		column := types.ColumnSchema{
			Name:       types.ColumnName(columnName),
			NativeType: types.NativeType(dataType),
		}
		fields = append(fields, column)
	}

	if err := rows.Err(); err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("table_name", tableName)
		return types.Schema{}, wrapped
	}

	return types.Schema{Fields: fields}, nil
}

func (ds SqlDataset) Location() location.Location {
	return &ds.location
}

func (ds SqlDataset) Iterator(ctx context.Context) (Iterator, error) {
	colNames := ds.schema.ColumnNames()
	tableName := ds.location.GetTable()
	cols := strings.Join(colNames[:], ", ")

	var query string
	if ds.limit == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", cols, tableName)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", cols, tableName, ds.limit)
	}

	rows, err := ds.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fferr.NewInternalErrorf("Failed to execute query: %v", err)
	}

	return NewSqlIterator(rows, ds.converter, ds.schema), nil
}

func (ds SqlDataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}

func (ds SqlDataset) ColumnNames() []string {
	return ds.schema.ColumnNames()
}

type SqlIterator struct {
	rows          *sql.Rows
	currentValues types.Row
	converter     types.ValueConverter[any]
	schema        types.Schema
	err           error
	closed        bool
	scanBuffers   []interface{} // Reusable buffer for SQL Scan
	valueBuffers  []interface{} // Holds scanned values before conversion
	rowBuffer     types.Row     // Reusable row buffer to avoid allocations on each Next call
}

func NewSqlIterator(rows *sql.Rows, converter types.ValueConverter[any], schema types.Schema) Iterator {
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
	}
}

func (it *SqlIterator) Values() types.Row {
	return it.currentValues
}

func (it *SqlIterator) Schema() (types.Schema, error) {
	return it.schema, nil
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
		convertedVal, err := it.converter.ConvertValue(it.schema.Fields[i].NativeType, val)
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
