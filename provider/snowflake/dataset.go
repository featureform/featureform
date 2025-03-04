package snowflake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/provider"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
)

type Dataset struct {
	db       *sql.DB
	location location.SQLLocation
	query    provider.OfflineTableQueries
	schema   types.Schema
}

func NewDataset(location location.SQLLocation, schema types.Schema) Dataset {
	return Dataset{location: location, schema: schema}
}

func (ds Dataset) Location() location.Location {
	return &ds.location
}

func (ds Dataset) getSchema(tableName string) (types.Schema, error) {
	qry := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? AND table_schema = CURRENT_SCHEMA() ORDER BY ordinal_position"

	rows, err := ds.db.Query(qry, tableName)
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

	schema := types.Schema{
		Fields: fields,
	}

	return schema, nil
}

func (ds Dataset) Iterator(limit int, ctx context.Context) (dataset.Iterator, error) {
	var query string
	colNames := ds.schema.ColumnNames()
	tableName := ds.location.GetTable()
	if limit == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", colNames, tableName)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", colNames, tableName, limit)
	}
	rows, err := ds.db.Query(query)
	if err != nil {
		return nil, fferr.NewExecutionError("SQL", err)
	}
	return NewIterator(rows, ds.schema), nil
}

func (ds Dataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}

// Iterator implements the dataset.Iterator interface for Snowflake
type Iterator struct {
	rows          *sql.Rows
	currentValues types.Row
	schema        types.Schema
	err           error
}

// NewIterator creates a new Iterator from sql.Rows and schema
func NewIterator(rows *sql.Rows, schema types.Schema) dataset.Iterator {
	return &Iterator{
		rows:   rows,
		schema: schema,
	}
}

// Values returns the current row values
func (it *Iterator) Values() types.Row {
	return it.currentValues
}

// Schema returns the dataset schema
func (it *Iterator) Schema() (types.Schema, error) {
	return it.schema, nil
}

// Err returns any error that occurred during iteration
func (it *Iterator) Err() error {
	return it.err
}

// Close closes the underlying database rows
func (it *Iterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return fferr.NewConnectionError("Snowflake", err)
	}
	return nil
}

func (it *Iterator) Next(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		it.rows.Close()
		return false
	default:
		// Continue with normal operation
	}

	if !it.rows.Next() {
		it.rows.Close()
		return false
	}

	columns, err := it.rows.Columns()
	if err != nil {
		it.err = fferr.NewExecutionError("Snowflake", err)
		it.rows.Close()
		return false
	}

	serializer := Serializer{}

	values := make([]interface{}, len(columns))
	pointers := make([]interface{}, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := it.rows.Scan(pointers...); err != nil {
		it.err = fferr.NewExecutionError("Snowflake", err)
		it.rows.Close()
		return false
	}

	row := make(types.Row, len(columns))
	for i, val := range values {
		if i >= len(it.schema.Fields) {
			// Handle case where schema doesn't match result set
			it.err = fferr.NewInternalErrorf("column count mismatch between schema and result set")
			it.rows.Close()
			return false
		}

		// Determine the ValueType for this field
		var fieldType types.ValueType
		fieldType = SnowflakeTypeMap[string(it.schema.Fields[i].NativeType)]

		if val == nil {
			// Handle NULL values
			row[i] = types.Value{
				NativeType: it.schema.Fields[i].NativeType,
				Value:      nil,
			}
			continue
		}

		convertedVal, err := serializer.Deserialize(fieldType, val)
		if err != nil {
			it.err = fferr.NewExecutionError("Snowflake", fmt.Errorf("serialization error for column %s: %w", columns[i], err))
			it.rows.Close()
			return false
		}

		row[i] = types.Value{
			NativeType: it.schema.Fields[i].NativeType,
			Value:      convertedVal,
		}
	}

	it.currentValues = row
	return true
}
