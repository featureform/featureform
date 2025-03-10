package dataset

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/location"
)

type SqlDataset struct {
	db       *sql.DB
	location location.SQLLocation
	schema   types.Schema
	mapper   types.NativeToValueTypeMapper
	limit    int
}

func NewSqlDataset(db *sql.DB, location location.SQLLocation, limit int) (SqlDataset, error) {
	var lmt = limit
	if limit == 0 {
		lmt = -1
	}
	schema, err := getSchema(db, location.GetTable())
	if err != nil {
		return SqlDataset{}, err
	}
	return SqlDataset{location: location, schema: schema, limit: lmt}, nil
}

func getSchema(db *sql.DB, tableName string) (types.Schema, error) {
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

func (ds SqlDataset) Location() location.Location {
	return &ds.location
}

func (ds SqlDataset) Iterator(ctx context.Context) (Iterator, error) {
	var query string
	colNames := ds.schema.ColumnNames()
	tableName := ds.location.GetTable()
	if ds.limit == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", colNames, tableName)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", colNames, tableName, ds.limit)
	}
	rows, err := ds.db.Query(query)
	if err != nil {
		return nil, fferr.NewExecutionError("SQL", err)
	}
	return NewSqlIterator(rows, ds.mapper, ds.schema), nil
}

func (ds SqlDataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}

type SqlIterator struct {
	rows          *sql.Rows
	currentValues types.Row
	mapper        types.NativeToValueTypeMapper
	schema        types.Schema
	err           error
}

func NewSqlIterator(rows *sql.Rows, nativeTypeToValueType types.NativeToValueTypeMapper, schema types.Schema) Iterator {
	return &SqlIterator{
		rows:   rows,
		mapper: nativeTypeToValueType,
		schema: schema,
	}
}

func (it *SqlIterator) Values() types.Row {
	return it.currentValues
}

func (it *SqlIterator) Schema() (types.Schema, error) {
	return it.schema, nil
}

func (it *SqlIterator) Err() error {
	return it.err
}

func (it *SqlIterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return fferr.NewConnectionError("Snowflake", err)
	}
	return nil
}

func (it *SqlIterator) Next(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		it.rows.Close()
		return false
	default:
		// continue
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

		var fieldType types.ValueType
		fieldType = it.mapper[it.schema.Fields[i].NativeType]

		if val == nil {
			// Handle NULL values
			row[i] = types.Value{
				NativeType: it.schema.Fields[i].NativeType,
				Value:      nil,
			}
			continue
		}

		serializer := types.GenericSerializer{}
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
