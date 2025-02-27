package snowflake

import (
	"context"
	"database/sql"

	types "github.com/featureform/fftypes"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
)

type Dataset struct {
	location location.SQLLocation
	schema   types.Schema
}

type SqlPrimaryToDatasetAdapter struct {
	sqlPrimaryTable *sqlPrimaryTable
}

func NewSnowflakeDataset(location location.SQLLocation, schema types.Schema) Dataset {
	return Dataset{location: location, schema: schema}
}

func (ds Dataset) Location() location.Location {
	return &ds.location
}

func (ds Dataset) Iterator(ctx context.Context) (dataset.Iterator, error) {
	/**
		columns, err := pt.query.getColumns(pt.db, pt.name)
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, sanitize(col.Name))
	}
	names := strings.Join(columnNames[:], ", ")
	var query string
	if n == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", names, sanitize(pt.name))
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", names, sanitize(pt.name), n)
	}
	rows, err := pt.db.Query(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.providerType.String(), err)
		wrapped.AddDetail("table_name", pt.name)
		return nil, wrapped
	}
	colTypes, err := pt.getValueColumnTypes(pt.name)
	if err != nil {
		return nil, err
	}
	return newsqlGenericTableIterator(rows, colTypes, columnNames, pt.query, pt.providerType), nil
	*/
	return Iterator{}
}

func (ds Dataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}

type Iterator struct {
	rows   sql.Rows
	schema types.Schema
}
