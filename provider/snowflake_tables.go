// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"

	"github.com/featureform/fferr"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
)

type snowflakePrimaryTable struct {
	sqlPrimaryTable
}

func (tbl snowflakePrimaryTable) Write(record GenericRecord) error {
	snowflakeQuery, isSnowflakeQuery := tbl.query.(*snowflakeSQLQueries)
	if !isSnowflakeQuery {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), fmt.Errorf("expected snowflakeSQLQueries type; received %T", tbl.query))
	}
	query := snowflakeQuery.genericInsert(tbl.sqlLocation.Location(), tbl.columns(), 1)
	_, err := tbl.db.Exec(query, record)
	if err != nil {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), err)
	}
	return nil
}

func (tbl snowflakePrimaryTable) WriteBatch(records []GenericRecord) error {
	snowflakeQuery, isSnowflakeQuery := tbl.query.(*snowflakeSQLQueries)
	if !isSnowflakeQuery {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), fmt.Errorf("expected snowflakeSQLQueries type; received %T", tbl.query))
	}
	query := snowflakeQuery.genericInsert(tbl.sqlLocation.Location(), tbl.columns(), len(records))

	var args []any
	for _, record := range records {
		args = append(args, record...)
	}

	_, err := tbl.db.Exec(query, args...)
	if err != nil {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), err)
	}
	return nil
}

func (tbl snowflakePrimaryTable) columns() []string {
	columns := make([]string, len(tbl.schema.Columns))
	for i, col := range tbl.schema.Columns {
		columns[i] = col.Name
	}
	return columns
}

func (tbl snowflakePrimaryTable) Location() string {
	return tbl.sqlLocation.Location()
}

type snowflakeOfflineTable struct {
	sqlOfflineTable
	location pl.Location
}

func (tbl snowflakeOfflineTable) Write(record ResourceRecord) error {
	snowflakeQuery, isSnowflakeQuery := tbl.query.(*snowflakeSQLQueries)
	if !isSnowflakeQuery {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), fmt.Errorf("expected snowflakeSQLQueries type; received %T", tbl.query))
	}
	query := snowflakeQuery.genericInsert(tbl.location.Location(), record.Columns(), 1)
	_, err := tbl.db.Exec(query, record)
	if err != nil {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), err)
	}
	return nil
}

func (tbl snowflakeOfflineTable) WriteBatch(records []ResourceRecord) error {
	if len(records) == 0 {
		return nil
	}
	snowflakeQuery, isSnowflakeQuery := tbl.query.(*snowflakeSQLQueries)
	if !isSnowflakeQuery {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), fmt.Errorf("expected snowflakeSQLQueries type; received %T", tbl.query))
	}
	query := snowflakeQuery.genericInsert(tbl.location.Location(), records[0].Columns(), len(records))

	var args []any
	for _, record := range records {
		args = append(args, record.Entity, record.Value, record.TS)
	}

	_, err := tbl.db.Exec(query, args...)
	if err != nil {
		return fferr.NewExecutionError(pt.SnowflakeOffline.String(), err)
	}
	return nil
}
