// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	db "github.com/jackc/pgx/v4"

	"github.com/featureform/fferr"
	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	"github.com/featureform/provider/snowflake"
)

const CATALOG_CLAUSE = "CATALOG = 'SNOWFLAKE' "

type snowflakeSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q snowflakeSQLQueries) genericInsert(tableName string, columns []string, recordCount int) string {
	var sb strings.Builder
	valuePlaceholders := q.valuePlaceholders(len(columns), recordCount)

	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) ", sanitize(tableName), strings.Join(columns, ", ")))
	sb.WriteString(fmt.Sprintf("VALUES %s", valuePlaceholders))

	return sb.String()
}

func (q snowflakeSQLQueries) valuePlaceholders(columnCount, recordCount int) string {
	valuePlaceholders := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		valuePlaceholders[i] = "?"
	}
	valuesPlaceholder := strings.Join(valuePlaceholders, ", ")

	recordPlaceholders := make([]string, recordCount)
	for i := 0; i < recordCount; i++ {
		recordPlaceholders[i] = fmt.Sprintf("(%s)", valuesPlaceholder)
	}
	return strings.Join(recordPlaceholders, ", ")
}

func (q snowflakeSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}

func (q snowflakeSQLQueries) dynamicIcebergTableCreate(tableName, query string, config metadata.ResourceSnowflakeConfig) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE DYNAMIC ICEBERG TABLE %s ", sanitize(tableName)))

	if config.DynamicTableConfig.TargetLag != "DOWNSTREAM" {
		sb.WriteString(fmt.Sprintf("TARGET_LAG = '%s' ", config.DynamicTableConfig.TargetLag))
	} else {
		sb.WriteString("TARGET_LAG = DOWNSTREAM ")
	}

	sb.WriteString(fmt.Sprintf("WAREHOUSE = '%s' ", config.Warehouse))
	sb.WriteString(fmt.Sprintf("EXTERNAL_VOLUME = '%s' ", config.DynamicTableConfig.ExternalVolume))
	sb.WriteString(CATALOG_CLAUSE)
	sb.WriteString(fmt.Sprintf("BASE_LOCATION = '%s' ", config.DynamicTableConfig.BaseLocation))
	sb.WriteString(fmt.Sprintf("REFRESH_MODE = %s ", config.DynamicTableConfig.RefreshMode))
	sb.WriteString(fmt.Sprintf("INITIALIZE = %s ", config.DynamicTableConfig.Initialize))
	sb.WriteString(fmt.Sprintf("AS %s", query))

	return sb.String()
}

func (q snowflakeSQLQueries) staticIcebergTableCreate(tableName, query string, config metadata.ResourceSnowflakeConfig) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE ICEBERG TABLE %s ", sanitize(tableName)))
	sb.WriteString(fmt.Sprintf("EXTERNAL_VOLUME = '%s' ", config.DynamicTableConfig.ExternalVolume))
	sb.WriteString(CATALOG_CLAUSE)
	sb.WriteString(fmt.Sprintf("BASE_LOCATION = '%s' ", config.DynamicTableConfig.BaseLocation))
	// TODO: Investigate the following keywords:
	// * [ CATALOG_SYNC = '<open_catalog_integration_name>']
	// * [ STORAGE_SERIALIZATION_POLICY = { COMPATIBLE | OPTIMIZED } ]
	// * [ CHANGE_TRACKING = { TRUE | FALSE } ]
	sb.WriteString(fmt.Sprintf("AS %s", query))

	return sb.String()
}

func (q snowflakeSQLQueries) viewCreate(tableName, query string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE VIEW %s ", sanitize(tableName)))
	sb.WriteString(fmt.Sprintf("AS %s", query))

	return sb.String()
}

func (q snowflakeSQLQueries) materializationCreateAsQuery(entity, value, ts, tableName string) string {
	var sb strings.Builder

	tsSelectStmt := toIcebergTimestamp(ts)
	tsOrderByStmt := fmt.Sprintf("ORDER BY IDENTIFIER('%s') DESC", ts)
	if ts == "" {
		tsSelectStmt = fmt.Sprintf("to_timestamp_ntz('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP_NTZ(6) AS ts", time.UnixMilli(0).UTC())
		tsOrderByStmt = "ORDER BY ts DESC"
	}

	cteFormat := "WITH OrderedSource AS (SELECT IDENTIFIER('%s') AS entity, IDENTIFIER('%s') AS value, %s, ROW_NUMBER() OVER (PARTITION BY IDENTIFIER('%s') %s) AS rn FROM %s) "
	cteClause := fmt.Sprintf(cteFormat, entity, value, tsSelectStmt, entity, tsOrderByStmt, tableName)
	sb.WriteString(cteClause)
	sb.WriteString("SELECT entity, value, ts, ROW_NUMBER() OVER (ORDER BY (entity)) AS row_number FROM OrderedSource WHERE rn = 1")

	return sb.String()
}

func (q snowflakeSQLQueries) dropTableQuery(loc pl.SQLLocation) string {
	return fmt.Sprintf("DROP TABLE %s", SanitizeSnowflakeIdentifier(loc.TableLocation()))
}

func (q snowflakeSQLQueries) dropViewQuery(loc pl.SQLLocation) string {
	return fmt.Sprintf("DROP VIEW %s", SanitizeSnowflakeIdentifier(loc.TableLocation()))
}

func SanitizeSnowflakeIdentifier(obj pl.FullyQualifiedObject) string {
	ident := db.Identifier{}

	// Add database only if schema is present
	if obj.Database != "" && obj.Schema != "" {
		ident = append(ident, obj.Database, obj.Schema)
	}

	// Assume the default schema is "PUBLIC" if the database is present and schema is not
	if obj.Database != "" && obj.Schema == "" {
		ident = append(ident, "PUBLIC")
	}

	ident = append(ident, obj.Table)

	return ident.Sanitize()
}

func (q *snowflakeSQLQueries) getSchema(db *sql.DB, converter fftypes.ValueConverter[any], location pl.SQLLocation) (fftypes.Schema, error) {
	tblName := location.GetTable()
	schema := location.GetSchema()

	// In Snowflake, we need to get columns including precision and scale information
	query := `
		SELECT 
			column_name, 
			data_type, 
			numeric_precision, 
			numeric_scale,
		FROM 
			information_schema.columns 
		WHERE 
			table_name = ?
			`

	// Add schema condition only if present
	if schema != "" {
		query += " AND table_schema = ?"
	}

	// Add the ordering
	query += " ORDER BY ordinal_position"

	// Prepare parameters for the query
	var params []interface{}
	params = append(params, tblName)
	if schema != "" {
		params = append(params, schema)
	}

	logging.GlobalLogger.Infow("Getting Snowflake schema", "query", query, "table", tblName, "schema", schema)

	// Execute query with parameters
	rows, err := db.Query(query, params...)
	if err != nil {
		wrapped := fferr.NewExecutionError("Snowflake", err)
		wrapped.AddDetail("schema", schema)
		wrapped.AddDetail("table_name", tblName)
		return fftypes.Schema{}, wrapped
	}
	defer rows.Close()

	// Process result set
	fields := make([]fftypes.ColumnSchema, 0)
	for rows.Next() {
		var columnName, dataType string
		var numericPrecision, numericScale sql.NullInt64

		if err := rows.Scan(&columnName, &dataType, &numericPrecision, &numericScale); err != nil {
			wrapped := fferr.NewExecutionError("Snowflake", err)
			wrapped.AddDetail("schema", schema)
			wrapped.AddDetail("table_name", tblName)
			return fftypes.Schema{}, wrapped
		}

		snowflakeDetails := snowflake.NewNativeTypeDetails(dataType, numericPrecision, numericScale)
		nativeType, err := converter.ParseNativeType(snowflakeDetails)
		if err != nil {
			wrapped := fferr.NewInternalErrorf("could not parse native type: %v", err)
			wrapped.AddDetail("schema", schema)
			wrapped.AddDetail("table_name", tblName)
			wrapped.AddDetail("column", columnName)
			wrapped.AddDetail("data_type", dataType)
			return fftypes.Schema{}, wrapped
		}
		valueType, err := converter.GetType(nativeType)
		if err != nil {
			wrapped := fferr.NewInternalErrorf("could not convert native type to value type: %v", err)
			wrapped.AddDetail("schema", schema)
			wrapped.AddDetail("table_name", tblName)
			wrapped.AddDetail("column", columnName)
			wrapped.AddDetail("data_type", dataType)
			return fftypes.Schema{}, wrapped
		}

		// Append column details
		column := fftypes.ColumnSchema{
			Name:       fftypes.ColumnName(columnName),
			NativeType: nativeType,
			Type:       valueType,
		}
		fields = append(fields, column)
	}

	// Check for row iteration errors
	if err := rows.Err(); err != nil {
		wrapped := fferr.NewExecutionError("Snowflake", err)
		wrapped.AddDetail("schema", schema)
		wrapped.AddDetail("table_name", tblName)
		return fftypes.Schema{}, wrapped
	}

	return fftypes.Schema{Fields: fields}, nil
}

func toIcebergTimestamp(tsCol string) string {
	if tsCol != "" {
		return fmt.Sprintf("CAST(IDENTIFIER('%s') AS TIMESTAMP_NTZ(6)) AS ts ", tsCol)
	} else {
		return fmt.Sprintf("to_timestamp_ntz('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP_NTZ(6) AS ts ", time.UnixMilli(0).UTC())
	}
}
