// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"
	"strings"
	"time"

	db "github.com/jackc/pgx/v4"

	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
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
	obj := loc.TableLocation()
	return fmt.Sprintf("DROP TABLE %s", pl.SanitizeFullyQualifiedObject(obj))
}

func (q snowflakeSQLQueries) dropViewQuery(loc pl.SQLLocation) string {
	obj := loc.TableLocation()
	return fmt.Sprintf("DROP VIEW %s", pl.SanitizeFullyQualifiedObject(obj))
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

func toIcebergTimestamp(tsCol string) string {
	if tsCol != "" {
		return fmt.Sprintf("CAST(IDENTIFIER('%s') AS TIMESTAMP_NTZ(6)) AS ts ", tsCol)
	} else {
		return fmt.Sprintf("to_timestamp_ntz('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP_NTZ(6) AS ts ", time.UnixMilli(0).UTC())
	}
}
