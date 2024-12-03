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

	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	db "github.com/jackc/pgx/v4"
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

func (q snowflakeSQLQueries) dynamicIcebergTableCreate(tableName, query string, config metadata.ResourceSnowflakeConfig) (string, error) {
	if err := config.Validate(); err != nil {
		return "", err
	}

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

	return sb.String(), nil
}

func (q snowflakeSQLQueries) resourceTableAsQuery(schema ResourceSchema, hasTimestamp bool) (string, error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("SELECT IDENTIFIER('%s') AS entity, IDENTIFIER('%s') AS value, ", schema.Entity, schema.Value))

	if hasTimestamp {
		sb.WriteString(fmt.Sprintf("CAST(IDENTIFIER('%s') AS TIMESTAMP_NTZ(6)) AS ts ", schema.TS))
	} else {
		sb.WriteString(fmt.Sprintf("to_timestamp_ntz('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP_NTZ(6) AS ts ", time.UnixMilli(0).UTC()))
	}

	sqlLoc, isSQLLocation := schema.SourceTable.(*pl.SQLLocation)
	if !isSQLLocation {
		return "", fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}

	// NOTE: We need to use TableLocation() here to get the correct table name as we cannot assume the table
	// is in the same database/schema as the current context.
	sb.WriteString(fmt.Sprintf("FROM TABLE('%s')", SanitizeSnowflakeIdentifier(sqlLoc.TableLocation())))

	return sb.String(), nil
}

// TODO: (Erik) Determine whether the query without the timestamp is correct
func (q snowflakeSQLQueries) materializationCreateAsQuery(entity, value, ts, tableName string) string {
	var sb strings.Builder

	tsSelectStmt := fmt.Sprintf("CAST(IDENTIFIER('%s') AS TIMESTAMP_NTZ(6)) AS ts", ts)
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

func (q snowflakeSQLQueries) trainingSetCreateAsQuery(def TrainingSetDef) (string, error) {
	var cteClauses []string
	var joinClauses []string
	var selectFieldsCte []string
	var selectFields []string
	var partitionFields []string

	labelLocation, isSQLLocation := def.LabelSourceMapping.Location.(*pl.SQLLocation)
	if !isSQLLocation {
		return "", fferr.NewInvalidArgumentErrorf("label source location is not an SQL location")
	}

	// Label CTE
	labelCTE := fmt.Sprintf(`label_cte AS (
		SELECT entity AS e, value AS label, ts AS time
		FROM %s
	)`, SanitizeSnowflakeIdentifier(labelLocation.TableLocation()))
	cteClauses = append(cteClauses, labelCTE)

	// Feature CTE(s) and JOIN(s)
	for i, feature := range def.FeatureSourceMappings {
		featureLocation, isSQLLocation := feature.Location.(*pl.SQLLocation)
		if !isSQLLocation {
			return "", fferr.NewInvalidArgumentErrorf("label source location is not an SQL location")
		}
		cteName := fmt.Sprintf("feature_%d_cte", i+1)
		cte := fmt.Sprintf(`%s AS (
			SELECT entity, value AS %s, ts
			FROM %s
		)`, cteName, sanitize(featureLocation.GetTable()), SanitizeSnowflakeIdentifier(featureLocation.TableLocation()))

		cteClauses = append(cteClauses, cte)

		joinClause := fmt.Sprintf(`LEFT JOIN %s f%d ON f%d.entity = l.e AND f%d.ts <= l.time`, cteName, i+1, i+1, i+1)
		joinClauses = append(joinClauses, joinClause)

		selectField := sanitize(featureLocation.GetTable())

		selectFields = append(selectFields, selectField)
		selectFieldsCte = append(selectFieldsCte, fmt.Sprintf("f%d.%s", i+1, selectField))
		partitionFields = append(partitionFields, fmt.Sprintf("f%d.%s", i+1, sanitize(featureLocation.GetTable())))
	}

	ctePart := strings.Join(cteClauses, ",\n")
	joinPart := strings.Join(joinClauses, "\n")
	selectCtePart := strings.Join(selectFieldsCte, ", ")
	selectPart := strings.Join(selectFields, ", ")

	partitionByClause := "l.e, l.label, l.time"
	if len(partitionFields) > 0 {
		partitionByClause = "l.e, l.label, l.time, " + strings.Join(partitionFields, ", ")
	}

	query := fmt.Sprintf(`
		WITH %s,
		combined_cte AS (
			SELECT l.e,
			       l.label,
			       l.time,
			       %s,
			       ROW_NUMBER() OVER (PARTITION BY %s ORDER BY l.time DESC) AS rn
			FROM label_cte l
			%s
		)
		SELECT %s, label
		FROM combined_cte
		WHERE rn = 1
	`, ctePart, selectCtePart, partitionByClause, joinPart, selectPart)

	return query, nil
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
