package provider

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	_ "github.com/denisenkom/go-mssqldb"
)

type MSSQLColumnType string

const (
	MSSQLInt       MSSQLColumnType = "integer"
	MSSQLBigInt                    = "bigint"
	MSSQLFloat                     = "float"
	MSSQLString                    = "varchar"
	MSSQLBool                      = "boolean"
	MSSQLTimestamp                 = "datetime"
)

func MSSQLOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.MSSQLConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, fmt.Errorf("invalid MSSQL config: %v", config)
	}
	queries := MSSQLQueries{}
	queries.setVariableBinding(MSSQLBindingStyle)
	sgConfig := SQLOfflineStoreConfig{
		Config:       config,
		Driver:       "sqlserver",
		ProviderType: pt.MSSQLOffline,
		QueryImpl:    &queries,
	}
	if sc.Host != "" && sc.Port != "" {
		sgConfig.ConnectionURL = fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s", sc.Username, sc.Password, sc.Host, sc.Port, sc.Database)
	} else {
		sgConfig.ConnectionURL = fmt.Sprintf("sqlserver://%s:%s@?database=%s", sc.Username, sc.Password, sc.Database)
	}
	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type MSSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q MSSQLQueries) tableExists() string {
	return `IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @p1) SELECT 1 ELSE SELECT 0`
}

func (q MSSQLQueries) viewExists() string {
	return `IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = @p1) SELECT 1 ELSE SELECT 0`
}

func (q MSSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, %s as ts FROM %s", sanitize(tableName),
			sanitize(schema.Entity), sanitize(schema.Value), sanitize(schema.TS), sanitize(schema.SourceTable))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, CONVERT(datetimeOffset, '%s', 120) as ts FROM %s",
		sanitize(tableName), sanitize(schema.Entity), sanitize(schema.Value), time.Now().UTC().Format("2006-01-02 15:04:05"), sanitize(schema.SourceTable))
	}
	fmt.Printf("Resource creation query: %s", query)

	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (q MSSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", sanitize(tableName), sanitize(sourceName))
}

//Indexed Views implementation of Materialization as Materialized Views are not supported in MSSQL
func (q MSSQLQueries) materializationCreate(tableName string, sourceName string) string {
    return fmt.Sprintf(
        "IF NOT EXISTS (SELECT * FROM sys.views WHERE name = '%s') "+
            "BEGIN "+
            "CREATE VIEW %s WITH SCHEMABINDING AS "+
            "SELECT entity, value, ts, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_number FROM "+
            "(SELECT entity, ts, value, ROW_NUMBER() OVER (PARTITION BY entity ORDER BY ts desc) "+
            "AS rn FROM %s) t WHERE rn=1 "+
            "CREATE UNIQUE CLUSTERED INDEX IDX_%s ON %s (entity) "+
            "END", sanitize(tableName), sanitize(tableName), sanitize(sourceName), sanitize(tableName), sanitize(tableName))
}

//Automatic Update is not supported in MSSQL, so we need to manually drop the view and recreate it
func (q MSSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
    dropViewQuery := fmt.Sprintf("IF EXISTS (SELECT * FROM sys.views WHERE name = '%s') DROP VIEW %s", sanitize(tableName), sanitize(tableName))
    _, err := db.Exec(dropViewQuery)
    if err != nil {
        return err
    }
    createViewQuery := q.materializationCreate(tableName, sourceName)
    _, err = db.Exec(createViewQuery)
    return err
}

func (q MSSQLQueries) materializationExists() string {
    return "SELECT * FROM sys.views WHERE name = @p1"
}

func (q MSSQLQueries) determineColumnType(valueType ValueType) (string, error) {
	switch valueType {
	case Int, Int32, Int64:
		return "INT", nil
	case Float32, Float64:
		return "FLOAT", nil
	case String:
		return "VARCHAR", nil
	case Bool:
		return "BOOLEAN", nil
	case Timestamp:
		return "DATETIME", nil
	case NilType:
		return "VARCHAR", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func (q MSSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (entity VARCHAR(255), value %s, ts DATETIME, PRIMARY KEY(entity, ts))",
		name, columnType,
	)
}

func (q MSSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("@p%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q MSSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q MSSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q MSSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	columns := make([]string, 0)
	query := fmt.Sprintf("(SELECT entity, value , ts from %s ) l", sanitize(labelName))
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		if err != nil {
			return err
		}
		santizedName := sanitize(tableName)
		tableJoinAlias := fmt.Sprintf("t%d", i)
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT JOIN (SELECT entity, value AS %s, ts FROM %s "+
			"WHERE entity=l.entity AND ts <= l.ts ORDER BY ts DESC LIMIT 1) AS %s ON %s.entity=l.entity",
			query, santizedName, santizedName, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )", query)
		}
	}
	columnStr := strings.Join(columns, ", ")

	if isUpdate {
		tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
		fullQuery := fmt.Sprintf("SELECT INTO %s AS (SELECT %s, l.value as label FROM %s ", tempName, columnStr, query)
		err := q.atomicUpdate(store.db, tableName, tempName, fullQuery)
		if err != nil {
			return err
		}
	} else {
		fullQuery := fmt.Sprintf("SELECT INTO %s AS (SELECT %s, l.value as label FROM %s ", sanitize(tableName), columnStr, query)
		if _, err := store.db.Exec(fullQuery); err != nil {
			return err
		}
	}
	return nil
}

func (q MSSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	switch t {
	case MSSQLInt:
		return int32(v.(int64))
	case MSSQLBigInt:
		return int(v.(int64))
	case MSSQLFloat:
		return v.(float64)
	case MSSQLString:
		return v.(string)
	case MSSQLBool:
		return v.(bool)
	case MSSQLTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q MSSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return MSSQLString
	case "int32":
		return MSSQLInt
	case "int64":
		return MSSQLBigInt
	case "float32", "float64", "interface {}":
		return MSSQLFloat
	case "bool":
		return MSSQLBool
	case "time.Time":
		return MSSQLTimestamp
	}
	return MSSQLString
}

func (q MSSQLQueries) numRows(n interface{}) (int64, error) {
	return n.(int64), nil
}

func (q MSSQLQueries) transformationCreate(name string, query string) string {
	return fmt.Sprintf("SELECT INTO %s AS %s", sanitize(name), query)
}

func (q MSSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	fullQuery := fmt.Sprintf("SELECT INTO %s AS %s", tempName, query)
	return q.atomicUpdate(db, tableName, tempName, fullQuery)
}

func (q MSSQLQueries) transformationExists() string {
	return "SELECT * FROM information_schema.tables	WHERE table_name = ? AND table_type = 'VIEW'"
}

func (q MSSQLQueries) getColumns(db *sql.DB, tableName string) ([]TableColumn, error) {
	qry := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1"
	rows, err := db.QueryContext(context.Background(), qry, sql.Named("p1", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("encountered an error during iteration: %w", err)
	}
	
	return columnNames, nil
}
