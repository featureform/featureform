package provider

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	_ "github.com/go-sql-driver/mysql"
)

type mySqlColumnType string

const (
	mySqlInt       mySqlColumnType = "integer"
	mySqlBigInt                    = "bigint"
	mySqlFloat                     = "float"
	mySqlString                    = "varchar"
	mySqlBool                      = "boolean"
	mySqlTimestamp                 = "timestamp"
)

func mySqlOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.MySqlConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, fmt.Errorf("invalid postgres config: %v", config)
	}
	queries := mySQLQueries{}
	queries.setVariableBinding(MySQLBindingStyle)
	sgConfig := SQLOfflineStoreConfig{
		Config:       config,
		Driver:       "mysql",
		ProviderType: pt.MySqlOffline,
		QueryImpl:    &queries,
	}
	if sc.Host != "" && sc.Port != "" {
		sgConfig.ConnectionURL = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", sc.Username, sc.Password, sc.Host, sc.Port, sc.Database)
	} else {
		sgConfig.ConnectionURL = fmt.Sprintf("%s:%s@/%s", sc.Username, sc.Password, sc.Database)
	}
	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type mySQLQueries struct {
	defaultOfflineSQLQueries
}

func (q mySQLQueries) tableExists() string {
	return "SELECT COUNT(*) FROM pg_tables WHERE  table_name  = $1"
}

func (q mySQLQueries) viewExists() string {
	return "SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?"
}

func (q mySQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query *sql.Stmt
	var err error
	if !timestamp {
		schema.TS = time.Now().UTC().Format("2006-01-02 15:04:05")
	}
	query, err = db.Prepare("CREATE VIEW ? AS SELECT ? as entity, ? as value, ? as ts FROM ?")
	if err != nil {
		return fmt.Errorf("error registering view: %w", err)
	}
	defer query.Close()
	fmt.Printf("Resource creation query: %v", query)
	_, err = query.Exec(tableName, schema.Entity, schema.Value, schema.TS, schema.SourceTable)
	return err
}

func (q mySQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", sanitize(tableName), sanitize(sourceName))
}

// materializationCreate satisfies the OfflineTableQueries interface.
// mySQL doesn't have materialized views.
func (q mySQLQueries) materializationCreate(tableName string, sourceName string) string {
	return q.primaryTableRegister(tableName, sourceName)
}

func (q mySQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	query := `DROP VIEW IF EXISTS ?;` + q.primaryTableCreate(tableName, sourceName)
	_, err := db.Exec(query, tableName)
	return err
}

func (q mySQLQueries) materializationExists() string {
	return "SELECT * FROM information_schema.tables	WHERE table_name = ? AND table_type = 'VIEW'"
}

func (q mySQLQueries) determineColumnType(valueType ValueType) (string, error) {
	switch valueType {
	case Int, Int32, Int64:
		return "INT", nil
	case Float32, Float64:
		return "FLOAT8", nil
	case String:
		return "VARCHAR", nil
	case Bool:
		return "BOOLEAN", nil
	case Timestamp:
		return "TIMESTAMP", nil
	case NilType:
		return "VARCHAR", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func (q mySQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (entity VARCHAR(255), value %s, ts TIMESTAMP, UNIQUE KEY(entity, ts))",
		name, columnType,
	)
}

func (q mySQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q mySQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q mySQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q mySQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
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
		fullQuery := fmt.Sprintf("CREATE TABLE %s AS (SELECT %s, l.value as label FROM %s ", tempName, columnStr, query)
		err := q.atomicUpdate(store.db, tableName, tempName, fullQuery)
		if err != nil {
			return err
		}
	} else {
		fullQuery := fmt.Sprintf("CREATE TABLE %s AS (SELECT %s, l.value as label FROM %s ", sanitize(tableName), columnStr, query)
		if _, err := store.db.Exec(fullQuery); err != nil {
			return err
		}
	}
	return nil
}

func (q mySQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	switch t {
	case mySqlInt:
		return int32(v.(int64))
	case mySqlBigInt:
		return int(v.(int64))
	case mySqlFloat:
		return v.(float64)
	case mySqlString:
		return v.(string)
	case mySqlBool:
		return v.(bool)
	case mySqlTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q mySQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return mySqlString
	case "int32":
		return mySqlInt
	case "int64":
		return mySqlBigInt
	case "float32", "float64", "interface {}":
		return mySqlFloat
	case "bool":
		return mySqlBool
	case "time.Time":
		return mySqlTimestamp
	}
	return mySqlString
}

func (q mySQLQueries) numRows(n interface{}) (int64, error) {
	return n.(int64), nil
}

func (q mySQLQueries) transformationCreate(name string, query string) string {
	return fmt.Sprintf("CREATE TABLE  %s AS %s", sanitize(name), query)
}

func (q mySQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	fullQuery := fmt.Sprintf("CREATE TABLE %s AS %s", tempName, query)
	return q.atomicUpdate(db, tableName, tempName, fullQuery)
}

func (q mySQLQueries) transformationExists() string {
	return "SELECT * FROM information_schema.tables	WHERE table_name = ? AND table_type = 'VIEW'"
}

func (q mySQLQueries) getColumns(db *sql.DB, tableName string) ([]TableColumn, error) {
	rows, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_name = ?", tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
	return columnNames, nil
}
