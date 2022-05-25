package provider

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
	"time"
)

type postgresColumnType string

const (
	pgInt       postgresColumnType = "integer"
	pgBigInt                       = "bigint"
	pgFloat                        = "float8"
	pgString                       = "varchar"
	pgBool                         = "boolean"
	pgTimestamp                    = "timestamp with time zone"
)

type PostgresConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

func (pg *PostgresConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pg)
	if err != nil {
		return err
	}
	return nil
}

func (pg *PostgresConfig) Serialize() []byte {
	conf, err := json.Marshal(pg)
	if err != nil {
		panic(err)
	}
	return conf
}

func postgresOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := PostgresConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid snowflake config")
	}
	queries := postgresSQLQueries{}
	queries.setVariableBinding(PostgresBindingStyle)
	sgConfig := SQLOfflineStoreConfig{
		Config:        config,
		ConnectionURL: fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", sc.Username, sc.Password, sc.Host, sc.Port, sc.Database),
		Driver:        "postgres",
		ProviderType:  PostgresOffline,
		QueryImpl:     &queries,
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type postgresSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q postgresSQLQueries) tableExists() string {
	return "SELECT COUNT(*) FROM pg_tables WHERE  tablename  = $1"
}

func (q postgresSQLQueries) viewExists() string {
	return "select count(*) from pg_views where viewname = $1"
}

func (q postgresSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, %s as ts FROM %s", sanitize(tableName),
			sanitize(schema.Entity), sanitize(schema.Value), sanitize(schema.TS), sanitize(schema.SourceTable))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, to_timestamp('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMPTZ as ts FROM %s", sanitize(tableName),
			sanitize(schema.Entity), sanitize(schema.Value), time.UnixMilli(0).UTC(), sanitize(schema.SourceTable))
	}
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (q postgresSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", sanitize(tableName), sanitize(sourceName))
}

func (q postgresSQLQueries) materializationCreate(tableName string, sourceName string) string {
	return fmt.Sprintf(
		"CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1);  CREATE UNIQUE INDEX ON %s (entity);", sanitize(tableName), sanitize(sourceName), sanitize(tableName))
}

func (q postgresSQLQueries) materializationUpdate(store *sqlOfflineStore, tableName string, sourceName string) error {
	_, err := store.db.Exec(fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", sanitize(tableName)))
	return err
}

func (q postgresSQLQueries) materializationExists() string {
	return fmt.Sprintf("SELECT * FROM pg_matviews WHERE matviewname = $1")
}

func (q postgresSQLQueries) determineColumnType(valueType ValueType) (string, error) {
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
		return "TIMESTAMPTZ", nil
	case NilType:
		return "VARCHAR", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func (q postgresSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMPTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
}

func (q postgresSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q postgresSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	columns := make([]string, 0)
	query := fmt.Sprintf(" (SELECT entity, value , ts from %s ) l ", sanitize(labelName))
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		if err != nil {
			return err
		}
		santizedName := sanitize(tableName)
		tableJoinAlias := fmt.Sprintf("t%d", i)
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT JOIN LATERAL (SELECT entity , value as %s, ts  FROM %s WHERE entity=l.entity and ts <= l.ts ORDER BY ts desc LIMIT 1) %s on %s.entity=l.entity ",
			query, santizedName, santizedName, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )", query)
		}
	}
	columnStr := strings.Join(columns, ", ")
	fullQuery := fmt.Sprintf("CREATE TABLE %s AS (SELECT %s, l.value as label FROM %s ", sanitize(tableName), columnStr, query)
	if _, err := store.db.Exec(fullQuery); err != nil {
		return err
	}
	return nil
}

func (q postgresSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	switch t {
	case pgInt:
		return int32(v.(int64))
	case pgBigInt:
		return int(v.(int64))
	case pgFloat:
		return v.(float64)
	case pgString:
		return v.(string)
	case pgBool:
		return v.(bool)
	case pgTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q postgresSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return pgString
	case "int32":
		return pgBigInt
	case "int64":
		return pgBigInt
	case "float32", "float64", "interface {}":
		return pgFloat
	case "bool":
		return pgBool
	case "time.Time":
		return pgTimestamp
	}
	return pgString
}

func (q postgresSQLQueries) numRows(n interface{}) (int64, error) {
	return n.(int64), nil
}

func (q postgresSQLQueries) transformationCreate(name string, query string) string {
	fmt.Println("Creating Table ", name)
	return fmt.Sprintf("CREATE TABLE  %s AS %s", sanitize(name), query)
}

func (q postgresSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {

	santizedName := sanitize(tableName)
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	oldName := sanitize(fmt.Sprintf("old_%s", tableName))
	transaction := fmt.Sprintf("BEGIN;"+
		"CREATE TABLE %s AS %s;"+
		"ALTER TABLE %s RENAME TO %s;"+
		"ALTER TABLE %s RENAME TO %s;"+
		"DROP TABLE %s;"+
		"COMMIT;", tempName, query, santizedName, oldName, tempName, santizedName, oldName)
	_, err := db.Exec(transaction)
	return err
}

func (q postgresSQLQueries) transformationExists() string {
	return fmt.Sprintf("SELECT * FROM pg_matviews WHERE matviewname = $1")
}

func (q postgresSQLQueries) getColumnNames(store *sql.DB, tableName string) (*sql.Rows, error) {
	qry := fmt.Sprintf("SELECT attname AS column_name FROM   pg_attribute WHERE  attrelid = 'public.%s'::regclass AND    attnum > 0 ORDER  BY attnum", tableName)
	return store.Query(qry)
}
