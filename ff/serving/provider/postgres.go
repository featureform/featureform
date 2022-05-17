package provider

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
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
		return nil, errors.New("invalid sql config")
	}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid snowflake config")
	}
	sgConfig := postgresSQLConfig{
		Username:   sc.Username,
		Password:   sc.Password,
		Host:       sc.Host,
		Port:       sc.Port,
		Database:   sc.Database,
		serialized: config,
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type postgresSQLConfig struct {
	Host       string
	Port       string
	Username   string
	Password   string
	Database   string
	serialized SerializedConfig
}

func (c postgresSQLConfig) getSerialized() SerializedConfig {
	return c.serialized
}

func (c postgresSQLConfig) isSQLOfflineStore() bool {
	return true
}

func (c postgresSQLConfig) getConnectionUrl() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", c.Username, c.Password, c.Host, c.Port, c.Database)
}

func (c postgresSQLConfig) getDriver() string {
	return "postgres"
}

func (c postgresSQLConfig) getProviderType() Type {
	return SnowflakeOffline
}

func (c postgresSQLConfig) getQueries() OfflineTableQueries {
	return postgresSQLQueries{}
}

type postgresSQLQueries struct{}

func (q postgresSQLQueries) tableExists() string {
	return "SELECT COUNT(*) FROM pg_tables WHERE  tablename  = $1"
	//"SELECT 1 FROM information_schema.tables WHERE table_name= $1"
}

func (q postgresSQLQueries) registerResourcesWithoutTS(db *sql.DB, tableName string, schema ResourceSchema) error {
	query := fmt.Sprintf("CREATE TABLE %s AS SELECT %s as entity, %s as value, null::TIMESTAMPTZ as ts FROM %s; ALTER TABLE %s ADD CONSTRAINT  %s  UNIQUE (entity, ts)", sanitize(tableName),
		sanitize(schema.Entity), sanitize(schema.Value), sanitize(schema.SourceTable), sanitize(tableName), sanitize(uuid.NewString()))
	if _, err := db.Exec(query); err != nil {
		return err
	}
	// Populates empty column with timestamp
	update := fmt.Sprintf("UPDATE %s SET ts = $1", sanitize(tableName))
	if _, err := db.Exec(update, time.UnixMilli(0).UTC()); err != nil {
		return err
	}
	return nil
}
func (q postgresSQLQueries) registerResourcesWithTS(db *sql.DB, tableName string, schema ResourceSchema) error {
	query := fmt.Sprintf("CREATE TABLE %s AS SELECT %s as entity, %s as value, %s as ts FROM %s; ALTER TABLE %s ADD CONSTRAINT  %s  UNIQUE (entity, ts)", sanitize(tableName),
		sanitize(schema.Entity), sanitize(schema.Value), sanitize(schema.TS), sanitize(schema.SourceTable), sanitize(tableName), sanitize(uuid.NewString()))
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}
func (q postgresSQLQueries) primaryTableFromTable(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", sanitize(tableName), sanitize(sourceName))
}
func (q postgresSQLQueries) getColumnNames() string {
	return "SELECT column_name FROM information_schema.columns WHERE table_name = $1 order by ordinal_position"
}
func (q postgresSQLQueries) primaryTableCreate(name string, columnString string) string {
	return fmt.Sprintf("CREATE TABLE %s ( %s )", sanitize(name), columnString)
}
func (q postgresSQLQueries) materializationCreate(tableName string, resultName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1)", sanitize(tableName), sanitize(resultName))
}
func (q postgresSQLQueries) materializationGet() string {
	return "SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name= $1"
}

func (q postgresSQLQueries) materializationDelete(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}

func (q postgresSQLQueries) materializationExists() string {
	return "SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name= $1"
}

func (q postgresSQLQueries) trainingRowSelect(columns string, trainingSetName string) string {
	return fmt.Sprintf("SELECT %s FROM %s", columns, sanitize(trainingSetName))
}

func (q postgresSQLQueries) getValueColumnTypes(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s", sanitize(tableName))
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

func (q postgresSQLQueries) resourceExists(tableName string) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM %s WHERE entity= $1 AND ts= $2 ", sanitize(tableName))
}
func (q postgresSQLQueries) writeUpdate(table string) string {
	return fmt.Sprintf("UPDATE %s SET value=$1 WHERE entity=$2 AND ts=$3 ", table)
}
func (q postgresSQLQueries) writeInserts(table string) string {
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES ($1, $2, $3)", table)
}
func (q postgresSQLQueries) writeExists(table string) string {
	return fmt.Sprintf("SELECT COUNT (*) FROM %s WHERE entity=$1 AND ts=$2", table)
}

func (q postgresSQLQueries) materializationIterateSegment(tableName string) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM %s WHERE row_number>$1 AND row_number<=$2)t1", sanitize(tableName))
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
		resourceTableName := sanitize(store.getResourceTableName(feature))
		tableJoinAlias := fmt.Sprintf("t%d", i)
		columns = append(columns, resourceTableName)
		query = fmt.Sprintf("%s LEFT JOIN LATERAL (SELECT entity , value as %s, ts  FROM %s WHERE entity=l.entity and ts <= l.ts ORDER BY ts desc LIMIT 1) %s on %s.entity=l.entity ",
			query, resourceTableName, resourceTableName, tableJoinAlias, tableJoinAlias)
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
	return fmt.Sprintf("CREATE TABLE %s AS %s ", sanitize(name), query)
}
