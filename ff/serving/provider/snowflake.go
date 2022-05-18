package provider

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/snowflakedb/gosnowflake"
	"strconv"
	"strings"
	"time"
)

// sqlColumnType is used to specify the column type of a resource value.
type snowflakeColumnType string

const (
	sfInt       snowflakeColumnType = "integer"
	sfNumber                        = "NUMBER"
	sfFloat                         = "FLOAT"
	sfString                        = "varchar"
	sfBool                          = "BOOLEAN"
	sfTimestamp                     = "TIMESTAMP_NTZ"
)

type SnowflakeConfig struct {
	Username     string
	Password     string
	Organization string
	Account      string
	Database     string
}

func (sf *SnowflakeConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, sf)
	if err != nil {
		return err
	}
	return nil
}

func (sf *SnowflakeConfig) Serialize() []byte {
	conf, err := json.Marshal(sf)
	if err != nil {
		panic(err)
	}
	return conf
}

func snowflakeOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid snowflake config")
	}
	sgConfig := SQLOfflineStoreConfig{
		Config:        config,
		ConnectionURL: fmt.Sprintf("%s:%s@%s-%s/%s/PUBLIC", sc.Username, sc.Password, sc.Organization, sc.Account, sc.Database),
		Driver:        "snowflake",
		ProviderType:  SnowflakeOffline,
		QueryImpl:     snowflakeSQLQueries{},
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type snowflakeSQLQueries struct{}

func (q snowflakeSQLQueries) tableExists() string {
	return `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`
}

func (q snowflakeSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE TABLE %s AS SELECT IDENTIFIER('%s') as entity,  IDENTIFIER('%s') as value,  IDENTIFIER('%s') as ts FROM TABLE('%s')", sanitize(tableName),
			schema.Entity, schema.Value, schema.TS, sanitize(schema.SourceTable))
	} else {
		query = fmt.Sprintf("CREATE TABLE %s AS SELECT IDENTIFIER('%s') as entity, IDENTIFIER('%s') as value, null::TIMESTAMP_NTZ as ts FROM TABLE('%s')", sanitize(tableName),
			schema.Entity, schema.Value, sanitize(schema.SourceTable))
	}
	if _, err := db.Exec(query); err != nil {
		return err
	}
	query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT  %s  UNIQUE (entity, ts)", sanitize(tableName), sanitize(uuid.NewString()))
	if _, err := db.Exec(query); err != nil {
		return err
	}
	if !timestamp {
		// Populates empty column with timestamp
		update := fmt.Sprintf("UPDATE %s SET ts = ?", sanitize(tableName))
		if _, err := db.Exec(update, time.UnixMilli(0).UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (q snowflakeSQLQueries) primaryTableFromTable(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM TABLE('%s')", sanitize(tableName), sanitize(sourceName))
}
func (q snowflakeSQLQueries) getColumnNames() string {
	return "SELECT column_name FROM information_schema.columns WHERE table_name = ? order by ordinal_position"
}
func (q snowflakeSQLQueries) primaryTableCreate(name string, columnString string) string {
	return fmt.Sprintf("CREATE TABLE %s ( %s )", sanitize(name), columnString)
}
func (q snowflakeSQLQueries) materializationCreate(tableName string, resultName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1)", sanitize(tableName), sanitize(resultName))
}
func (q snowflakeSQLQueries) getTable() string {
	return "SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=?"
}

func (q snowflakeSQLQueries) dropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}

func (q snowflakeSQLQueries) trainingRowSelect(columns string, trainingSetName string) string {
	return fmt.Sprintf("SELECT %s FROM %s", columns, sanitize(trainingSetName))
}

func (q snowflakeSQLQueries) getValueColumnTypes(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s", sanitize(tableName)) //"select data_type from (select column_name, data_type from information_schema.columns where table_name = ? order by ordinal_position) t"
}

func (q snowflakeSQLQueries) determineColumnType(valueType ValueType) (string, error) {
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
		return "TIMESTAMP_NTZ", nil
	case NilType:
		return "VARCHAR", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func (q snowflakeSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMP_NTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
}

func (q snowflakeSQLQueries) resourceExists(tableName string) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM %s WHERE entity=? AND ts=? ", sanitize(tableName))
}
func (q snowflakeSQLQueries) writeUpdate(table string) string {
	return fmt.Sprintf("UPDATE %s SET value=? WHERE entity=? AND ts=? ", table)
}
func (q snowflakeSQLQueries) writeInserts(table string) string {
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (?, ?, ?)", table)
}
func (q snowflakeSQLQueries) writeExists(table string) string {
	return fmt.Sprintf("SELECT COUNT (*) FROM %s WHERE entity=? AND ts=?", table)
}

func (q snowflakeSQLQueries) materializationIterateSegment(tableName string) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM %s WHERE row_number>? AND row_number<=?)", sanitize(tableName))
}

func (q snowflakeSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for _ = range columns {
		placeholders = append(placeholders, fmt.Sprintf("?"))
	}
	return strings.Join(placeholders, ", ")
}

func (q snowflakeSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	columns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		resourceTableName := sanitize(store.getResourceTableName(feature))
		tableJoinAlias := fmt.Sprintf("t%d", i+1)
		columns = append(columns, resourceTableName)
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value as %s, ts FROM %s ORDER BY ts desc) as %s ON (%s.entity=t0.entity AND %s.ts <= t0.ts)",
			query, resourceTableName, resourceTableName, tableJoinAlias, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )) WHERE rn=1", query)
		}
	}
	columnStr := strings.Join(columns, ", ")
	fullQuery := fmt.Sprintf(
		"CREATE TABLE %s AS (SELECT %s, label FROM ("+
			"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY time desc) as rn FROM ( "+
			"SELECT t0.entity as e, t0.value as label, t0.ts as time, %s from %s as t0 %s )",
		sanitize(tableName), columnStr, columnStr, sanitize(labelName), query)
	if _, err := store.db.Exec(fullQuery); err != nil {
		return err
	}
	return nil
}

func (q snowflakeSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	switch t {
	case sfInt, sfNumber:
		if intVar, err := strconv.Atoi(v.(string)); err != nil {
			return v
		} else {
			return intVar
		}
	case sfFloat:
		if s, err := strconv.ParseFloat(v.(string), 64); err != nil {
			return v
		} else {
			return s
		}
	case sfString:
		return v.(string)
	case sfBool:
		return v.(bool)
	case sfTimestamp:
		ts := v.(time.Time).UTC()
		return ts
	default:
		return v
	}
}

func (q snowflakeSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return sfString
	case "int64":
		return sfInt
	case "float32", "float64":
		return sfFloat
	case "bool":
		return sfBool
	case "time.Time":
		return sfTimestamp
	}
	return sfString
}

func (q snowflakeSQLQueries) numRows(n interface{}) (int64, error) {
	if intVar, err := strconv.Atoi(n.(string)); err != nil {
		return 0, err
	} else {
		return int64(intVar), nil
	}
}

func (q snowflakeSQLQueries) transformationCreate(name string, query string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM ( %s )", sanitize(name), query)
}
