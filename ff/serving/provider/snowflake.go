// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	sf "github.com/snowflakedb/gosnowflake"
	"strconv"
	"strings"
	"time"
)

// snowflakeColumnType is used to specify the column type of a resource value.
type snowflakeColumnType string

const (
	SFInt    snowflakeColumnType = "integer"
	SFNumber                     = "NUMBER"
	SFFloat                      = "float8"
	SFString                     = "varchar"
	SFBool                       = "BOOLEAN"
)

type SnowflakeSchema struct {
	ValueType
}

func (ps *SnowflakeSchema) Serialize() []byte {
	schema, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}
	return schema
}

func (ps *SnowflakeSchema) Deserialize(schema SerializedTableSchema) error {
	err := json.Unmarshal(schema, ps)
	if err != nil {
		return err
	}
	return nil
}

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

type snowflakeOfflineStore struct {
	db *sql.DB
	BaseProvider
}

func snowflakeOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid snowflake config")
	}

	store, err := NewSnowflakeOfflineStore(sc)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// NewSnowflakeOfflineStore creates a connection to a snowflake database
// and initializes a table to track currently active Resource tables.
func NewSnowflakeOfflineStore(sc SnowflakeConfig) (*snowflakeOfflineStore, error) {
	url := fmt.Sprintf("%s:%s@%s-%s/%s/PUBLIC", sc.Username, sc.Password, sc.Organization, sc.Account, sc.Database)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return nil, err
	}
	return &snowflakeOfflineStore{
		db: db,
	}, nil
}

func (store *snowflakeOfflineStore) getResourceTableName(id ResourceID) string {
	var idType string
	if id.Type == Feature {
		idType = "feature"
	} else {
		idType = "label"
	}
	return fmt.Sprintf("featureform_resource_%s_%s_%s", idType, id.Name, id.Variant)
}

func (store *snowflakeOfflineStore) getMaterializationTableName(ftID MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", ftID)
}

func (store *snowflakeOfflineStore) getTrainingSetName(id ResourceID) string {
	return fmt.Sprintf("featureform_trainingset_%s_%s", id.Name, id.Variant)
}

func (store *snowflakeOfflineStore) tableExists(id ResourceID) (bool, error) {
	n := -1
	var tableName string
	if id.check(Feature, Label) == nil {
		tableName = store.getResourceTableName(id)
	} else if id.check(TrainingSet) == nil {
		tableName = store.getTrainingSetName(id)
	}
	err := store.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`, tableName).Scan(&n)
	if n == 0 {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (store *snowflakeOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *snowflakeOfflineStore) CreateResourceTable(id ResourceID, schema SerializedTableSchema) (OfflineTable, error) {
	psSchema := SnowflakeSchema{}
	if err := psSchema.Deserialize(schema); err != nil {
		return nil, err
	}
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}

	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName := store.getResourceTableName(id)
	table, err := newSnowflakeOfflineTable(store.db, tableName, psSchema.ValueType)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *snowflakeOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getSnowflakeResourceTable(id)
}

type snowflakeMaterialization struct {
	id        MaterializationID
	db        *sql.DB
	tableName string
}

func (mat *snowflakeMaterialization) ID() MaterializationID {
	return mat.id
}

// NumRows checks for the max row number to return as the number of rows.
// If there are no rows in the table, the interface n is checked for Nil,
// otherwise the interface is converted from a string to an int64
func (mat *snowflakeMaterialization) NumRows() (int64, error) {
	var n interface{}
	query := fmt.Sprintf("SELECT MAX(row_number) FROM %s", sanitize(mat.tableName))
	rows := mat.db.QueryRow(query)
	err := rows.Scan(&n)
	if err != nil {
		return 0, err
	}
	if n == nil {
		return 0, nil
	}
	if intVar, err := strconv.Atoi(n.(string)); err != nil {
		return 0, err
	} else {
		return int64(intVar), nil
	}
}

func (mat *snowflakeMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := fmt.Sprintf(""+
		"SELECT entity, value, ts FROM ( SELECT * FROM %s WHERE row_number>? AND row_number<=?)", sanitize(mat.tableName))

	rows, err := mat.db.Query(query, start, end)
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	colType := mat.getValueColumnType(types[1])
	if err != nil {
		return nil, err
	}
	return newSnowflakeFeatureIterator(rows, colType), nil
}

type snowflakeFeatureIterator struct {
	rows         *sql.Rows
	err          error
	currentValue ResourceRecord
	columnType   snowflakeColumnType
}

func newSnowflakeFeatureIterator(rows *sql.Rows, columnType snowflakeColumnType) FeatureIterator {
	return &snowflakeFeatureIterator{
		rows:         rows,
		err:          nil,
		currentValue: ResourceRecord{},
		columnType:   columnType,
	}
}

func (iter *snowflakeFeatureIterator) Next() bool {
	if !iter.rows.Next() {
		iter.rows.Close()
		return false
	}
	var rec ResourceRecord
	var value interface{}
	var ts time.Time
	if err := iter.rows.Scan(&rec.Entity, &value, &ts); err != nil {
		iter.rows.Close()
		iter.err = err
		return false
	}
	rec.Value = castSnowflakeTableItemType(value, iter.columnType)
	rec.TS = ts.UTC()
	iter.currentValue = rec
	return true
}

func (iter *snowflakeFeatureIterator) Value() ResourceRecord {
	return iter.currentValue
}

func (iter *snowflakeFeatureIterator) Err() error {
	return nil
}

// castTableItemType returns the value casted as its original type
func castSnowflakeTableItemType(v interface{}, t snowflakeColumnType) interface{} {
	switch t {
	case SFInt, SFNumber:
		if intVar, err := strconv.Atoi(v.(string)); err != nil {
			return v
		} else {
			return intVar
		}
	case SFFloat:
		if s, err := strconv.ParseFloat(v.(string), 64); err == nil {
			return v
		} else {
			return s
		}
	case SFString:
		return v.(string)
	case SFBool:
		return v.(bool)
	default:
		return v
	}
}

// getValueColumnType gets the column type for the value of a resource.
// Used to cast the value to the proper type after it is queried
func (mat *snowflakeMaterialization) getValueColumnType(t *sql.ColumnType) snowflakeColumnType {
	switch t.ScanType().String() {
	case "string":
		return SFString
	case "int64":
		return SFInt
	case "float32", "float64":
		return SFFloat
	case "boolean":
		return SFBool
	}
	return SFString
}

func (store *snowflakeOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	resTable, err := store.getSnowflakeResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(id.Name)
	matTableName := store.getMaterializationTableName(matID)
	sanitizedTableName := sanitize(matTableName)
	resTableName := sanitize(resTable.name)

	materializeQry := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1)", sanitizedTableName, resTableName)

	_, err = store.db.Exec(materializeQry)
	if err != nil {
		return nil, err
	}

	return &snowflakeMaterialization{
		id:        matID,
		db:        store.db,
		tableName: matTableName,
	}, nil
}

func (store *snowflakeOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=?")
	rows, err := store.db.Query(getMatQry, tableName)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	rowCount := 0
	if rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return nil, &MaterializationNotFound{id}
	}
	return &snowflakeMaterialization{
		id:        id,
		db:        store.db,
		tableName: tableName,
	}, err
}

func (store *snowflakeOfflineStore) DeleteMaterialization(id MaterializationID) error {
	tableName := store.getMaterializationTableName(id)
	if exists, err := store.materializationExists(id); err != nil {
		return err
	} else if !exists {
		return &MaterializationNotFound{id}
	}
	query := fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
	if _, err := store.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (store *snowflakeOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=?")
	rows, err := store.db.Query(getMatQry, tableName)
	defer rows.Close()
	if err != nil {
		return false, err
	}
	rowCount := 0
	if rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return false, nil
	}
	return true, nil
}

func (store *snowflakeOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getSnowflakeResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName := store.getTrainingSetName(def.ID)

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
		sanitize(tableName), columnStr, columnStr, sanitize(label.name), query)
	if _, err := store.db.Exec(fullQuery); err != nil {
		return err
	}
	return nil
}

func (store *snowflakeOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TrainingSetNotFound{id}
	}
	trainingSetName := store.getTrainingSetName(id)
	rows, err := store.db.Query(
		"SELECT column_name FROM information_schema.columns WHERE table_name = ? order by ordinal_position",
		trainingSetName)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	features := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		features = append(features, sanitize(column))
	}
	columns := strings.Join(features[:], ", ")
	trainingSetQry := fmt.Sprintf("SELECT %s FROM %s", columns, sanitize(trainingSetName))
	rows, err = store.db.Query(trainingSetQry)
	if err != nil {
		return nil, err
	}
	colTypes, err := store.getValueColumnTypes(trainingSetName)
	if err != nil {
		return nil, err
	}
	return newSnowflakeTrainingSetIterator(rows, colTypes), nil
}

// getValueColumnTypes returns a list of column types. Columns consist of feature and label values
// within a training set.
func (store *snowflakeOfflineStore) getValueColumnTypes(table string) ([]snowflakeColumnType, error) {
	rows, err := store.db.Query(
		"select data_type from (select column_name, data_type from information_schema.columns where table_name = ? order by ordinal_position) t",
		table)
	if err != nil {
		return nil, err
	}
	colTypes := make([]snowflakeColumnType, 0)

	for rows.Next() {
		var colString string
		if err := rows.Scan(&colString); err != nil {
			return nil, err
		}
		colTypes = append(colTypes, snowflakeColumnType(colString))
	}
	return colTypes, nil
}

type snowflakeTrainingRowsIterator struct {
	rows            *sql.Rows
	currentFeatures []interface{}
	currentLabel    interface{}
	err             error
	columnTypes     []snowflakeColumnType
	isHeaderRow     bool
}

func newSnowflakeTrainingSetIterator(rows *sql.Rows, columnTypes []snowflakeColumnType) TrainingSetIterator {
	return &snowflakeTrainingRowsIterator{
		rows:            rows,
		currentFeatures: nil,
		currentLabel:    nil,
		err:             nil,
		columnTypes:     columnTypes,
		isHeaderRow:     true,
	}
}

func (it *snowflakeTrainingRowsIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	columnNames, err := it.rows.Columns()
	if err != nil {
		it.rows.Close()
		it.err = err
		return false
	}
	if err != nil {
		it.err = err
		it.rows.Close()
		return false
	}
	values := make([]interface{}, len(columnNames))
	pointers := make([]interface{}, len(columnNames))
	for i, _ := range values {
		pointers[i] = &values[i]
	}
	if err := it.rows.Scan(pointers...); err != nil {
		it.rows.Close()
		it.err = err
		return false
	}
	var label interface{}
	numFeatures := len(columnNames) - 1
	featureVals := make([]interface{}, numFeatures)
	for i, value := range values {
		if value == nil {
			continue
		}
		if i < numFeatures {
			featureVals[i] = castSnowflakeTableItemType(value, it.columnTypes[i])
		} else {
			label = castSnowflakeTableItemType(value, it.columnTypes[i])
		}
	}
	it.currentFeatures = featureVals
	it.currentLabel = label

	return true
}

func (it *snowflakeTrainingRowsIterator) Err() error {
	return it.err
}

func (it *snowflakeTrainingRowsIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *snowflakeTrainingRowsIterator) Label() interface{} {
	return it.currentLabel
}

func (store *snowflakeOfflineStore) getSnowflakeResourceTable(id ResourceID) (*snowflakeOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return &snowflakeOfflineTable{
		db:   store.db,
		name: store.getResourceTableName(id),
	}, nil
}

type snowflakeOfflineTable struct {
	db   *sql.DB
	name string
}

func newSnowflakeOfflineTable(db *sql.DB, name string, valueType ValueType) (*snowflakeOfflineTable, error) {
	columnType, err := determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMP_NTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
	_, err = db.Exec(tableCreateQry)
	if err != nil {
		return nil, err
	}
	return &snowflakeOfflineTable{
		db:   db,
		name: name,
	}, nil
}

func (table *snowflakeOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := sanitize(table.name)
	if err := rec.check(); err != nil {
		return err
	}

	n := -1
	existsQuery := fmt.Sprintf("SELECT COUNT (*) FROM %s WHERE entity=? AND ts=?", tb)
	if err := table.db.QueryRow(existsQuery, rec.Entity, rec.TS).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		insertQuery := fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (?, ?, ?)", tb)
		if _, err := table.db.Exec(insertQuery, rec.Entity, rec.Value, sf.DataTypeTimestampNtz, rec.TS); err != nil {
			return err
		}
	} else if n > 0 {
		updateQuery := fmt.Sprintf("UPDATE %s SET value=? WHERE entity=? AND ts=? ", tb)
		if _, err := table.db.Exec(updateQuery, rec.Value, rec.Entity, rec.TS); err != nil {
			return err
		}
	}
	return nil
}

func (table *snowflakeOfflineTable) resourceExists(rec ResourceRecord) (bool, error) {
	rec = checkTimestamp(rec)
	query := fmt.Sprintf("SELECT entity, value, ts FROM %s WHERE entity=? AND ts=? ", sanitize(table.name))
	rows, err := table.db.Query(query, rec.Entity, rec.TS)
	defer rows.Close()
	if err != nil {
		return false, err
	}
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return false, nil
	}
	return true, nil
}
