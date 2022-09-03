// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	db "github.com/jackc/pgx/v4"
	sf "github.com/snowflakedb/gosnowflake"
)

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

type SQLOfflineStoreConfig struct {
	Config        SerializedConfig
	ConnectionURL string
	Driver        string
	ProviderType  Type
	QueryImpl     OfflineTableQueries
}

type OfflineTableQueries interface {
	setVariableBinding(b variableBindingStyle)
	tableExists() string
	viewExists() string
	resourceExists(tableName string) string
	registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error
	primaryTableRegister(tableName string, sourceName string) string
	primaryTableCreate(name string, columnString string) string
	getColumns(db *sql.DB, tableName string) ([]TableColumn, error)
	getValueColumnTypes(tableName string) string
	determineColumnType(valueType ValueType) (string, error)
	materializationCreate(tableName string, sourceName string) string
	materializationUpdate(db *sql.DB, tableName string, sourceName string) error
	materializationExists() string
	materializationDrop(tableName string) string
	getTable() string
	dropTable(tableName string) string
	materializationIterateSegment(tableName string) string
	newSQLOfflineTable(name string, columnType string) string
	writeUpdate(table string) string
	writeInserts(table string) string
	writeExists(table string) string
	createValuePlaceholderString(columns []TableColumn) string
	trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingRowSelect(columns string, trainingSetName string) string
	castTableItemType(v interface{}, t interface{}) interface{}
	getValueColumnType(t *sql.ColumnType) interface{}
	numRows(n interface{}) (int64, error)
	transformationCreate(name string, query string) string
	transformationUpdate(db *sql.DB, tableName string, query string) error
	transformationExists() string
}

type sqlOfflineStore struct {
	db     *sql.DB
	parent SQLOfflineStoreConfig
	query  OfflineTableQueries
	BaseProvider
}

// NewPostgresOfflineStore creates a connection to a postgres database
// and initializes a table to track currently active Resource tables.
func NewSQLOfflineStore(config SQLOfflineStoreConfig) (*sqlOfflineStore, error) {
	url := config.ConnectionURL
	db, err := sql.Open(config.Driver, url)
	if err != nil {
		return nil, err
	}

	return &sqlOfflineStore{
		db:     db,
		parent: config,
		query:  config.QueryImpl,
		BaseProvider: BaseProvider{
			ProviderType:   config.ProviderType,
			ProviderConfig: config.Config,
		},
	}, nil
}

func checkName(id ResourceID) error {
	if strings.Contains(id.Name, "__") || strings.Contains(id.Variant, "__") {
		return fmt.Errorf("names cannot contain double underscores '__': %s", id.Name)
	}
	return nil
}

func (store *sqlOfflineStore) getResourceTableName(id ResourceID) (string, error) {
	if err := checkName(id); err != nil {
		return "", err
	}
	var idType string
	if id.Type == Feature {
		idType = "feature"
	} else {
		idType = "label"
	}
	return fmt.Sprintf("featureform_resource_%s__%s__%s", idType, id.Name, id.Variant), nil
}

func (store *sqlOfflineStore) getMaterializationTableName(id MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", id)
}

func (store *sqlOfflineStore) getTrainingSetName(id ResourceID) (string, error) {
	if err := checkName(id); err != nil {
		return "", err
	}
	return fmt.Sprintf("featureform_trainingset__%s__%s", id.Name, id.Variant), nil
}

func GetPrimaryTableName(id ResourceID) (string, error) {
	if err := checkName(id); err != nil {
		return "", err
	}

	return fmt.Sprintf("featureform_primary__%s__%s", id.Name, id.Variant), nil
}

func (store *sqlOfflineStore) tableExists(id ResourceID) (bool, error) {
	n := -1
	var tableName string
	var err error
	if id.check(Feature, Label) == nil {
		tableName, err = store.getResourceTableName(id)
	} else if id.check(TrainingSet) == nil {
		tableName, err = store.getTrainingSetName(id)
	} else if id.check(Primary) == nil || id.check(Transformation) == nil {
		tableName, err = GetPrimaryTableName(id)
	}
	if err != nil {
		return false, fmt.Errorf("type check: %v: %v", id, err)
	}
	query := store.query.tableExists()
	err = store.db.QueryRow(query, tableName).Scan(&n)
	if n > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("table exists check: %v", err)
	}
	query = store.query.viewExists()
	err = store.db.QueryRow(query, tableName).Scan(&n)
	if n > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("view exists check: %v", err)
	}
	return false, nil
}

func (store *sqlOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *sqlOfflineStore) Close() error {
	return store.db.Close()
}

func (store *sqlOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, fmt.Errorf("type check: %w", err)
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, fmt.Errorf("exists error: %w", err)
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	if schema.Entity == "" || schema.Value == "" {
		return nil, fmt.Errorf("non-empty entity and value columns required")
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		return nil, fmt.Errorf("get name: %w", err)
	}
	if schema.TS == "" {
		if err := store.query.registerResources(store.db, tableName, schema, false); err != nil {
			return nil, fmt.Errorf("register no ts: %w", err)
		}
	} else {
		if err := store.query.registerResources(store.db, tableName, schema, true); err != nil {
			return nil, fmt.Errorf("register ts: %w", err)
		}
	}

	return &sqlOfflineTable{
		db:    store.db,
		name:  tableName,
		query: store.query,
	}, nil
}

func (store *sqlOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, fmt.Errorf("check fail: %w", err)
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, fmt.Errorf("table exist: %w", err)
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, fmt.Errorf("get name: %w", err)
	}
	query := store.query.primaryTableRegister(tableName, sourceName)
	if _, err := store.db.Exec(query); err != nil {
		return nil, fmt.Errorf("register table: %w", err)
	}

	columnNames, err := store.query.getColumns(store.db, tableName)

	return &sqlPrimaryTable{
		db:     store.db,
		name:   tableName,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *sqlOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("cannot create primary table without columns")
	}
	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	table, err := store.newsqlPrimaryTable(store.db, tableName, schema)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *sqlOfflineStore) newsqlPrimaryTable(db *sql.DB, name string, schema TableSchema) (*sqlPrimaryTable, error) {
	query, err := store.createsqlPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}
	return &sqlPrimaryTable{
		db:     db,
		name:   name,
		schema: schema,
		query:  store.query,
	}, nil
}

// primaryTableCreate creates a query for table creation based on the
// specified TableSchema. Returns the query if successful. Returns an error
// if there is an invalid column type.
func (store *sqlOfflineStore) createsqlPrimaryTableQuery(name string, schema TableSchema) (string, error) {
	columns := make([]string, 0)
	for _, column := range schema.Columns {
		columnType, err := store.query.determineColumnType(column.ValueType)
		if err != nil {
			return "", err
		}
		columns = append(columns, fmt.Sprintf("%s %s", column.Name, columnType))
	}
	columnString := strings.Join(columns, ", ")
	return store.query.primaryTableCreate(name, columnString), nil
}

func (store *sqlOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	columnNames, err := store.query.getColumns(store.db, name)

	return &sqlPrimaryTable{
		db:     store.db,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *sqlOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	existsQuery := store.query.tableExists()
	rows, err := store.db.Query(existsQuery, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("transformation not found: %v", name)
	}
	columnNames, err := store.query.getColumns(store.db, name)

	return &sqlPrimaryTable{
		db:     store.db,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *sqlOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, fmt.Errorf("ID check failed: %v", err)
	}

	if exists, err := store.tableExists(id); err != nil {
		return nil, fmt.Errorf("could not check if table exists: %v", err)
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		return nil, fmt.Errorf("could not get resource table name: %v", err)
	}
	var valueType ValueType
	if valueIndex := store.getValueIndex(schema.Columns); valueIndex > 0 {
		valueType = schema.Columns[valueIndex].ValueType
	} else {
		valueType = NilType

	}
	table, err := store.newsqlOfflineTable(store.db, tableName, valueType)
	if err != nil {
		return nil, fmt.Errorf("could not return SQL offline table: %v", err)
	}
	return table, nil
}

// getValueIndex returns the index of the value column in the schema.
// Returns -1 if an entity column is not found
func (store *sqlOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *sqlOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getsqlResourceTable(id)
}

type sqlMaterialization struct {
	id        MaterializationID
	db        *sql.DB
	tableName string
	query     OfflineTableQueries
}

func (mat *sqlMaterialization) ID() MaterializationID {
	return mat.id
}

// NumRows checks for the count of rows to return as the number of rows.
// If there are no rows in the table, the interface n is checked for Nil,
// otherwise the interface is converted from a string to an int64
func (mat *sqlMaterialization) NumRows() (int64, error) {
	var n interface{}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitize(mat.tableName))
	rows := mat.db.QueryRow(query)
	err := rows.Scan(&n)
	if err != nil {
		return 0, err
	}
	if n == nil {
		return 0, nil
	}
	intVar, err := mat.query.numRows(n)
	if err != nil {
		return 0, nil
	}
	return intVar, nil

}

func (mat *sqlMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName)

	rows, err := mat.db.Query(query, start, end)
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	colType := mat.query.getValueColumnType(types[1])
	if err != nil {
		return nil, err
	}
	return newsqlFeatureIterator(rows, colType, mat.query), nil
}

type sqlFeatureIterator struct {
	rows         *sql.Rows
	err          error
	currentValue ResourceRecord
	columnType   interface{}
	query        OfflineTableQueries
}

func newsqlFeatureIterator(rows *sql.Rows, columnType interface{}, query OfflineTableQueries) FeatureIterator {
	return &sqlFeatureIterator{
		rows:         rows,
		err:          nil,
		currentValue: ResourceRecord{},
		columnType:   columnType,
		query:        query,
	}
}

func (iter *sqlFeatureIterator) Next() bool {
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
	rec.Value = iter.query.castTableItemType(value, iter.columnType)
	rec.TS = ts.UTC()
	iter.currentValue = rec
	return true
}

func (iter *sqlFeatureIterator) Value() ResourceRecord {
	return iter.currentValue
}

func (iter *sqlFeatureIterator) Err() error {
	return nil
}

func (iter *sqlFeatureIterator) Close() error {
	return iter.rows.Close()
}

func (store *sqlOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	resTable, err := store.getsqlResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(id.Name)
	matTableName := store.getMaterializationTableName(matID)
	materializeQry := store.query.materializationCreate(matTableName, resTable.name)

	_, err = store.db.Exec(materializeQry)
	if err != nil {
		return nil, err
	}
	return &sqlMaterialization{
		id:        matID,
		db:        store.db,
		tableName: matTableName,
		query:     store.query,
	}, nil
}

func (store *sqlOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {

	tableName := store.getMaterializationTableName(id)

	getMatQry := store.query.materializationExists()

	rows, err := store.db.Query(getMatQry, tableName)
	if err != nil {
		return nil, fmt.Errorf("could not get materialization: %w", err)
	}
	defer rows.Close()

	rowCount := 0
	if rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return nil, &MaterializationNotFound{id}
	}
	return &sqlMaterialization{
		id:        id,
		db:        store.db,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *sqlOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	matID := MaterializationID(id.Name)
	tableName := store.getMaterializationTableName(matID)
	getMatQry := store.query.materializationExists()
	resTable, err := store.getsqlResourceTable(id)
	if err != nil {
		return nil, err
	}

	rows, err := store.db.Query(getMatQry, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, &MaterializationNotFound{matID}
	}
	err = store.query.materializationUpdate(store.db, tableName, resTable.name)
	if err != nil {
		return nil, err
	}
	return &sqlMaterialization{
		id:        matID,
		db:        store.db,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *sqlOfflineStore) DeleteMaterialization(id MaterializationID) error {
	tableName := store.getMaterializationTableName(id)
	if exists, err := store.materializationExists(id); err != nil {
		return err
	} else if !exists {
		return &MaterializationNotFound{id}
	}
	query := store.query.materializationDrop(tableName)
	if _, err := store.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (store *sqlOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := store.query.materializationExists()
	rows, err := store.db.Query(getMatQry, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	rowCount := 0
	if rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return false, nil
	}
	return true, nil
}

func (store *sqlOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getsqlResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}
	if err := store.query.trainingSetCreate(store, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getsqlResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}
	if err := store.query.trainingSetUpdate(store, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	fmt.Printf("Getting Training Set: %v\n", id)
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	fmt.Printf("Checking if Training Set exists: %v\n", id)
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TrainingSetNotFound{id}
	}
	trainingSetName, err := store.getTrainingSetName(id)
	if err != nil {
		return nil, err
	}
	columnNames, err := store.query.getColumns(store.db, trainingSetName)
	if err != nil {
		return nil, err
	}
	features := make([]string, 0)
	for _, name := range columnNames {
		features = append(features, sanitize(name.Name))
	}
	columns := strings.Join(features[:], ", ")
	trainingSetQry := store.query.trainingRowSelect(columns, trainingSetName)
	fmt.Printf("Training Set Query: %s\n", trainingSetQry)
	rows, err := store.db.Query(trainingSetQry)
	if err != nil {
		return nil, err
	}
	colTypes, err := store.getValueColumnTypes(trainingSetName)
	if err != nil {
		return nil, err
	}
	return store.newsqlTrainingSetIterator(rows, colTypes), nil
}

// getValueColumnTypes returns a list of column types. Columns consist of feature and label values
// within a training set.
func (store *sqlOfflineStore) getValueColumnTypes(table string) ([]interface{}, error) {
	query := store.query.getValueColumnTypes(table)
	rows, err := store.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	colTypes := make([]interface{}, 0)

	if rows.Next() {

		rawType, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		for _, t := range rawType {
			colTypes = append(colTypes, store.query.getValueColumnType(t))
		}
	}
	return colTypes, nil
}

type sqlTrainingRowsIterator struct {
	rows            *sql.Rows
	currentFeatures []interface{}
	currentLabel    interface{}
	err             error
	columnTypes     []interface{}
	isHeaderRow     bool
	query           OfflineTableQueries
}

func (store *sqlOfflineStore) newsqlTrainingSetIterator(rows *sql.Rows, columnTypes []interface{}) TrainingSetIterator {
	return &sqlTrainingRowsIterator{
		rows:            rows,
		currentFeatures: nil,
		currentLabel:    nil,
		err:             nil,
		columnTypes:     columnTypes,
		isHeaderRow:     true,
		query:           store.query,
	}
}

func (it *sqlTrainingRowsIterator) Next() bool {
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
			featureVals[i] = it.query.castTableItemType(value, it.columnTypes[i])
		} else {
			label = it.query.castTableItemType(value, it.columnTypes[i])
		}
	}
	it.currentFeatures = featureVals
	it.currentLabel = label

	return true
}

func (it *sqlTrainingRowsIterator) Err() error {
	return it.err
}

func (it *sqlTrainingRowsIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *sqlTrainingRowsIterator) Label() interface{} {
	return it.currentLabel
}

func (store *sqlOfflineStore) getsqlResourceTable(id ResourceID) (*sqlOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	table, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	return &sqlOfflineTable{
		db:    store.db,
		name:  table,
		query: store.query,
	}, nil
}

type sqlOfflineTable struct {
	db    *sql.DB
	query OfflineTableQueries
	name  string
}

type sqlPrimaryTable struct {
	db     *sql.DB
	name   string
	query  OfflineTableQueries
	schema TableSchema
}

func (table *sqlPrimaryTable) GetName() string {
	return table.name
}

func (table *sqlPrimaryTable) Write(rec GenericRecord) error {
	tb := sanitize(table.name)
	columns := table.getColumnNameString()
	placeholder := table.query.createValuePlaceholderString(table.schema.Columns)
	upsertQuery := fmt.Sprintf(""+
		"INSERT INTO %s ( %s ) "+
		"VALUES ( %s ) ", tb, columns, placeholder)
	if _, err := table.db.Exec(upsertQuery, rec...); err != nil {
		return err
	}
	return nil
}

func (table *sqlPrimaryTable) getColumnNameString() string {
	columns := make([]string, 0)
	for _, column := range table.schema.Columns {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns, ", ")
}

func (pt *sqlPrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	columns, err := pt.query.getColumns(pt.db, pt.name)
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, sanitize(col.Name))
	}
	names := strings.Join(columnNames[:], ", ")
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", names, sanitize(pt.name), n)
	rows, err := pt.db.Query(query)
	if err != nil {
		return nil, err
	}
	colTypes, err := pt.getValueColumnTypes(pt.name)
	if err != nil {
		return nil, err
	}
	return newsqlGenericTableIterator(rows, colTypes, columnNames, pt.query), nil
}

func (pt *sqlPrimaryTable) getValueColumnTypes(table string) ([]interface{}, error) {
	query := pt.query.getValueColumnTypes(table)
	rows, err := pt.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	colTypes := make([]interface{}, 0)
	if rows.Next() {

		rawType, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		for _, t := range rawType {
			colTypes = append(colTypes, pt.query.getValueColumnType(t))
		}
	}

	return colTypes, nil
}

func (pt *sqlPrimaryTable) NumRows() (int64, error) {
	n := int64(0)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitize(pt.name))
	rows := pt.db.QueryRow(query)

	err := rows.Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func determineColumnType(valueType ValueType) (string, error) {
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

func (store *sqlOfflineStore) newsqlOfflineTable(db *sql.DB, name string, valueType ValueType) (*sqlOfflineTable, error) {
	columnType, err := determineColumnType(valueType)
	if err != nil {
		return nil, fmt.Errorf("could not determine column type: %v", err)
	}
	tableCreateQry := store.query.newSQLOfflineTable(name, columnType)
	_, err = db.Exec(tableCreateQry)
	if err != nil {
		return nil, fmt.Errorf("could not create table query: %v", err)
	}
	return &sqlOfflineTable{
		db:    db,
		name:  name,
		query: store.query,
	}, nil
}

func (table *sqlOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := sanitize(table.name)
	if err := rec.check(); err != nil {
		return err
	}

	n := -1
	existsQuery := table.query.writeExists(tb)

	if err := table.db.QueryRow(existsQuery, rec.Entity, rec.TS).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		insertQuery := table.query.writeInserts(tb)
		if _, err := table.db.Exec(insertQuery, rec.Entity, rec.Value, rec.TS); err != nil {
			return err
		}
	} else if n > 0 {
		updateQuery := table.query.writeUpdate(tb)
		if _, err := table.db.Exec(updateQuery, rec.Value, rec.Entity, rec.TS); err != nil {
			return err
		}
	}
	return nil
}

func (table *sqlOfflineTable) resourceExists(rec ResourceRecord) (bool, error) {
	rec = checkTimestamp(rec)
	query := table.query.resourceExists(table.name)

	rows, err := table.db.Query(query, rec.Entity, rec.TS)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return false, nil
	}
	return true, nil
}

func (store *sqlOfflineStore) CreateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	query := store.query.transformationCreate(name, config.Query)
	if _, err := store.db.Exec(query); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) UpdateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	err = store.query.transformationUpdate(store.db, name, config.Query)
	if err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) createTransformationName(id ResourceID) (string, error) {
	switch id.Type {
	case Transformation:
		return GetPrimaryTableName(id)
	case Label:
		return "", TransformationTypeError{"Invalid Transformation Type: Label"}
	case Feature:
		return "", TransformationTypeError{"Invalid Transformation Type: Feature"}
	case TrainingSet:
		return "", TransformationTypeError{"Invalid Transformation Type: Training Set"}
	case Primary:
		return "", TransformationTypeError{"Invalid Transformation Type: Primary"}
	default:
		return "", TransformationTypeError{"Invalid Transformation Type"}
	}
}

type sqlGenericTableIterator struct {
	rows          *sql.Rows
	currentValues GenericRecord
	err           error
	columnTypes   []interface{}
	columnNames   []string
	query         OfflineTableQueries
}

func newsqlGenericTableIterator(rows *sql.Rows, columnTypes []interface{}, columnNames []string, query OfflineTableQueries) GenericTableIterator {
	return &sqlGenericTableIterator{
		rows:          rows,
		currentValues: nil,
		err:           nil,
		columnTypes:   columnTypes,
		columnNames:   columnNames,
		query:         query,
	}
}

func (it *sqlGenericTableIterator) Next() bool {
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

	rowValues := make(GenericRecord, len(columnNames))
	for i, value := range values {
		if value == nil {
			continue
		}
		rowValues[i] = it.query.castTableItemType(value, it.columnTypes[i])
	}
	it.currentValues = rowValues
	return true
}

func (it *sqlGenericTableIterator) Values() GenericRecord {
	return it.currentValues
}

func (it *sqlGenericTableIterator) Columns() []string {
	return it.columnNames
}

func (it *sqlGenericTableIterator) Err() error {
	return it.err
}

func (it *sqlGenericTableIterator) Close() error {
	return it.rows.Close()
}

type defaultOfflineSQLQueries struct {
	BindingStyle variableBindingStyle
}

func (q *defaultOfflineSQLQueries) newVariableBindingIterator() VariableBindingIterator {
	return VariableBindingIterator{
		Current: 0,
		Style:   q.BindingStyle,
	}
}

type variableBindingStyle string

const (
	PostgresBindingStyle variableBindingStyle = "POSTGRESBIND"
	MySQLBindingStyle                         = "SQLBIND"
)

type VariableBindingIterator struct {
	Current int
	Style   variableBindingStyle
}

func (it *VariableBindingIterator) Next() string {
	if it.Style == PostgresBindingStyle {
		it.Current = it.Current + 1
		return fmt.Sprintf("$%d", it.Current)
	} else {
		return "?"
	}
}

func (q *defaultOfflineSQLQueries) setVariableBinding(b variableBindingStyle) {
	q.BindingStyle = b
}

const genericExists = `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`

func (q defaultOfflineSQLQueries) tableExists() string {
	return genericExists
}

func (q defaultOfflineSQLQueries) viewExists() string {
	return genericExists
}

func (q defaultOfflineSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT IDENTIFIER('%s') as entity,  IDENTIFIER('%s') as value,  IDENTIFIER('%s') as ts FROM TABLE('%s')", sanitize(tableName),
			schema.Entity, schema.Value, schema.TS, sanitize(schema.SourceTable))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT IDENTIFIER('%s') as entity, IDENTIFIER('%s') as value, to_timestamp_ntz('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP_NTZ as ts FROM TABLE('%s')", sanitize(tableName),
			schema.Entity, schema.Value, time.UnixMilli(0).UTC(), sanitize(schema.SourceTable))
	}
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (q defaultOfflineSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM TABLE('%s')", sanitize(tableName), sourceName)
}
func (q defaultOfflineSQLQueries) getColumns(db *sql.DB, name string) ([]TableColumn, error) {
	bind := q.newVariableBindingIterator()
	qry := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = %s order by ordinal_position", bind.Next())
	rows, err := db.Query(qry, name)
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
func (q defaultOfflineSQLQueries) primaryTableCreate(name string, columnString string) string {
	return fmt.Sprintf("CREATE TABLE %s ( %s )", sanitize(name), columnString)
}
func (q defaultOfflineSQLQueries) materializationCreate(tableName string, sourceName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1)", sanitize(tableName), sanitize(sourceName))
}

func (q defaultOfflineSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	sanitizedTable := sanitize(tableName)
	tempTable := sanitize(fmt.Sprintf("tmp_%s", tableName))
	oldTable := sanitize(fmt.Sprintf("old_%s", tableName))
	query := fmt.Sprintf(
		"BEGIN TRANSACTION;"+
			"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1);"+
			"ALTER TABLE %s RENAME TO %s;"+
			"ALTER TABLE %s RENAME TO %s;"+
			"DROP TABLE %s;"+
			"COMMIT;"+
			"", tempTable, sanitize(sourceName), sanitizedTable, oldTable, tempTable, sanitizedTable, oldTable)
	var numStatements = 6
	ctx = context.Background()
	stmt, _ := sf.WithMultiStatement(ctx, numStatements)
	_, err := db.QueryContext(stmt, query)

	return err
}

func (q defaultOfflineSQLQueries) getTable() string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=%s", bind.Next())
}

func (q defaultOfflineSQLQueries) materializationExists() string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=%s", bind.Next())
}

func (q defaultOfflineSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP MATERIALIZED VIEW %s", sanitize(tableName))
}

func (q defaultOfflineSQLQueries) dropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}

func (q defaultOfflineSQLQueries) trainingRowSelect(columns string, trainingSetName string) string {
	return fmt.Sprintf("SELECT %s FROM %s", columns, sanitize(trainingSetName))
}

func (q defaultOfflineSQLQueries) getValueColumnTypes(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s", sanitize(tableName))
}

func (q defaultOfflineSQLQueries) determineColumnType(valueType ValueType) (string, error) {
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

func (q defaultOfflineSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMP_NTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
}

func (q defaultOfflineSQLQueries) resourceExists(tableName string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT entity, value, ts FROM %s WHERE entity=%s AND ts=%s ", sanitize(tableName), bind.Next(), bind.Next())
}
func (q defaultOfflineSQLQueries) writeUpdate(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("UPDATE %s SET value=%s WHERE entity=%s AND ts=%s ", table, bind.Next(), bind.Next(), bind.Next())
}
func (q defaultOfflineSQLQueries) writeInserts(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (%s, %s, %s)", table, bind.Next(), bind.Next(), bind.Next())
}
func (q defaultOfflineSQLQueries) writeExists(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT COUNT (*) FROM %s WHERE entity=%s AND ts=%s", table, bind.Next(), bind.Next())
}

func (q defaultOfflineSQLQueries) materializationIterateSegment(tableName string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM %s WHERE row_number>%s AND row_number<=%s)t1", sanitize(tableName), bind.Next(), bind.Next())
}

func (q defaultOfflineSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for _ = range columns {
		placeholders = append(placeholders, fmt.Sprintf("?"))
	}
	return strings.Join(placeholders, ", ")
}

func (q defaultOfflineSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	columns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {

		tableName, err := store.getResourceTableName(feature)
		santizedName := sanitize(tableName)
		if err != nil {
			return err
		}
		tableJoinAlias := fmt.Sprintf("t%d", i+1)
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value as %s, ts FROM %s ORDER BY ts desc) as %s ON (%s.entity=t0.entity AND %s.ts <= t0.ts)",
			query, santizedName, santizedName, tableJoinAlias, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )) WHERE rn=1", query)
		}
	}
	columnStr := strings.Join(columns, ", ")
	if !isUpdate {
		fullQuery := fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY time desc) as rn FROM ( "+
				"SELECT t0.entity as e, t0.value as label, t0.ts as time, %s from %s as t0 %s )",
			sanitize(tableName), columnStr, columnStr, sanitize(labelName), query)
		if _, err := store.db.Exec(fullQuery); err != nil {
			return err
		}
	} else {
		tempTable := sanitize(fmt.Sprintf("tmp_%s", tableName))
		fullQuery := fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY time desc) as rn FROM ( "+
				"SELECT t0.entity as e, t0.value as label, t0.ts as time, %s from %s as t0 %s )",
			tempTable, columnStr, columnStr, sanitize(labelName), query)
		err := q.atomicUpdate(store.db, tableName, tempTable, fullQuery)
		return err
	}
	return nil
}

func (q defaultOfflineSQLQueries) atomicUpdate(db *sql.DB, tableName string, tempName string, query string) error {
	sanitizedTable := sanitize(tableName)
	oldTable := sanitize(fmt.Sprintf("old_%s", tableName))
	transaction := fmt.Sprintf(
		"BEGIN TRANSACTION;"+
			"%s;"+
			"ALTER TABLE %s RENAME TO %s;"+
			"ALTER TABLE %s RENAME TO %s;"+
			"DROP TABLE %s;"+
			"COMMIT;"+
			"", query, sanitizedTable, oldTable, tempName, sanitizedTable, oldTable)
	var numStatements = 6
	ctx = context.Background()
	stmt, _ := sf.WithMultiStatement(ctx, numStatements)
	_, err := db.QueryContext(stmt, transaction)
	return err
}

func (q defaultOfflineSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q defaultOfflineSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q defaultOfflineSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
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

func (q defaultOfflineSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
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

func (q defaultOfflineSQLQueries) numRows(n interface{}) (int64, error) {
	if intVar, err := strconv.Atoi(n.(string)); err != nil {
		return 0, err
	} else {
		return int64(intVar), nil
	}
}

func (q defaultOfflineSQLQueries) transformationCreate(name string, query string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM ( %s )", sanitize(name), query)
}

func (q defaultOfflineSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	fullQuery := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM ( %s )", tempName, query)
	err := q.atomicUpdate(db, tableName, tempName, fullQuery)
	if err != nil {
		return err
	}
	return nil
}
func (q defaultOfflineSQLQueries) transformationExists() string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=%s", bind.Next())
}

func GetTransformationTableName(id ResourceID) (string, error) {
	if err := checkName(id); err != nil {
		return "", err
	}
	return fmt.Sprintf("featureform_transformation__%s__%s", id.Name, id.Variant), nil
}
