// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	db "github.com/jackc/pgx/v4"
	"strings"
	"time"
)

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

type SQLOfflineStore interface {
	IsSQLOfflineStore() bool
	IsSQLProvider() bool
	getConnectionUrl() string
	getDriver() string
	getProviderType() Type
	getQueries() SQLQuery
}

type SQLQuery interface {
	tableExists() string
	registerResourcesFromSourceTableNoTS(db *sql.DB, tableName string, schema ResourceSchema) error
	registerResourcesFromSourceTableWithTS(db *sql.DB, tableName string, schema ResourceSchema) error
	createPrimaryFromSourceTableQuery(tableName string, sourceName string) string
	getColumnNames() string
	createPrimaryTableQuery(name string, columnString string) string
	createMaterialization(tableName string, resultName string) string
	getMaterialization() string
	deleteMaterializaion(tableName string) string
	checkIfMaterializationExists() string
	selectTrainingRows(columns string, trainingSetName string) string
	getValueColumnTypes(tableName string) string
	determineColumnType(valueType ValueType) (string, error)
	newSQLOfflineTable(name string, columnType string) string
	resourceExists(tableName string) string
	writeUpdateQuery(table string) string
	writeInsertsQuery(table string) string
	writeExistsQuery(table string) string
	materializationIterateSegment(tableName string) string
	createValuePlaceholderString(columns []TableColumn) string
	createTrainingSet(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	castTableItemType(v interface{}, t interface{}) interface{}
	getValueColumnType(t *sql.ColumnType) interface{}
	numRows(n interface{}) (int64, error)
	createTransformation(name string, query string) string
}

type sqlOfflineStore struct {
	db     *sql.DB
	parent SQLOfflineStore
	query  SQLQuery
	BaseProvider
}

type sqlSchema struct {
	ValueType
}

func (ps *sqlSchema) Serialize() []byte {
	schema, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}
	return schema
}

func (ps *sqlSchema) Deserialize(schema SerializedTableSchema) error {
	err := json.Unmarshal(schema, ps)
	if err != nil {
		return err
	}
	return nil
}

type sqlConfig struct {
	Username     string
	Password     string
	Organization string
	Account      string
	Database     string
}

func (sf *sqlConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, sf)
	if err != nil {
		return err
	}
	return nil
}

func (sf *sqlConfig) Serialize() []byte {
	conf, err := json.Marshal(sf)
	if err != nil {
		panic(err)
	}
	return conf
}

//
//func sqlOfflineStoreFactory(config SerializedConfig) (Provider, error) {
//	sc := sqlConfig{}
//	if err := sc.Deserialize(config); err != nil {
//		return nil, errors.New("invalid sql config")
//	}
//
//	store, err := NewSQLOfflineStore(sc)
//	if err != nil {
//		return nil, err
//	}
//	return store, nil
//}

// NewPostgresOfflineStore creates a connection to a postgres database
// and initializes a table to track currently active Resource tables.
func NewSQLOfflineStore(config SQLOfflineStore) (*sqlOfflineStore, error) {
	url := config.getConnectionUrl()
	db, err := sql.Open(config.getDriver(), url)
	if err != nil {
		return nil, err
	}
	return &sqlOfflineStore{
		db:     db,
		parent: config,
		query:  config.getQueries(),
		BaseProvider: BaseProvider{
			ProviderType: config.getProviderType(),
			//ProviderConfig: config, // Change this later
		},
	}, nil
}

func (store *sqlOfflineStore) getResourceTableName(id ResourceID) string {
	var idType string
	if id.Type == Feature {
		idType = "feature"
	} else {
		idType = "label"
	}
	return fmt.Sprintf("featureform_resource_%s_%s_%s", idType, id.Name, id.Variant)
}

func (store *sqlOfflineStore) getMaterializationTableName(ftID MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", ftID)
}

func (store *sqlOfflineStore) getTrainingSetName(id ResourceID) string {
	return fmt.Sprintf("featureform_trainingset_%s_%s", id.Name, id.Variant)
}

func GetTransformationName(id ResourceID) string {
	return fmt.Sprintf("featureform_transformation_%s_%s", id.Name, id.Variant)
}

func GetPrimaryTableName(id ResourceID) string {
	return fmt.Sprintf("featureform_primary_%s_%s", id.Name, id.Variant)
}

func (store *sqlOfflineStore) tableExists(id ResourceID) (bool, error) {
	n := -1
	var tableName string
	if id.check(Feature, Label) == nil {
		tableName = store.getResourceTableName(id)
	} else if id.check(TrainingSet) == nil {
		tableName = store.getTrainingSetName(id)
	} else if id.check(Primary) == nil {
		tableName = GetPrimaryTableName(id)
	} else if id.check(Transformation) == nil {
		tableName = GetTransformationName(id)
	}
	query := store.query.tableExists()
	//err := store.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`, tableName).Scan(&n)
	err := store.db.QueryRow(query, tableName).Scan(&n)
	if n == 0 {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (store *sqlOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *sqlOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	if schema.Entity == "" || schema.Value == "" {
		return nil, fmt.Errorf("non-empty entity and value columns required")
	}
	tableName := store.getResourceTableName(id)

	if schema.TS == "" {
		if err := store.query.registerResourcesFromSourceTableNoTS(store.db, tableName, schema); err != nil {
			return nil, err
		}
		//query := fmt.Sprintf("CREATE TABLE %s AS SELECT IDENTIFIER('%s') as entity, IDENTIFIER('%s') as value, null::TIMESTAMP_NTZ as ts FROM TABLE('%s')", sanitize(tableName),
		//	schema.Entity, schema.Value, sanitize(schema.SourceTable))
		//if _, err := store.db.Exec(query); err != nil {
		//	fmt.Println("1:", err)
		//	return nil, err
		//}
		//query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT  %s  UNIQUE (entity, ts)", sanitize(tableName), sanitize(uuid.NewString()))
		//if _, err := store.db.Exec(query); err != nil {
		//	fmt.Println("2:", err)
		//	return nil, err
		//}
		//// Populates empty column with timestamp
		//update := fmt.Sprintf("UPDATE %s SET ts = ?", sanitize(tableName))
		//if _, err := store.db.Exec(update, time.UnixMilli(0).UTC()); err != nil {
		//	fmt.Println("3:", err)
		//	return nil, err
		//}
	} else {
		if err := store.query.registerResourcesFromSourceTableWithTS(store.db, tableName, schema); err != nil {
			return nil, err
		}
		//query := fmt.Sprintf("CREATE TABLE %s AS SELECT IDENTIFIER('%s') as entity,  IDENTIFIER('%s') as value,  IDENTIFIER('%s') as ts FROM TABLE('%s')", sanitize(tableName),
		//	schema.Entity, schema.Value, schema.TS, sanitize(schema.SourceTable))
		//if _, err := store.db.Exec(query); err != nil {
		//	fmt.Println("4:", err)
		//	return nil, err
		//}
		//query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT  %s  UNIQUE (entity, ts)", sanitize(tableName), sanitize(uuid.NewString()))
		//if _, err := store.db.Exec(query); err != nil {
		//	fmt.Println("5:", err)
		//	return nil, err
		//}
	}

	return &sqlOfflineTable{
		db:    store.db,
		name:  tableName,
		query: store.query,
	}, nil
}

func (store *sqlOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName := GetPrimaryTableName(id)
	query := store.query.createPrimaryFromSourceTableQuery(tableName, sourceName)
	//query := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM TABLE('%s')", sanitize(tableName), sanitize(sourceName))
	if _, err := store.db.Exec(query); err != nil {
		return nil, err
	}
	columnQuery := store.query.getColumnNames()
	//rows, err := store.db.Query(
	//	"SELECT column_name FROM information_schema.columns WHERE table_name = ? order by ordinal_position",
	//	tableName)
	rows, err := store.db.Query(columnQuery, tableName)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
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
	tableName := GetPrimaryTableName(id)
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

// createPrimaryTableQuery creates a query for table creation based on the
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
	return store.query.createPrimaryTableQuery(name, columnString), nil
	//return fmt.Sprintf("CREATE TABLE %s ( %s )", sanitize(name), columnString), nil
}

// determineColumnType returns an acceptable Postgres column Type to use for the given value
//func determinesqlColumnType(valueType ValueType) (string, error) {
//	switch valueType {
//	case Int, Int32, Int64:
//		return "INT", nil
//	case Float32, Float64:
//		return "FLOAT8", nil
//	case String:
//		return "VARCHAR", nil
//	case Bool:
//		return "BOOLEAN", nil
//	case Timestamp:
//		return "TIMESTAMP_NTZ", nil
//	case NilType:
//		return "VARCHAR", nil
//	default:
//		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
//	}
//}

//func GetPrimaryTableName(id ResourceID) string {
//	return fmt.Sprintf("featureform_primary_%s_%s", id.Name, id.Variant)
//}

func (store *sqlOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	name := GetPrimaryTableName(id)
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	columnQuery := store.query.getColumnNames()
	rows, err := store.db.Query(columnQuery, name)
	//rows, err := store.db.Query(
	//	"SELECT column_name FROM information_schema.columns WHERE table_name = ? order by ordinal_position",
	//	name)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
	return &sqlPrimaryTable{
		db:     store.db,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *sqlOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	name := GetTransformationName(id)
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	columnQuery := store.query.getColumnNames()
	rows, err := store.db.Query(columnQuery, name)
	//rows, err := store.db.Query(
	//	"SELECT column_name FROM information_schema.columns WHERE table_name = ? order by ordinal_position",
	//	name)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
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
		return nil, err
	}

	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName := store.getResourceTableName(id)
	var valueType ValueType
	if valueIndex := store.getValueIndex(schema.Columns); valueIndex > 0 {
		valueType = schema.Columns[valueIndex].ValueType
	} else {
		valueType = NilType
	}
	table, err := store.newsqlOfflineTable(store.db, tableName, valueType)
	if err != nil {
		return nil, err
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
	query     SQLQuery
}

func (mat *sqlMaterialization) ID() MaterializationID {
	return mat.id
}

// NumRows checks for the max row number to return as the number of rows.
// If there are no rows in the table, the interface n is checked for Nil,
// otherwise the interface is converted from a string to an int64
func (mat *sqlMaterialization) NumRows() (int64, error) {
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
	intVar, err := mat.query.numRows(n)
	if err != nil {
		return 0, nil
	}
	return intVar, nil
	//if intVar, err := strconv.Atoi(n.(string)); err != nil {
	//	return 0, err
	//} else {
	//	return int64(intVar), nil
	//}
}

func (mat *sqlMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName)
	//query := fmt.Sprintf(""+
	//	"SELECT entity, value, ts FROM ( SELECT * FROM %s WHERE row_number>? AND row_number<=?)", sanitize(mat.tableName))

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
	query        SQLQuery
}

func newsqlFeatureIterator(rows *sql.Rows, columnType interface{}, query SQLQuery) FeatureIterator {
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
	materializeQry := store.query.createMaterialization(matTableName, resTable.name)

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

	getMatQry := store.query.getMaterialization()

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
	return &sqlMaterialization{
		id:        id,
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
	query := store.query.deleteMaterializaion(tableName)
	if _, err := store.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (store *sqlOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := store.query.checkIfMaterializationExists()
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

func (store *sqlOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getsqlResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName := store.getTrainingSetName(def.ID)
	if err := store.query.createTrainingSet(store, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TrainingSetNotFound{id}
	}
	trainingSetName := store.getTrainingSetName(id)
	columnQuery := store.query.getColumnNames()
	rows, err := store.db.Query(columnQuery, trainingSetName)

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
	trainingSetQry := store.query.selectTrainingRows(columns, trainingSetName)

	rows, err = store.db.Query(trainingSetQry)
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
	query           SQLQuery
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
	return &sqlOfflineTable{
		db:    store.db,
		name:  store.getResourceTableName(id),
		query: store.query,
	}, nil
}

type sqlOfflineTable struct {
	db    *sql.DB
	query SQLQuery
	name  string
}

type sqlPrimaryTable struct {
	db     *sql.DB
	name   string
	query  SQLQuery
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

	query := pt.query.getColumnNames()
	rows, err := pt.db.Query(query, pt.name)

	defer rows.Close()
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			fmt.Println("COL ERROR HERE")
			return nil, err
		}
		columnNames = append(columnNames, sanitize(column))
	}
	columns := strings.Join(columnNames[:], ", ")
	query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", columns, sanitize(pt.name), n)
	rows, err = pt.db.Query(query)
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
		return nil, err
	}
	tableCreateQry := store.query.newSQLOfflineTable(name, columnType)
	//tableCreateQry := fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMP_NTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
	_, err = db.Exec(tableCreateQry)
	if err != nil {
		return nil, err
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
	existsQuery := table.query.writeExistsQuery(tb)

	if err := table.db.QueryRow(existsQuery, rec.Entity, rec.TS).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		insertQuery := table.query.writeInsertsQuery(tb)

		if _, err := table.db.Exec(insertQuery, rec.Entity, rec.Value, rec.TS); err != nil {
			return err
		}
	} else if n > 0 {
		updateQuery := table.query.writeUpdateQuery(tb)

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

func (store *sqlOfflineStore) CreateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	// Only allow transformations to be run with SELECT queries

	query := store.query.createTransformation(name, config.Query)
	if _, err := store.db.Exec(query); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) createTransformationName(id ResourceID) (string, error) {
	switch id.Type {
	case Label, Feature:
		return store.getResourceTableName(id), nil
	case Transformation:
		return GetTransformationName(id), nil
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
	query         SQLQuery
}

func newsqlGenericTableIterator(rows *sql.Rows, columnTypes []interface{}, columnNames []string, query SQLQuery) GenericTableIterator {
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
