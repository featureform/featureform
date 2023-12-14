package provider

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

const (
	chInt     = "Int"
	chInt32   = "Int32"
	chUInt32  = "UInt32"
	chInt64   = "Int64"
	chUInt64  = "UInt64"
	chFloat64 = "Float64"
	chFloat32 = "Float32"
	chString  = "String"
	chBool    = "Bool"
	// we assume nanoseconds
	chDateTime   = "DateTime64(9)"
	chDateTime64 = "DateTime64(9)"
)

func sanitizeCH(ident string) string {
	s := strings.ReplaceAll(ident, string([]byte{0}), "")
	return "`" + s + "`"
}

type clickHouseOfflineStore struct {
	sqlOfflineStore
}

func (store *clickHouseOfflineStore) getResourceTableName(id ResourceID) (string, error) {
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

func (store *clickHouseOfflineStore) getTrainingSetName(id ResourceID) (string, error) {
	if err := checkName(id); err != nil {
		return "", err
	}
	return fmt.Sprintf("featureform_trainingset__%s__%s", id.Name, id.Variant), nil
}

func (store *clickHouseOfflineStore) tableExists(id ResourceID) (bool, error) {
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

func (store *clickHouseOfflineStore) createTransformationName(id ResourceID) (string, error) {
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

func (store *clickHouseOfflineStore) createsqlPrimaryTableQuery(name string, schema TableSchema) (string, error) {
	columns := make([]string, 0)
	for _, column := range schema.Columns {
		columnType, err := store.query.determineColumnType(column.ValueType)
		if err != nil {
			return "", err
		}
		//enforce all columns as nullable
		columns = append(columns, fmt.Sprintf("%s Nullable(%s)", column.Name, columnType))
	}
	columnString := strings.Join(columns, ", ")
	return store.query.primaryTableCreate(name, columnString), nil
}

func (store *clickHouseOfflineStore) newsqlPrimaryTable(db *sql.DB, name string, schema TableSchema) (*clickhousePrimaryTable, error) {
	query, err := store.createsqlPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}
	return &clickhousePrimaryTable{
		db:     db,
		name:   name,
		schema: schema,
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *clickHouseOfflineStore) newsqlOfflineTable(db *sql.DB, name string, valueType ValueType) (*clickhouseOfflineTable, error) {
	columnType, err := determineColumnType(valueType)
	if err != nil {
		return nil, fmt.Errorf("could not determine column type: %v", err)
	}
	tableCreateQry := store.query.newSQLOfflineTable(name, columnType)
	_, err = db.Exec(tableCreateQry)
	if err != nil {
		return nil, fmt.Errorf("could not create table query: %v", err)
	}
	return &clickhouseOfflineTable{
		db:    db,
		name:  name,
		query: store.query,
	}, nil
}

func (store *clickHouseOfflineStore) getsqlResourceTable(id ResourceID) (*clickhouseOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	table, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	return &clickhouseOfflineTable{
		db:    store.db,
		name:  table,
		query: store.query,
	}, nil
}

func (store *clickHouseOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := store.query.materializationExists()
	n := -1
	err := store.db.QueryRow(getMatQry, tableName).Scan(&n)
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	return true, nil
}

func (store *clickHouseOfflineStore) getMaterializationTableName(id MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", id)
}

func clickhouseOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	store, err := NewClickHouseOfflineStore(config)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func NewClickHouseOfflineStore(config pc.SerializedConfig) (*clickHouseOfflineStore, error) {
	cc := pc.ClickHouseConfig{}
	if err := cc.Deserialize(config); err != nil {
		return nil, NewProviderError(Runtime, pt.ClickHouseOffline, ConfigDeserialize, err.Error())
	}
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cc.Host, cc.Port)},
		Auth: clickhouse.Auth{
			Database: cc.Database,
			Username: cc.Username,
			Password: cc.Password,
		},
		Settings: clickhouse.Settings{
			"final": "1",
		},
	})
	queries := clickhouseSQLQueries{}
	// numeric and should work
	queries.setVariableBinding(PostgresBindingStyle)
	sgConfig := SQLOfflineStoreConfig{
		Config:       config,
		Driver:       "clickhouse",
		ProviderType: pt.ClickHouseOffline,
		QueryImpl:    &queries,
	}
	if err := db.Ping(); err != nil {
		return nil, NewProviderError(Connection, pt.ClickHouseOffline, ClientInitialization, err.Error())
	}
	//we bypass NewSQLOfflineStore as we want to estalish our connection using non dsn syntax
	return &clickHouseOfflineStore{sqlOfflineStore{
		db:     db,
		parent: sgConfig,
		query:  &queries,
		BaseProvider: BaseProvider{
			ProviderType:   pt.ClickHouseOffline,
			ProviderConfig: config,
		},
	}}, nil
}

type clickhouseOfflineTable struct {
	db    *sql.DB
	query OfflineTableQueries
	name  string
}

func (table *clickhouseOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := sanitizeCH(table.name)
	if err := rec.check(); err != nil {
		return err
	}
	// we use a ReplacingMergeTree off offline tables so and thus don't distinguish between updates and inserts
	insertQuery := table.query.writeInserts(tb)
	_, err := table.db.Exec(insertQuery, rec.Entity, rec.Value, rec.TS)
	return err
}

const batchSize = 10000

func (table *clickhouseOfflineTable) WriteBatch(recs []ResourceRecord) error {
	tb := sanitizeCH(table.name)
	scope, err := table.db.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s (entity, value, ts)", tb))
	if err != nil {
		return err
	}
	b := 0
	for i, _ := range recs {
		if recs[i].Entity == "" && recs[i].Value == nil && recs[i].TS.IsZero() {
			return fmt.Errorf("invalid record at offset %d", i)
		}
		ts := recs[i].TS
		// insert empty time.Time{} as 1970
		ts = checkZeroTime(recs[i].TS)
		_, err := batch.Exec(recs[i].Entity, recs[i].Value, ts)
		if err != nil {
			return err
		}
		b += 1
		if b == batchSize {
			err = scope.Commit()
			if err != nil {
				return err
			}
			batch, err = scope.Prepare(fmt.Sprintf("INSERT INTO %s (entity, value, ts)", tb))
			if err != nil {
				return err
			}
			b = 0
		}
	}
	return scope.Commit()
}

type clickhousePrimaryTable struct {
	db     *sql.DB
	name   string
	query  OfflineTableQueries
	schema TableSchema
}

func (table *clickhousePrimaryTable) Write(rec GenericRecord) error {
	tb := sanitizeCH(table.name)
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

func (table *clickhousePrimaryTable) WriteBatch(recs []GenericRecord) error {
	tb := sanitizeCH(table.name)
	columns := table.getColumnNameString()
	scope, err := table.db.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s (%s)", tb, columns))
	if err != nil {
		return err
	}
	b := 0
	for i, _ := range recs {
		_, err := batch.Exec(recs[i]...)
		if err != nil {
			return err
		}
		b += 1
		if b == batchSize {
			err = scope.Commit()
			if err != nil {
				return err
			}
			batch, err = scope.Prepare(fmt.Sprintf("INSERT INTO %s (%s)", tb, columns))
			if err != nil {
				return err
			}
			b = 0
		}
	}
	return scope.Commit()
}

func (table *clickhousePrimaryTable) getColumnNameString() string {
	columns := make([]string, 0)
	for _, column := range table.schema.Columns {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns, ", ")
}

func (table *clickhousePrimaryTable) GetName() string {
	return table.name
}

func (table *clickhousePrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	columns, err := table.query.getColumns(table.db, table.name)
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, sanitizeCH(col.Name))
	}
	names := strings.Join(columnNames[:], ", ")
	var query string
	if n == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", names, sanitizeCH(table.name))
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", names, sanitizeCH(table.name), n)
	}
	rows, err := table.db.Query(query)
	if err != nil {
		return nil, err
	}
	colTypes, err := table.getValueColumnTypes(table.name)
	if err != nil {
		return nil, err
	}
	return newClickHouseTableIterator(rows, colTypes, columnNames, table.query), nil
}

func (table *clickhousePrimaryTable) getValueColumnTypes(tb string) ([]interface{}, error) {
	query := table.query.getValueColumnTypes(tb)
	rows, err := table.db.Query(query)
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
			colTypes = append(colTypes, table.query.getValueColumnType(t))
		}
	}
	return colTypes, nil
}

func (table clickhousePrimaryTable) NumRows() (int64, error) {
	n := int64(0)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitizeCH(table.name))
	rows := table.db.QueryRow(query)

	err := rows.Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

type clickHouseTableIterator struct {
	rows          *sql.Rows
	currentValues GenericRecord
	err           error
	columnTypes   []interface{}
	columnNames   []string
	query         OfflineTableQueries
}

func newClickHouseTableIterator(rows *sql.Rows, columnTypes []interface{}, columnNames []string, query OfflineTableQueries) GenericTableIterator {
	return &clickHouseTableIterator{
		rows:          rows,
		currentValues: nil,
		err:           nil,
		columnTypes:   columnTypes,
		columnNames:   columnNames,
		query:         query,
	}
}

// Next custom implementation for ClickHouse as we need to pass pointers to pointers for nullable types (so we can detect nil)
func (it *clickHouseTableIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	columnTypes, err := it.rows.ColumnTypes()
	if err != nil {
		it.err = err
		it.rows.Close()
		return false
	}
	pointers := make([]interface{}, len(columnTypes))
	for i := range columnTypes {
		elementType := columnTypes[i].ScanType()
		// Create a pointer to a pointer - we need for nulls
		pointers[i] = reflect.New(elementType).Interface()
	}
	if err := it.rows.Scan(pointers...); err != nil {
		it.rows.Close()
		it.err = err
		return false
	}
	rowValues := make(GenericRecord, len(columnTypes))
	for i, value := range pointers {
		rowValues[i] = it.query.castTableItemType(value, it.columnTypes[i])
	}
	it.currentValues = rowValues
	return true
}

func (it *clickHouseTableIterator) Values() GenericRecord {
	return it.currentValues
}

func (it *clickHouseTableIterator) Columns() []string {
	return it.columnNames
}

func (it *clickHouseTableIterator) Err() error {
	return it.err
}

func (it *clickHouseTableIterator) Close() error {
	return it.rows.Close()
}

func (store *clickHouseOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
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

	return &clickhouseOfflineTable{
		db:    store.db,
		name:  tableName,
		query: store.query,
	}, nil
}

func (store *clickHouseOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *clickHouseOfflineStore) CheckHealth() (bool, error) {
	err := store.db.Ping()
	if err != nil {
		return false, NewProviderError(Connection, store.Type(), Ping, err.Error())
	}
	return true, nil
}

func (store *clickHouseOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
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

	return &clickhousePrimaryTable{
		db:     store.db,
		name:   tableName,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) CreateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	queries := store.query.transformationCreate(name, config.Query)
	for _, query := range queries {
		if _, err := store.db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (store *clickHouseOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	n := -1
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	existsQuery := store.query.tableExists()
	err = store.db.QueryRow(existsQuery, name).Scan(&n)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, fmt.Errorf("transformation not found: %v", name)
	}
	columnNames, err := store.query.getColumns(store.db, name)

	return &clickhousePrimaryTable{
		db:     store.db,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) UpdateTransformation(config TransformationConfig) error {
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

func (store *clickHouseOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
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

func (store *clickHouseOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
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

func (store *clickHouseOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
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

func (store *clickHouseOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getsqlResourceTable(id)
}

func (store *clickHouseOfflineStore) GetBatchFeatures(ids []ResourceID) (BatchFeatureIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (store *clickHouseOfflineStore) CreateMaterialization(id ResourceID, options ...MaterializationOptions) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	resTable, err := store.getsqlResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(id.Name)
	matTableName := store.getMaterializationTableName(matID)
	materializeQueries := store.query.materializationCreate(matTableName, resTable.name)
	for _, materializeQry := range materializeQueries {
		_, err = store.db.Exec(materializeQry)
		if err != nil {
			return nil, err
		}
	}
	return &clickHouseMaterialization{
		id:        matID,
		db:        store.db,
		tableName: matTableName,
		query:     store.query,
	}, nil
}

func (store *clickHouseOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {

	tableName := store.getMaterializationTableName(id)

	getMatQry := store.query.materializationExists()
	n := -1
	err := store.db.QueryRow(getMatQry, tableName).Scan(&n)
	if err != nil {
		return nil, fmt.Errorf("could not get materialization: %w", err)
	}
	if n == 0 {
		return nil, &MaterializationNotFound{id}
	}
	return &clickHouseMaterialization{
		id:        id,
		db:        store.db,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *clickHouseOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
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
	return &clickHouseMaterialization{
		id:        matID,
		db:        store.db,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *clickHouseOfflineStore) DeleteMaterialization(id MaterializationID) error {
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

func (store *clickHouseOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
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
	if err := store.query.trainingSetCreate(&store.sqlOfflineStore, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *clickHouseOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
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
	if err := store.query.trainingSetUpdate(&store.sqlOfflineStore, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *clickHouseOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
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
		features = append(features, sanitizeCH(name.Name))
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

func (store *clickHouseOfflineStore) Close() error {
	return store.db.Close()
}

type clickhouseSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q clickhouseSQLQueries) tableExists() string {
	return "SELECT count() FROM system.tables WHERE table = $1"
}

func (q clickhouseSQLQueries) viewExists() string {
	return "SELECT count() FROM system.tables WHERE table = $1 AND engine='View'"
}

func (q clickhouseSQLQueries) primaryTableCreate(name string, columnString string) string {
	return fmt.Sprintf("CREATE TABLE %s ( %s ) ENGINE=MergeTree ORDER BY ()", sanitizeCH(name), columnString)
}

func (q clickhouseSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, %s as ts FROM %s", sanitizeCH(tableName),
			sanitizeCH(schema.Entity), sanitizeCH(schema.Value), sanitizeCH(schema.TS), sanitizeCH(schema.SourceTable))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, toDateTime64(0, 9) AS ts FROM %s", sanitizeCH(tableName),
			sanitizeCH(schema.Entity), sanitizeCH(schema.Value), sanitizeCH(schema.SourceTable))
	}
	fmt.Printf("Resource creation query: %s", query)
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (q clickhouseSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", sanitizeCH(tableName), sourceName)
}

func (q clickhouseSQLQueries) materializationCreate(tableName string, sourceName string) []string {
	return []string{fmt.Sprintf("CREATE TABLE %s ENGINE = ReplacingMergeTree ORDER BY (entity, ts) SETTINGS allow_nullable_key=1 EMPTY AS SELECT * FROM %s", sanitizeCH(tableName), sanitizeCH(sourceName)),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN row_number UInt64;", sanitizeCH(tableName)),
		fmt.Sprintf("INSERT INTO %s SELECT entity, value, tis AS ts, row_number() OVER () AS row_number FROM (SELECT entity, max(ts) AS tis, argMax(value, ts) AS value FROM %s GROUP BY entity ORDER BY entity ASC, value ASC);", sanitizeCH(tableName), sanitizeCH(sourceName)),
	}
	return nil
}

func (q clickhouseSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	// create a new table
	currentTime := time.Now()
	epochMilliseconds := currentTime.UnixNano() / int64(time.Millisecond)
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s AS %s", sanitizeCH(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)), sanitizeCH(tableName))); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s SELECT entity, value, tis AS ts, row_number() OVER () AS row_number FROM (SELECT entity, max(ts) AS tis, argMax(value, ts) AS value FROM %s GROUP BY entity ORDER BY entity ASC, value ASC);", sanitizeCH(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)), sanitizeCH(sourceName))); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf("EXCHANGE TABLES %s AND %s", sanitizeCH(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)), sanitizeCH(tableName))); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", sanitizeCH(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)))); err != nil {
		return err
	}
	return nil
}

func (q clickhouseSQLQueries) materializationExists() string {
	return q.tableExists()
}

func (q clickhouseSQLQueries) determineColumnType(valueType ValueType) (string, error) {
	switch valueType {
	case Int:
		return chInt, nil
	case Int64:
		return chInt64, nil
	case Int32:
		return chInt32, nil
	case Float32:
		return chFloat32, nil
	case Float64:
		return chFloat64, nil
	case String:
		return chString, nil
	case Bool:
		return chBool, nil
	case Timestamp:
		//TODO: determine precision
		return chDateTime, nil
	case NilType:
		return "Null", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func (q clickhouseSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity String, value Nullable(%s), ts DateTime64(9)) ENGINE = ReplacingMergeTree ORDER BY (entity, ts) SETTINGS allow_nullable_key=1", sanitizeCH(name), columnType)
}

func (q clickhouseSQLQueries) writeUpdate(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (%s, %s, %s)", table, bind.Next(), bind.Next(), bind.Next())
}

func (q clickhouseSQLQueries) writeInserts(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (%s, %s, %s)", table, bind.Next(), bind.Next(), bind.Next())
}

func (q clickhouseSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q clickhouseSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q clickhouseSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func buildTrainingSelect(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) (string, error) {
	columns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		if err != nil {
			return "", err
		}
		santizedName := sanitizeCH(tableName)
		tableJoinAlias := fmt.Sprintf("t%d", i)
		columns = append(columns, fmt.Sprintf("%s.value AS %s", tableJoinAlias, santizedName))
		query = fmt.Sprintf("%s ASOF LEFT JOIN (SELECT entity, value, ts FROM %s) AS %s ON (%s.entity = l.entity) AND (%s.ts <= l.ts)",
			query, santizedName, tableJoinAlias, tableJoinAlias, tableJoinAlias)
	}
	columnStr := strings.Join(columns, ", ")
	query = fmt.Sprintf("SELECT %s, l.value as label FROM %s AS l %s", columnStr, sanitizeCH(labelName), query)
	return query, nil
}

func (q clickhouseSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	query, err := buildTrainingSelect(store, def, tableName, labelName)
	if err != nil {
		return err
	}
	if !isUpdate {
		// use a 2-step EMPTY create so ClickHouse Cloud compatible
		createQuery := fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY label SETTINGS allow_nullable_key=1 EMPTY AS (%s)", sanitizeCH(tableName), query)
		if _, err := store.db.Exec(createQuery); err != nil {
			return err
		}
		insertQuery := fmt.Sprintf("INSERT INTO %s %s", sanitizeCH(tableName), query)
		if _, err := store.db.Exec(insertQuery); err != nil {
			return err
		}
	} else {
		tempName := sanitizeCH(fmt.Sprintf("tmp_%s", tableName))
		createQuery := fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY label SETTINGS allow_nullable_key=1 EMPTY AS (%s)", tempName, query)
		if _, err := store.db.Exec(createQuery); err != nil {
			return err
		}
		insertQuery := fmt.Sprintf("INSERT INTO %s %s", tempName, query)
		if _, err := store.db.Exec(insertQuery); err != nil {
			return err
		}
		if _, err := store.db.Exec(fmt.Sprintf("EXCHANGE TABLES %s AND %s", sanitizeCH(tableName), tempName)); err != nil {
			return err
		}
		if _, err := store.db.Exec(fmt.Sprintf("DROP TABLE %s", tempName)); err != nil {
			return err
		}
		if err != nil {
			return err
		}
	}
	return nil
}

const pattern = `Nullable\((.*)\)`

var nullableRe = regexp.MustCompile(pattern)

func deferencePointer(v interface{}) interface{} {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			//return null as nil
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

func checkZeroTime(t time.Time) time.Time {
	//1970-01-01 in datetime64 in ClickHouse is 0 but client returns this as time.Time{} which is 0 year
	if t.IsZero() {
		return time.UnixMilli(0).UTC()
	}
	return t.UTC()
}

func (q clickhouseSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	// v might be a pointer to a pointer (so we can handle nulls)
	v = deferencePointer(v)
	//type might be nullable.
	match := nullableRe.FindStringSubmatch(t.(string))
	if len(match) == 2 {
		t = match[1]
	}
	switch t {
	case chInt:
		return v.(int)
	case chInt32:
		return int(v.(int32))
	case chUInt32:
		if intValue := v.(uint32); intValue <= math.MaxInt32 {
			return int(intValue)
		}
		return v.(uint32)
	case chInt64:
		if intValue := v.(int64); intValue >= math.MinInt && intValue <= math.MaxInt {
			return int(intValue)
		}
		return v.(int64)
	case chUInt64:
		if intValue := v.(uint64); intValue <= uint64(math.MaxInt) {
			return int(intValue)
		}
		return v.(uint64)
	case chDateTime:
		return checkZeroTime(v.(time.Time))
	default:
		// other types don't need checking as will be correct type by client
		return v
	}
}

func (q clickhouseSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	return t.DatabaseTypeName()
}

func (q clickhouseSQLQueries) numRows(n interface{}) (int64, error) {
	// not ideal but this is what the OfflineTableQueries supports
	return int64(n.(uint64)), nil
}

func (q clickhouseSQLQueries) transformationCreate(name string, query string) []string {
	// 2-step with EMPTY for ClickHouse Cloud support
	return []string{
		fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY tuple() EMPTY AS %s;",
			sanitizeCH(name), query),
		fmt.Sprintf(" INSERT INTO %s %s;", sanitizeCH(name), query),
	}
}

func (q clickhouseSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitizeCH(fmt.Sprintf("tmp_%s", tableName))
	createQuery := fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY tuple() EMPTY AS %s", tempName, query)
	if _, err := db.Exec(createQuery); err != nil {
		return err
	}
	insertQuery := fmt.Sprintf("INSERT INTO %s %s", tempName, query)
	if _, err := db.Exec(insertQuery); err != nil {
		return err
	}
	exchangeTableQuery := fmt.Sprintf("EXCHANGE TABLES %s AND %s", tempName, sanitizeCH(tableName))
	if _, err := db.Exec(exchangeTableQuery); err != nil {
		return err
	}
	dropTableQuery := fmt.Sprintf("DROP TABLE %s", tempName)
	if _, err := db.Exec(dropTableQuery); err != nil {
		return err
	}
	return nil
}

func (q clickhouseSQLQueries) transformationExists() string {
	return q.tableExists()
}

func (q clickhouseSQLQueries) getColumns(db *sql.DB, tableName string) ([]TableColumn, error) {
	qry := fmt.Sprintf("SELECT name FROM system.columns WHERE table = '%s'", tableName)
	rows, err := db.Query(qry)
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

func (q clickhouseSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitizeCH(tableName))
}

func (q clickhouseSQLQueries) materializationIterateSegment(tableName string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT entity, value, ts FROM (SELECT * FROM %s WHERE row_number>%s AND row_number<=%s)t1", sanitizeCH(tableName), bind.Next(), bind.Next())
}

type clickHouseMaterialization struct {
	id        MaterializationID
	db        *sql.DB
	tableName string
	query     OfflineTableQueries
}

func (mat *clickHouseMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *clickHouseMaterialization) NumRows() (int64, error) {
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

func (mat *clickHouseMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName)
	fmt.Println(query)
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
	return newClickHouseFeatureIterator(rows, colType, mat.query), nil
}

func newClickHouseFeatureIterator(rows *sql.Rows, columnType interface{}, query OfflineTableQueries) FeatureIterator {
	return &clickHouseFeatureIterator{
		rows:         rows,
		err:          nil,
		currentValue: ResourceRecord{},
		columnType:   columnType,
		query:        query,
	}
}

type clickHouseFeatureIterator struct {
	rows         *sql.Rows
	err          error
	currentValue ResourceRecord
	columnType   interface{}
	query        OfflineTableQueries
}

func (iter *clickHouseFeatureIterator) Next() bool {
	if !iter.rows.Next() {
		iter.rows.Close()
		return false
	}
	var rec ResourceRecord
	var entity interface{}
	var value interface{}
	var ts time.Time
	if err := iter.rows.Scan(&entity, &value, &ts); err != nil {
		iter.rows.Close()
		iter.err = err
		return false
	}
	if err := rec.SetEntity(entity); err != nil {
		iter.err = err
		return false
	}
	rec.Value = iter.query.castTableItemType(value, iter.columnType)
	rec.TS = checkZeroTime(ts)
	iter.currentValue = rec
	return true
}

func (iter *clickHouseFeatureIterator) Value() ResourceRecord {
	return iter.currentValue
}

func (iter *clickHouseFeatureIterator) Err() error {
	return nil
}

func (iter *clickHouseFeatureIterator) Close() error {
	return iter.rows.Close()
}
