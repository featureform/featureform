// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	db "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"strings"
	"time"
)

// postgresColumnType is used to specify the column type of a resource value.
type postgresColumnType string

const (
	PGInt    postgresColumnType = "integer"
	PGFloat                     = "float8"
	PGString                    = "varchar"
	PGBool                      = "boolean"
)

type PostgresTableSchema struct {
	ValueType
}

type postgresOfflineStore struct {
	conn *pgxpool.Pool
	ctx  context.Context
	BaseProvider
}

type PostgresConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
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
	pg := PostgresConfig{}
	if err := pg.Deserialize(config); err != nil {
		return nil, errors.New("invalid postgres config")
	}

	store, err := NewPostgresOfflineStore(pg)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// NewPostgresOfflineStore creates a connection to a postgres database
// and initializes a table to track currently active Resource tables.
func NewPostgresOfflineStore(pg PostgresConfig) (*postgresOfflineStore, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pg.Username, pg.Password, pg.Host, pg.Port, pg.Database)
	ctx := context.Background()
	conn, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	return &postgresOfflineStore{
		conn: conn,
		ctx:  ctx,
		BaseProvider: BaseProvider{
			ProviderType:   PostgresOffline,
			ProviderConfig: pg.Serialize(),
		},
	}, nil
}

func (store *postgresOfflineStore) getPrimaryTableName(id ResourceID) string {
	return fmt.Sprintf("featureform_primary_%s_%s", id.Name, id.Variant)
}

func (store *postgresOfflineStore) getResourceTableName(id ResourceID) string {
	var idType string
	if id.Type == Feature {
		idType = "feature"
	} else {
		idType = "label"
	}
	return fmt.Sprintf("featureform_resource_%s_%s_%s", idType, id.Name, id.Variant)
}

func (store *postgresOfflineStore) getMaterializationTableName(ftID MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", ftID)
}

func (store *postgresOfflineStore) getTrainingSetName(id ResourceID) string {
	return fmt.Sprintf("featureform_trainingset_%s_%s", id.Name, id.Variant)
}

func (store *postgresOfflineStore) tableExists(id ResourceID) (bool, error) {
	var n int64
	var tableName string
	if id.check(Feature, Label) == nil {
		tableName = store.getResourceTableName(id)
	} else if id.check(TrainingSet) == nil {
		tableName = store.getTrainingSetName(id)
	} else if id.check(Primary) == nil {
		tableName = store.getPrimaryTableName(id)
	}
	err := store.conn.QueryRow(context.Background(), "SELECT 1 FROM information_schema.tables WHERE table_name=$1", tableName).Scan(&n)
	if err == db.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (store *postgresOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *postgresOfflineStore) AsSQLOfflineStore() (SQLOfflineStore, error) {
	return store, nil
}

func (store *postgresOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName := store.getPrimaryTableName(id)
	table, err := newPostgresPrimaryTable(store.conn, tableName, schema)
	if err != nil {
		return nil, err
	}
	return table, nil
}

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *postgresOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
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
	table, err := newPostgresOfflineTable(store.conn, tableName, valueType)
	if err != nil {
		return nil, err
	}
	return table, nil
}

// getValueIndex returns the index of the value column in the schema.
// Returns -1 if an entity column is not found
func (store *postgresOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *postgresOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getPostgresResourceTable(id)
}

func (store *postgresOfflineStore) getPostgresResourceTable(id ResourceID) (*postgresOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return &postgresOfflineTable{
		conn: store.conn,
		name: store.getResourceTableName(id),
	}, nil
}

func (store *postgresOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	resTable, err := store.getPostgresResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(id.Name)
	matTableName := store.getMaterializationTableName(matID)
	sanitizedTableName := sanitize(matTableName)
	resTableName := sanitize(resTable.name)
	tableCreateQry := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts FROM %s WHERE 1=2)", sanitizedTableName, resTableName)

	_, err = store.conn.Exec(
		context.Background(), tableCreateQry)
	if err != nil {
		return nil, err
	}

	materializeQry := fmt.Sprintf(
		"INSERT INTO %s SELECT entity, value, ts FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1", sanitizedTableName, resTableName)

	_, err = store.conn.Exec(context.Background(), materializeQry)
	if err != nil {
		return nil, err
	}

	return &postgresMaterialization{
		id:        matID,
		conn:      store.conn,
		tableName: matTableName,
	}, nil

}
func (store *postgresOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	tableName := store.getMaterializationTableName(id)
	if exists, err := store.materializationExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &MaterializationNotFound{id}
	}
	return &postgresMaterialization{
		id:        id,
		conn:      store.conn,
		tableName: tableName,
	}, nil
}

func (store *postgresOfflineStore) DeleteMaterialization(id MaterializationID) error {
	tableName := store.getMaterializationTableName(id)
	if exists, err := store.materializationExists(id); err != nil {
		return err
	} else if !exists {
		return &MaterializationNotFound{id}
	}
	query := fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
	if _, err := store.conn.Exec(context.Background(), query); err != nil {
		return err
	}
	return nil
}

func (store *postgresOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=$1")
	rows, err := store.conn.Query(context.Background(), getMatQry, tableName)
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

func (store *postgresOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getPostgresResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName := store.getTrainingSetName(def.ID)

	columns := make([]string, 0)
	query := fmt.Sprintf(" (SELECT entity, value , ts from %s ) l ", sanitize(label.name))
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

	if _, err := store.conn.Exec(context.Background(), fullQuery); err != nil {
		return err
	}
	return nil
}

func (store *postgresOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TrainingSetNotFound{id}
	}
	trainingSetName := store.getTrainingSetName(id)
	rows, err := store.conn.Query(
		context.Background(),
		"SELECT column_name FROM information_schema.columns WHERE table_name = $1 order by ordinal_position",
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
	rows, err = store.conn.Query(context.Background(), trainingSetQry)
	if err != nil {
		return nil, err
	}
	colTypes, err := store.getValueColumnTypes(trainingSetName)
	if err != nil {
		return nil, err
	}
	return newPostgresTrainingSetIterator(rows, colTypes), nil
}

// getValueColumnTypes returns a list of column types. Columns consist of feature and label values
// within a training set.
func (store *postgresOfflineStore) getValueColumnTypes(table string) ([]postgresColumnType, error) {
	rows, err := store.conn.Query(context.Background(),
		"select data_type from (select column_name, data_type from information_schema.columns where table_name = $1 order by ordinal_position) t",
		table)
	if err != nil {
		return nil, err
	}
	colTypes := make([]postgresColumnType, 0)
	for rows.Next() {
		if types, err := rows.Values(); err != nil {
			return nil, err
		} else {
			for _, t := range types {
				colTypes = append(colTypes, postgresColumnType(t.(string)))
			}
		}
	}
	return colTypes, nil
}

type postgresTrainingRowsIterator struct {
	rows            db.Rows
	currentFeatures []interface{}
	currentLabel    interface{}
	err             error
	columnTypes     []postgresColumnType
}

func newPostgresTrainingSetIterator(rows db.Rows, columnTypes []postgresColumnType) TrainingSetIterator {
	return &postgresTrainingRowsIterator{
		rows:            rows,
		currentFeatures: nil,
		currentLabel:    nil,
		err:             nil,
		columnTypes:     columnTypes,
	}
}

func (it *postgresTrainingRowsIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	var label interface{}
	values, err := it.rows.Values()
	if err != nil {
		it.rows.Close()
		it.err = err
		return false
	}
	numFeatures := len(values) - 1
	featureVals := make([]interface{}, numFeatures)
	for i, value := range values {
		if i < numFeatures {
			featureVals[i] = castPostgresTableItemType(value, it.columnTypes[i])
		} else {
			label = castPostgresTableItemType(value, it.columnTypes[i])
		}
	}
	it.currentFeatures = featureVals
	it.currentLabel = label
	return true
}

func (it *postgresTrainingRowsIterator) Err() error {
	return it.err
}

func (it *postgresTrainingRowsIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *postgresTrainingRowsIterator) Label() interface{} {
	return it.currentLabel
}

type postgresOfflineTable struct {
	conn *pgxpool.Pool
	name string
}

type postgresPrimaryTable struct {
	conn   *pgxpool.Pool
	name   string
	schema TableSchema
}

// determineColumnType returns an acceptable Postgres column Type to use for the given value
func determineColumnType(valueType ValueType) (string, error) {
	switch valueType {
	case Int, Int8, Int16, Int32, Int64:
		return "INT", nil
	case Float32, Float64:
		return "FLOAT8", nil
	case String:
		return "VARCHAR", nil
	case Bool:
		return "BOOLEAN", nil
	case "time.Time":
		return "TIMESTAMPTZ", nil
	case NilType:
		return "VARCHAR", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func newPostgresOfflineTable(conn *pgxpool.Pool, name string, valueType ValueType) (*postgresOfflineTable, error) {
	columnType, err := determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMPTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
	_, err = conn.Exec(context.Background(), tableCreateQry)
	if err != nil {
		return nil, err
	}
	return &postgresOfflineTable{
		conn: conn,
		name: name,
	}, nil
}

func (table *postgresOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := sanitize(table.name)
	if err := rec.check(); err != nil {
		return err
	}
	upsertQuery := fmt.Sprintf(""+
		"INSERT INTO %s (entity, value, ts) "+
		"VALUES ($1, $2, $3) "+
		"ON CONFLICT (entity, ts)"+
		"DO UPDATE SET value=$2 WHERE excluded.entity=$1 AND excluded.ts=$3", tb)
	if _, err := table.conn.Exec(context.Background(), upsertQuery, rec.Entity, rec.Value, rec.TS); err != nil {
		return err
	}

	return nil
}

func newPostgresPrimaryTable(conn *pgxpool.Pool, name string, schema TableSchema) (*postgresPrimaryTable, error) {
	query, err := createPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}
	_, err = conn.Exec(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return &postgresPrimaryTable{
		conn:   conn,
		name:   name,
		schema: schema,
	}, nil
}

// createPrimaryTableQuery creates a query for table creation based on the
// specified TableSchema. Returns the query if successful. Returns an error
// if there is an invalid column type.
func createPrimaryTableQuery(name string, schema TableSchema) (string, error) {
	columns := make([]string, 0)
	for _, column := range schema.Columns {
		columnType, err := determineColumnType(column.ValueType)
		if err != nil {
			return "", err
		}
		columns = append(columns, fmt.Sprintf("%s %s", column.Name, columnType))
	}
	columnString := strings.Join(columns, ", ")
	return fmt.Sprintf("CREATE TABLE %s ( %s )", sanitize(name), columnString), nil
}

func (table *postgresPrimaryTable) Write(rec GenericRecord) error {
	tb := sanitize(table.name)
	columns := table.getColumnNameString()
	placeholder := table.createValuePlaceholderString()
	upsertQuery := fmt.Sprintf(""+
		"INSERT INTO %s ( %s ) "+
		"VALUES ( %s ) ", tb, columns, placeholder)
	if _, err := table.conn.Exec(context.Background(), upsertQuery, rec...); err != nil {
		return err
	}

	return nil
}

func (table *postgresPrimaryTable) GetName() string {
	return table.name
}
func (table *postgresPrimaryTable) createValuePlaceholderString() string {
	placeholders := make([]string, 0)
	for i := range table.schema.Columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (table *postgresPrimaryTable) getColumnNameString() string {
	columns := make([]string, 0)
	for _, column := range table.schema.Columns {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns, ", ")
}

type postgresMaterialization struct {
	id        MaterializationID
	conn      *pgxpool.Pool
	tableName string
}

func (mat *postgresMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *postgresMaterialization) NumRows() (int64, error) {
	n := int64(0)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitize(mat.tableName))
	rows := mat.conn.QueryRow(context.Background(), query)
	err := rows.Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (mat *postgresMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := fmt.Sprintf(""+
		"SELECT entity, value, ts::timestamptz FROM "+
		"( SELECT * FROM "+
		"( SELECT *, row_number() over() FROM %s )t1 WHERE row_number>$1 AND row_number<=$2)t2", sanitize(mat.tableName))
	rows, err := mat.conn.Query(context.Background(), query, start, end)
	if err != nil {
		return nil, err
	}
	colType, err := mat.getValueColumnType()
	if err != nil {
		return nil, err
	}
	return newPostgresFeatureIterator(rows, colType), nil
}

// getValueColumnType gets the column type for the value of a resource.
// Used to cast the value to the proper type after it is queried
func (mat *postgresMaterialization) getValueColumnType() (postgresColumnType, error) {
	var name, colType string
	err := mat.conn.QueryRow(context.Background(),
		"select column_name, data_type from information_schema.columns where table_name = $1 AND column_name = 'value'",
		mat.tableName).Scan(&name, &colType)
	if err != nil || colType == "" {
		return "", err
	}
	t := postgresColumnType(colType)
	return t, nil
}

type postgresFeatureIterator struct {
	rows         db.Rows
	err          error
	currentValue ResourceRecord
	columnType   postgresColumnType
}

func newPostgresFeatureIterator(rows db.Rows, columnType postgresColumnType) FeatureIterator {
	return &postgresFeatureIterator{
		rows:         rows,
		err:          nil,
		currentValue: ResourceRecord{},
		columnType:   columnType,
	}
}

func (iter *postgresFeatureIterator) Next() bool {
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
	rec.Value = castPostgresTableItemType(value, iter.columnType)
	rec.TS = ts.UTC()
	iter.currentValue = rec
	return true
}

func (iter *postgresFeatureIterator) Value() ResourceRecord {
	return iter.currentValue
}

func (iter *postgresFeatureIterator) Err() error {
	return nil
}

// castTableItemType returns the value casted as its original type
func castPostgresTableItemType(v interface{}, t postgresColumnType) interface{} {
	if v == nil {
		return v
	}
	switch t {
	case PGInt:
		return int(v.(int32))
	case "bigint":
		return int(v.(int64))
	case PGFloat:
		return v.(float64)
	case PGString:
		return v.(string)
	case PGBool:
		return v.(bool)
	default:
		return v
	}
}

func (store *postgresOfflineStore) CreateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	splitQuery := strings.Split(config.Query, " ")
	if strings.ToUpper(splitQuery[0]) != "SELECT" {
		return fmt.Errorf("query invalid. must start with SELECT: %s", config.Query)
	}
	var query string
	// Let this fail if the table exists?
	// Need a way to determine what columns are which if its a resource table
	if config.TargetTableID.Type == Primary {
		query = fmt.Sprintf("CREATE TABLE %s AS %s ", sanitize(name), config.Query)
	} else if config.TargetTableID.Type == Feature || config.TargetTableID.Type == Label {
		columnMap, err := mapColumns(config.ColumnMapping, config.Query)
		if err != nil {
			return err
		}
		constraintName := uuid.NewString()
		query = fmt.Sprintf("CREATE TABLE %s AS %s ; ALTER TABLE %s ADD CONSTRAINT  %s  UNIQUE (entity, ts)", sanitize(name), columnMap, sanitize(name), sanitize(constraintName))
		fmt.Println(query)
	}

	if _, err := store.conn.Exec(context.Background(), query); err != nil {
		return err
	}
	return nil
}

func (store *postgresOfflineStore) createTransformationName(id ResourceID) (string, error) {
	switch id.Type {
	case Label, Feature:
		return store.getResourceTableName(id), nil
	case Primary:
		return store.getPrimaryTableName(id), nil
	case TrainingSet:
		return "", TransformationTypeError{"Invalid Transformation Type"}
	default:
		return "", TransformationTypeError{"Invalid Transformation Type"}
	}
}

func mapColumns(columns []ColumnMapping, query string) (string, error) {
	if len(columns) != 3 {
		return "", errors.New(fmt.Sprintf("was expecting 3 columns from ColumnMapping, received %d", len(columns)))
	}
	var entity, value, ts string
	for _, column := range columns {
		if column.resourceColumn == "entity" {
			entity = column.sourceColumn
		} else if column.resourceColumn == "value" {
			value = column.sourceColumn
		} else if column.resourceColumn == "ts" {
			ts = column.sourceColumn
		}
	}
	if entity == "" {
		return "", errors.New("missing entity column")
	}
	if value == "" {
		return "", errors.New("missing entity column")
	}
	if ts == "" {
		return "", errors.New("missing entity column")
	}
	return fmt.Sprintf("( SELECT %s as entity, %s as value, %s as ts FROM ( %s )t  )", entity, value, ts, query), nil
}

type SQLError struct {
	error string
}

func (e SQLError) Error() string {
	return e.error
}

type TransformationTypeError struct {
	error string
}

func (e TransformationTypeError) Error() string {
	return e.error
}

func (store *postgresOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	name := store.getPrimaryTableName(id)
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return &postgresPrimaryTable{
		conn: store.conn,
		name: name, // Add columns
	}, nil
}

func (pt *postgresPrimaryTable) IterateSegment(start, end int64) (GenericTableIterator, error) {
	rows, err := pt.conn.Query(
		context.Background(),
		"SELECT column_name FROM information_schema.columns WHERE table_name = $1 order by ordinal_position",
		pt.name)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, sanitize(column))
	}
	columns := strings.Join(columnNames[:], ", ")
	trainingSetQry := fmt.Sprintf("SELECT %s FROM %s", columns, sanitize(pt.name))
	rows, err = pt.conn.Query(context.Background(), trainingSetQry)
	if err != nil {
		return nil, err
	}
	colTypes, err := pt.getValueColumnTypes(pt.name)
	if err != nil {
		return nil, err
	}
	return newPostgresGenericTableIterator(rows, colTypes, columnNames), nil
}

func (pt *postgresPrimaryTable) NumRows() (int64, error) {
	n := int64(0)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitize(pt.name))
	rows := pt.conn.QueryRow(context.Background(), query)
	err := rows.Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

type postgresGenericTableIterator struct {
	rows          db.Rows
	currentValues GenericRecord
	err           error
	columnTypes   []postgresColumnType
	columnNames   []string
}

func newPostgresGenericTableIterator(rows db.Rows, columnTypes []postgresColumnType, columnNames []string) GenericTableIterator {
	return &postgresGenericTableIterator{
		rows:          rows,
		currentValues: nil,
		err:           nil,
		columnTypes:   columnTypes,
		columnNames:   columnNames,
	}
}

func (it *postgresGenericTableIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	values, err := it.rows.Values()
	if err != nil {
		it.rows.Close()
		it.err = err
		return false
	}
	currentValues := make([]interface{}, len(values))
	for i, value := range values {
		currentValues[i] = castPostgresTableItemType(value, it.columnTypes[i])
	}
	it.currentValues = currentValues
	return true
}

func (it *postgresGenericTableIterator) Values() GenericRecord {
	return it.currentValues
}

func (it *postgresGenericTableIterator) Columns() []string {
	return it.columnNames
}

func (it *postgresGenericTableIterator) Err() error {
	return it.err
}

func (pt *postgresPrimaryTable) getValueColumnTypes(table string) ([]postgresColumnType, error) {
	rows, err := pt.conn.Query(context.Background(),
		"select data_type from (select column_name, data_type from information_schema.columns where table_name = $1 order by ordinal_position) t",
		table)
	if err != nil {
		return nil, err
	}
	colTypes := make([]postgresColumnType, 0)
	for rows.Next() {
		if types, err := rows.Values(); err != nil {
			return nil, err
		} else {
			for _, t := range types {
				colTypes = append(colTypes, postgresColumnType(t.(string)))
			}
		}
	}
	return colTypes, nil
}
