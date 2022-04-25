// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func (ps *PostgresTableSchema) Serialize() []byte {
	schema, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}
	return schema
}

func (ps *PostgresTableSchema) Deserialize(schema SerializedTableSchema) error {
	err := json.Unmarshal(schema, ps)
	if err != nil {
		return err
	}
	return nil
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

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *postgresOfflineStore) CreateResourceTable(id ResourceID, schema SerializedTableSchema) (OfflineTable, error) {
	psSchema := PostgresTableSchema{}
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
	table, err := newPostgresOfflineTable(store.conn, tableName, psSchema.ValueType)
	if err != nil {
		return nil, err
	}
	return table, nil
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

func (table *postgresOfflineTable) resourceExists(rec ResourceRecord) (bool, error) {
	rec = checkTimestamp(rec)
	query := fmt.Sprintf("SELECT entity, value, ts FROM %s WHERE entity=$1 AND ts=$2 ", sanitize(table.name))
	rows, err := table.conn.Query(context.Background(), query, rec.Entity, rec.TS)
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
	switch t {
	case PGInt:
		return int(v.(int32))
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
