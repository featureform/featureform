package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	_ "github.com/lib/pq"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	bqInt       = "INTEGER"
	bqBigInt    = "BIGINT"
	bqFloat     = "DECIMAL"
	bqString    = "STRING"
	bqBool      = "BOOL"
	bqTimestamp = "TIMESTAMP"
)

const (
	SleepTime = 6 * time.Second
)

type BigQueryConfig struct {
	ProjectId   string
	DatasetId   string
	Credentials []byte
}

func (bq *BigQueryConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, bq)
	if err != nil {
		return err
	}
	return nil
}

func (bq *BigQueryConfig) Serialize() []byte {
	conf, err := json.Marshal(bq)
	if err != nil {
		panic(err)
	}
	return conf
}

type BQOfflineStoreConfig struct {
	Config       SerializedConfig
	ProjectId    string
	ProviderType Type
	QueryImpl    BQOfflineTableQueries
}

type BQOfflineTableQueries interface {
	tableExists(tableName string) string
	viewExists(viewName string) string
	determineColumnType(valueType ValueType) (string, error)
	primaryTableCreate(name string, columnString string) string
	upsertQuery(tb string, columns string, placeholder string) string
	createValuePlaceholderString(columns []TableColumn) string
	registerResources(client *bigquery.Client, tableName string, schema ResourceSchema, timestamp bool) error
	writeUpdate(table string) string
	writeInserts(table string) string
	writeExists(table string) string
	newBQOfflineTable(name string, columnType string) string
	materializationCreate(tableName string, resultName string) string
	materializationIterateSegment(tableName string, start int64, end int64) string
	getNumRowsQuery(tableName string) string
	getTablePrefix() string
	setTablePrefix(prefix string)
	getContext() context.Context
	setContext()
	castTableItemType(v interface{}, t interface{}) interface{}
	materializationExists(tableName string) string
	materializationDrop(tableName string) string
	materializationUpdate(client *bigquery.Client, tableName string, sourceName string) error
	monitorJob(job *bigquery.Job) error
	transformationCreate(name string, query string) string
	getColumns(client *bigquery.Client, name string) ([]TableColumn, error)
	transformationUpdate(client *bigquery.Client, tableName string, query string) error
	trainingSetCreate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingSetUpdate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingSetQuery(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error
	atomicUpdate(client *bigquery.Client, tableName string, tempName string, query string) error
	trainingRowSelect(columns string, trainingSetName string) string
	primaryTableRegister(tableName string, sourceName string) string
}

type defaultBQQueries struct {
	TablePrefix string
	Ctx         context.Context
}

type bqGenericTableIterator struct {
	iter         *bigquery.RowIterator
	currentValue GenericRecord
	err          error
	query        BQOfflineTableQueries
}

type bqPrimaryTable struct {
	client *bigquery.Client
	name   string
	query  BQOfflineTableQueries
	schema TableSchema
}

func (pt *bqPrimaryTable) GetName() string {
	return pt.name
}

func (pt *bqPrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	tableName := fmt.Sprintf("%s.%s", pt.query.getTablePrefix(), pt.name)
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d", tableName, n)
	bqQ := pt.client.Query(query)
	it, err := bqQ.Read(pt.query.getContext())

	if err != nil {
		return nil, err
	}
	return newBigQueryGenericTableIterator(it, pt.query), nil
}

func (pt *bqPrimaryTable) NumRows() (int64, error) {
	var n []bigquery.Value
	tableName := fmt.Sprintf("%s.%s", pt.query.getTablePrefix(), pt.name)
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

	bqQ := pt.client.Query(query)

	it, err := bqQ.Read(pt.query.getContext())
	if err != nil {
		return 0, err
	}

	err = it.Next(&n)
	if err != nil {
		return 0, err
	}

	return n[0].(int64), nil
}

func (pt *bqPrimaryTable) Write(rec GenericRecord) error {
	tb := pt.name
	recordsParameter, columns, columnsString := pt.getNonNullRecords(rec)

	placeholder := pt.query.createValuePlaceholderString(columns)
	upsertQuery := pt.query.upsertQuery(tb, columnsString, placeholder)

	bqQ := pt.client.Query(upsertQuery)
	bqQ.Parameters = recordsParameter

	_, err := bqQ.Read(pt.query.getContext())

	return err
}

func (pt *bqPrimaryTable) getNonNullRecords(rec GenericRecord) ([]bigquery.QueryParameter, []TableColumn, string) {
	recordsParameter := make([]bigquery.QueryParameter, 0)
	recordColumns := make([]TableColumn, 0)
	selectedColumns := make([]string, 0)
	for i, r := range rec {
		if r == nil {
			continue
		}
		recordsParameter = append(recordsParameter, bigquery.QueryParameter{Value: r})
		recordColumns = append(recordColumns, pt.schema.Columns[i])
		selectedColumns = append(selectedColumns, pt.schema.Columns[i].Name)
	}

	columns := strings.Join(selectedColumns, ",")

	return recordsParameter, recordColumns, columns
}

func (it *bqGenericTableIterator) Next() bool {
	var rowValues []bigquery.Value
	err := it.iter.Next(&rowValues)
	if err == iterator.Done {
		it.err = nil
		return false
	} else if err != nil {
		it.err = err
		return false
	}

	genRows := make(GenericRecord, len(rowValues))
	for i, value := range rowValues {
		if value == nil {
			continue
		}

		colType := it.iter.Schema[i].Type
		genRows[i] = it.query.castTableItemType(value, colType)
	}
	it.currentValue = genRows
	return true
}

func (it *bqGenericTableIterator) Values() GenericRecord {
	return it.currentValue
}

func (it *bqGenericTableIterator) Columns() []string {
	var columns []string
	for _, col := range it.iter.Schema {
		columns = append(columns, col.Name)
	}
	return columns
}

func (it *bqGenericTableIterator) Err() error {
	return it.err
}

func newBigQueryGenericTableIterator(it *bigquery.RowIterator, query BQOfflineTableQueries) GenericTableIterator {
	return &bqGenericTableIterator{
		iter:         it,
		currentValue: nil,
		err:          nil,
		query:        query,
	}
}

func (q defaultBQQueries) registerResources(client *bigquery.Client, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW `%s.%s` AS SELECT %s as entity, %s as value, %s as ts FROM `%s.%s`", q.getTablePrefix(), tableName,
			schema.Entity, schema.Value, schema.TS, q.getTablePrefix(), schema.SourceTable)
	} else {
		query = fmt.Sprintf("CREATE VIEW `%s.%s` AS SELECT %s as entity, %s as value, to_timestamp('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP as ts FROM `%s.%s`", q.getTablePrefix(), tableName,
			schema.Entity, schema.Value, time.UnixMilli(0).UTC(), q.getTablePrefix(), schema.SourceTable)
	}

	bqQ := client.Query(query)
	_, err := bqQ.Read(q.getContext())
	return err
}

func (q defaultBQQueries) writeUpdate(table string) string {
	return fmt.Sprintf("UPDATE `%s.%s` SET value=? WHERE entity=? AND ts=? ", q.getTablePrefix(), table)
}
func (q defaultBQQueries) writeInserts(table string) string {
	return fmt.Sprintf("INSERT INTO `%s.%s` (entity, value, ts, insert_ts) VALUES (?, ?, ?, CURRENT_TIMESTAMP())", q.getTablePrefix(), table)
}

func (q defaultBQQueries) writeExists(table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s` WHERE entity=\"%s\" AND ts=timestamp(\"%s\")", q.getTablePrefix(), table, "?", "?")
}

func (q defaultBQQueries) tableExists(tableName string) string {
	return fmt.Sprintf("SELECT COUNT(*) AS total FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='BASE TABLE' AND table_name='%s'", q.getTablePrefix(), tableName)
}

func (q defaultBQQueries) viewExists(viewName string) string {
	return fmt.Sprintf("SELECT COUNT(*) AS total FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='VIEW' AND table_name='%s'", q.getTablePrefix(), viewName)
}

func (q defaultBQQueries) determineColumnType(valueType ValueType) (string, error) {
	switch valueType {
	case Int:
		return "INTEGER", nil
	case Int32, Int64:
		return "BIGINT", nil
	case Float32, Float64:
		return "FLOAT64", nil
	case String:
		return "STRING", nil
	case Bool:
		return "BOOLEAN", nil
	case Timestamp:
		return "TIMESTAMP", nil
	case NilType:
		return "STRING", nil
	default:
		return "", fmt.Errorf("cannot find column type for value type: %s", valueType)
	}
}

func (q defaultBQQueries) primaryTableCreate(name string, columnString string) string {
	query := fmt.Sprintf("CREATE TABLE `%s.%s` ( %s )", q.getTablePrefix(), name, columnString)
	return query
}

func (q defaultBQQueries) upsertQuery(tb string, columns string, placeholder string) string {
	return fmt.Sprintf("INSERT INTO `%s.%s` ( %s ) VALUES ( %s )", q.getTablePrefix(), tb, columns, placeholder)
}

func (q defaultBQQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for _ = range columns {
		placeholders = append(placeholders, fmt.Sprintf("?"))
	}
	return strings.Join(placeholders, ", ")
}

func (q defaultBQQueries) newBQOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE `%s.%s` (entity STRING, value %s, ts TIMESTAMP, insert_ts TIMESTAMP)", q.getTablePrefix(), name, columnType)
}

func (q defaultBQQueries) materializationCreate(tableName string, resultName string) string {
	query := fmt.Sprintf(
		"CREATE TABLE `%s.%s` AS (SELECT entity, value, ts, row_number() over(ORDER BY entity) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts DESC, insert_ts DESC) "+
			"AS rn FROM `%s.%s`) t WHERE rn=1)", q.getTablePrefix(), tableName, q.getTablePrefix(), resultName)

	return query
}

func (q defaultBQQueries) materializationIterateSegment(tableName string, start int64, end int64) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM `%s.%s` WHERE row_number > %v AND row_number <= %v)", q.getTablePrefix(), tableName, start, end)
}

func (q defaultBQQueries) getNumRowsQuery(tableName string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s`", q.getTablePrefix(), tableName)
}

func (q *defaultBQQueries) getTablePrefix() string {
	return q.TablePrefix
}

func (q *defaultBQQueries) setTablePrefix(prefix string) {
	q.TablePrefix = prefix
}

func (q *defaultBQQueries) setContext() {
	q.Ctx = context.Background()
}

func (q *defaultBQQueries) getContext() context.Context {
	return q.Ctx
}

func (q defaultBQQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}

	t = fmt.Sprintf("%s", t)
	switch t {
	case bqInt:
		return int(v.(int64))
	case bqBigInt:
		return int64(v.(int64))
	case bqFloat:
		return v.(float64)
	case bqString:
		v := v.(string)
		return v
	case bqBool:
		return v.(bool)
	case bqTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q defaultBQQueries) materializationExists(tableName string) string {
	return fmt.Sprintf("SELECT DISTINCT(table_name) FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='BASE TABLE' AND table_name='%s'", q.getTablePrefix(), tableName)
}

func (q defaultBQQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE `%s.%s`", q.getTablePrefix(), tableName)
}

func (q defaultBQQueries) materializationUpdate(client *bigquery.Client, tableName string, sourceName string) error {
	sanitizedTable := tableName
	tempTable := fmt.Sprintf("tmp_%s", tableName)
	oldTable := fmt.Sprintf("old_%s", tableName)

	materializationCreateQuery := fmt.Sprintf("CREATE TABLE `%s.%s` AS (SELECT entity, value, ts, row_number() over(ORDER BY (entity)) as row_number FROM "+
		"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts DESC, insert_ts DESC) "+
		"AS rn FROM `%s.%s`) t WHERE rn=1);", q.getTablePrefix(), tempTable, q.getTablePrefix(), sourceName)

	alterTables := fmt.Sprintf(
		"ALTER TABLE `%s.%s` RENAME TO `%s`;"+
			"ALTER TABLE `%s.%s` RENAME TO `%s`;", q.getTablePrefix(), sanitizedTable, oldTable, q.getTablePrefix(), tempTable, sanitizedTable)

	dropTable := fmt.Sprintf("DROP TABLE `%s.%s`;", q.getTablePrefix(), oldTable)

	query := fmt.Sprintf("%s %s %s", materializationCreateQuery, alterTables, dropTable)

	bqQ := client.Query(query)
	job, err := bqQ.Run(q.getContext())
	if err != nil {
		return err
	}

	err = q.monitorJob(job)
	return err
}

func (q defaultBQQueries) monitorJob(job *bigquery.Job) error {
	for {
		time.Sleep(SleepTime)
		status, err := job.Status(q.getContext())
		if err != nil {
			return err
		} else if status.Err() != nil {
			return fmt.Errorf("%s", status.Err())
		}

		switch status.State {
		case bigquery.Done:
			return nil
		default:
			continue
		}
	}
}

func (q defaultBQQueries) transformationCreate(name string, query string) string {
	qry := fmt.Sprintf("CREATE TABLE `%s.%s` AS %s", q.getTablePrefix(), name, query)
	return qry
}

func (q defaultBQQueries) getColumns(client *bigquery.Client, name string) ([]TableColumn, error) {
	qry := fmt.Sprintf("SELECT column_name FROM `%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name=\"%s\" ORDER BY ordinal_position", q.getTablePrefix(), name)

	bqQ := client.Query(qry)
	it, err := bqQ.Read(q.getContext())
	if err != nil {
		return nil, err
	}

	columnNames := make([]TableColumn, 0)
	for {
		var column []bigquery.Value
		err := it.Next(&column)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		columnNames = append(columnNames, TableColumn{Name: column[0].(string)})
	}

	return columnNames, nil
}

func (q defaultBQQueries) transformationUpdate(client *bigquery.Client, tableName string, query string) error {
	tempName := fmt.Sprintf("tmp_%s", tableName)
	fullQuery := fmt.Sprintf("CREATE TABLE `%s.%s` AS %s", q.getTablePrefix(), tempName, query)

	err := q.atomicUpdate(client, tableName, tempName, fullQuery)
	if err != nil {
		return err
	}
	return nil
}

func (q defaultBQQueries) atomicUpdate(client *bigquery.Client, tableName string, tempName string, query string) error {
	sanitizedTable := tableName
	oldTable := fmt.Sprintf("old_%s", tableName)
	updateQuery := fmt.Sprintf(
		"%s;"+
			"ALTER TABLE `%s.%s` RENAME TO `%s`;"+
			"ALTER TABLE `%s.%s` RENAME TO `%s`;"+
			"DROP TABLE `%s.%s`;"+
			"", query, q.getTablePrefix(), sanitizedTable, oldTable, q.getTablePrefix(), tempName, sanitizedTable, q.getTablePrefix(), oldTable)

	bdQ := client.Query(updateQuery)
	job, err := bdQ.Run(q.getContext())
	if err != nil {
		return err
	}

	err = q.monitorJob(job)
	return err
}

func (q defaultBQQueries) trainingSetCreate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q defaultBQQueries) trainingSetUpdate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q defaultBQQueries) trainingSetQuery(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	columns := make([]string, 0)
	selectColumns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		santizedName := strings.Replace(tableName, "-", "_", -1)
		if err != nil {
			return err
		}
		tableJoinAlias := fmt.Sprintf("t%d", i+1)
		selectColumns = append(selectColumns, fmt.Sprintf("%s_rnk", tableJoinAlias))
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value AS `%s`, ts, RANK() OVER (ORDER BY ts DESC, insert_ts DESC) AS %s_rnk FROM `%s.%s` ORDER BY ts desc) AS %s ON (%s.entity=t0.entity AND %s.ts <= t0.ts)",
			query, santizedName, tableJoinAlias, q.getTablePrefix(), tableName, tableJoinAlias, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )) WHERE rn=1", query)
		}
	}
	columnStr := strings.Join(columns, ", ")
	selectColumnStr := strings.Join(selectColumns, ", ")

	if !isUpdate {
		fullQuery := fmt.Sprintf(
			"CREATE TABLE `%s.%s` AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY \"time\", %s DESC) AS rn FROM ( "+
				"SELECT t0.entity AS e, t0.value AS label, t0.ts AS time, %s, %s FROM `%s.%s` AS t0 %s )",
			q.getTablePrefix(), tableName, columnStr, selectColumnStr, columnStr, selectColumnStr, q.getTablePrefix(), labelName, query)

		bqQ := store.client.Query(fullQuery)
		job, err := bqQ.Run(store.query.getContext())
		if err != nil {
			return err
		}

		err = store.query.monitorJob(job)
		return err
	} else {
		tempTable := fmt.Sprintf("tmp_%s", tableName)
		fullQuery := fmt.Sprintf(
			"CREATE TABLE `%s.%s` AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY \"time\", %s desc) AS rn FROM ( "+
				"SELECT t0.entity AS e, t0.value AS label, t0.ts AS time, %s, %s FROM `%s.%s` AS t0 %s )",
			q.getTablePrefix(), tempTable, columnStr, selectColumnStr, columnStr, selectColumnStr, q.getTablePrefix(), labelName, query)
		err := q.atomicUpdate(store.client, tableName, tempTable, fullQuery)
		return err
	}
}

func (q defaultBQQueries) trainingRowSelect(columns string, trainingSetName string) string {
	return fmt.Sprintf("SELECT %s FROM `%s.%s`", columns, q.getTablePrefix(), trainingSetName)
}

func (q defaultBQQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW `%s.%s` AS SELECT * FROM `%s.%s`", q.getTablePrefix(), tableName, q.getTablePrefix(), sourceName)
}

type bqMaterialization struct {
	id        MaterializationID
	client    *bigquery.Client
	tableName string
	query     BQOfflineTableQueries
}

func (mat *bqMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *bqMaterialization) NumRows() (int64, error) {
	var n []bigquery.Value
	query := mat.query.getNumRowsQuery(mat.tableName)

	bqQ := mat.client.Query(query)
	it, err := bqQ.Read(mat.query.getContext())
	if err != nil {
		return 0, err
	}

	err = it.Next(&n)
	if err != nil {
		return 0, err
	}
	if n == nil {
		return 0, nil
	}
	return n[0].(int64), nil

}

func (mat *bqMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName, start, end)

	bqQ := mat.client.Query(query)
	it, err := bqQ.Read(mat.query.getContext())
	if err != nil {
		return nil, err
	}
	return newbqFeatureIterator(it, mat.query), nil
}

type bqFeatureIterator struct {
	iter         *bigquery.RowIterator
	currentValue ResourceRecord
	err          error
	query        BQOfflineTableQueries
}

func newbqFeatureIterator(it *bigquery.RowIterator, query BQOfflineTableQueries) FeatureIterator {
	return &bqFeatureIterator{
		iter:         it,
		err:          nil,
		currentValue: ResourceRecord{},
		query:        query,
	}
}

func (it *bqFeatureIterator) Next() bool {
	var rowValue []bigquery.Value
	err := it.iter.Next(&rowValue)
	if err == iterator.Done {
		it.err = nil
		return false
	} else if err != nil {
		it.err = err
		return false
	}

	var currValue ResourceRecord
	valueColType := it.iter.Schema[1].Type
	currValue.Entity = rowValue[0].(string)
	currValue.Value = it.query.castTableItemType(rowValue[1], valueColType)
	currValue.TS = rowValue[2].(time.Time)

	it.currentValue = currValue
	return true
}

func (it *bqFeatureIterator) Value() ResourceRecord {
	return it.currentValue
}

func (it *bqFeatureIterator) Err() error {
	return it.err
}

type bqOfflineTable struct {
	client *bigquery.Client
	query  BQOfflineTableQueries
	name   string
}

func (table *bqOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := table.name
	if err := rec.check(); err != nil {
		return err
	}

	var n []bigquery.Value
	existsQuery := table.query.writeExists(tb)

	bqQ := table.client.Query(existsQuery)
	bqQ.Parameters = []bigquery.QueryParameter{
		{
			Value: rec.Entity,
		},
		{
			Value: rec.TS,
		},
	}

	iter, err := bqQ.Read(table.query.getContext())
	if err != nil {
		return err
	}

	err = iter.Next(&n)
	if err != nil {
		return err
	}

	if n == nil {
		return fmt.Errorf("Cannot find %s table", tb)
	}

	var writeQuery string
	var params []bigquery.QueryParameter
	if n[0].(int64) == 0 {
		writeQuery = table.query.writeInserts(tb)
		params = []bigquery.QueryParameter{bigquery.QueryParameter{Value: rec.Entity}, bigquery.QueryParameter{Value: rec.Value}, bigquery.QueryParameter{Value: rec.TS}}
	} else if n[0].(int64) > 0 {
		writeQuery = table.query.writeUpdate(tb)
		params = []bigquery.QueryParameter{bigquery.QueryParameter{Value: rec.Value}, bigquery.QueryParameter{Value: rec.Entity}, bigquery.QueryParameter{Value: rec.TS}}
	}

	bqQ = table.client.Query(writeQuery)
	bqQ.Parameters = params

	_, err = bqQ.Read(table.query.getContext())

	return err
}

type bqOfflineStore struct {
	client *bigquery.Client
	parent BQOfflineStoreConfig
	query  BQOfflineTableQueries
	BaseProvider
}

func NewBQOfflineStore(config BQOfflineStoreConfig) (*bqOfflineStore, error) {
	ctx := context.Background()

	sc := BigQueryConfig{}
	if err := sc.Deserialize(config.Config); err != nil {
		return nil, errors.New("invalid bigquery config")
	}

	client, err := bigquery.NewClient(ctx, config.ProjectId, option.WithCredentialsJSON(sc.Credentials))
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return &bqOfflineStore{
		client: client,
		parent: config,
		query:  config.QueryImpl,
		BaseProvider: BaseProvider{
			ProviderType:   config.ProviderType,
			ProviderConfig: config.Config,
		},
	}, nil
}

func bigQueryOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := BigQueryConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid bigquery config")
	}
	queries := defaultBQQueries{}
	queries.setTablePrefix(fmt.Sprintf("%s.%s", sc.ProjectId, sc.DatasetId))
	queries.setContext()
	sgConfig := BQOfflineStoreConfig{
		Config:       config,
		ProjectId:    sc.ProjectId,
		ProviderType: BigQueryOffline,
		QueryImpl:    &queries,
	}

	store, err := NewBQOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (store *bqOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
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
		if err := store.query.registerResources(store.client, tableName, schema, false); err != nil {
			return nil, fmt.Errorf("register no ts: %w", err)
		}
	} else {
		if err := store.query.registerResources(store.client, tableName, schema, true); err != nil {
			return nil, fmt.Errorf("register ts: %w", err)
		}
	}

	return &bqOfflineTable{
		client: store.client,
		name:   tableName,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
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

	bqQ := store.client.Query(query)
	job, err := bqQ.Run(store.query.getContext())
	if err != nil {
		return nil, err
	}

	err = store.query.monitorJob(job)
	if err != nil {
		return nil, err
	}

	columnNames, err := store.query.getColumns(store.client, tableName)
	return &bqPrimaryTable{
		client: store.client,
		name:   tableName,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) CreateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	query := store.query.transformationCreate(name, config.Query)

	bqQ := store.client.Query(query)
	job, err := bqQ.Run(store.query.getContext())
	if err != nil {
		return err
	}

	err = store.query.monitorJob(job)
	return err
}

func (store *bqOfflineStore) createTransformationName(id ResourceID) (string, error) {
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

func (store *bqOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}

	existsQuery := store.query.tableExists(name)
	bqQ := store.client.Query(existsQuery)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, err
	}

	var row []bigquery.Value
	err = it.Next(&row)

	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, fmt.Errorf("transformation not found: %v", name)
	}

	columnNames, err := store.query.getColumns(store.client, name)
	if err != nil {
		return nil, err
	}

	return &bqPrimaryTable{
		client: store.client,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) UpdateTransformation(config TransformationConfig) error {
	name, err := store.createTransformationName(config.TargetTableID)
	if err != nil {
		return err
	}
	err = store.query.transformationUpdate(store.client, name, config.Query)
	if err != nil {
		return err
	}

	return nil
}

func (store *bqOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
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
	table, err := store.newBigQueryPrimaryTable(store.client, tableName, schema)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *bqOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	columnNames, err := store.query.getColumns(store.client, name)
	if err != nil {
		return nil, err
	}

	return &bqPrimaryTable{
		client: store.client,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}

	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	var valueType ValueType
	if valueIndex := store.getValueIndex(schema.Columns); valueIndex > 0 {
		valueType = schema.Columns[valueIndex].ValueType
	} else {
		valueType = NilType
	}
	table, err := store.newbqOfflineTable(store.client, tableName, valueType)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (store *bqOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *bqOfflineStore) newbqOfflineTable(client *bigquery.Client, name string, valueType ValueType) (*bqOfflineTable, error) {
	columnType, err := store.query.determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := store.query.newBQOfflineTable(name, columnType)
	bqQ := client.Query(tableCreateQry)
	_, err = bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, err
	}
	return &bqOfflineTable{
		client: client,
		name:   name,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getbqResourceTable(id)
}

func (store *bqOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	resTable, err := store.getbqResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(id.Name)
	matTableName := store.getMaterializationTableName(matID)
	materializeQry := store.query.materializationCreate(matTableName, resTable.name)

	bqQ := store.client.Query(materializeQry)
	_, err = bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, err
	}
	return &bqMaterialization{
		id:        matID,
		client:    store.client,
		tableName: matTableName,
		query:     store.query,
	}, nil
}

func (store *bqOfflineStore) getbqResourceTable(id ResourceID) (*bqOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}

	table, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	return &bqOfflineTable{
		client: store.client,
		name:   table,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) getMaterializationTableName(id MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", id)
}

func (store *bqOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := store.query.materializationExists(tableName)

	bqQry := store.client.Query(getMatQry)
	it, err := bqQry.Read(store.query.getContext())
	if err != nil {
		return nil, fmt.Errorf("could not get materialization: %w", err)
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		return nil, fmt.Errorf("could not get materialization: %w", err)
	}

	if len(row) == 0 {
		return nil, &MaterializationNotFound{id}
	}
	return &bqMaterialization{
		id:        id,
		client:    store.client,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *bqOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	matID := MaterializationID(id.Name)
	tableName := store.getMaterializationTableName(matID)
	getMatQry := store.query.materializationExists(tableName)
	resTable, err := store.getbqResourceTable(id)
	if err != nil {
		return nil, err
	}

	bqQ := store.client.Query(getMatQry)
	it, err := bqQ.Read(store.query.getContext())
	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		return nil, fmt.Errorf("could not get materialization: %w", err)
	}
	if len(row) == 0 {
		return nil, &MaterializationNotFound{matID}
	}

	err = store.query.materializationUpdate(store.client, tableName, resTable.name)
	if err != nil {
		return nil, err
	}

	return &bqMaterialization{
		id:        matID,
		client:    store.client,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *bqOfflineStore) DeleteMaterialization(id MaterializationID) error {
	tableName := store.getMaterializationTableName(id)
	if exists, err := store.materializationExists(id); err != nil {
		return err
	} else if !exists {
		return &MaterializationNotFound{id}
	}
	query := store.query.materializationDrop(tableName)
	bqQ := store.client.Query(query)
	_, err := bqQ.Read(store.query.getContext())

	return err
}

func (store *bqOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	tableName := store.getMaterializationTableName(id)
	getMatQry := store.query.materializationExists(tableName)

	bqQ := store.client.Query(getMatQry)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return false, err
	}

	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		return false, nil
	} else {
		return true, nil
	}
}

func (store *bqOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getbqResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}

	err = store.query.trainingSetCreate(store, def, tableName, label.name)
	return err
}

func (store *bqOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getbqResourceTable(def.Label)
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

func (store *bqOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
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
	columnNames, err := store.query.getColumns(store.client, trainingSetName)
	if err != nil {
		return nil, err
	}
	features := make([]string, 0)
	for _, name := range columnNames {
		features = append(features, name.Name)
	}
	columns := strings.Join(features[:], ", ")
	trainingSetQry := store.query.trainingRowSelect(columns, trainingSetName)

	fmt.Printf("Training Set Query: %s\n", trainingSetQry)
	bqQ := store.client.Query(trainingSetQry)
	iter, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, err
	}

	return store.newbqTrainingSetIterator(iter), nil
}

type bqTrainingRowsIterator struct {
	iter            *bigquery.RowIterator
	currentFeatures []interface{}
	currentLabel    interface{}
	err             error
	isHeaderRow     bool
	query           BQOfflineTableQueries
}

func (store *bqOfflineStore) newbqTrainingSetIterator(iter *bigquery.RowIterator) TrainingSetIterator {
	return &bqTrainingRowsIterator{
		iter:            iter,
		currentFeatures: nil,
		currentLabel:    nil,
		err:             nil,
		isHeaderRow:     true,
		query:           store.query,
	}
}

func (it *bqTrainingRowsIterator) Next() bool {
	var rowValues []bigquery.Value
	err := it.iter.Next(&rowValues)
	if err == iterator.Done {
		it.err = nil
		return false
	} else if err != nil {
		it.err = err
		return false
	}

	var label interface{}
	numFeatures := len(it.iter.Schema) - 1
	featureVals := make([]interface{}, numFeatures)
	for i, value := range rowValues {
		if value == nil {
			continue
		}
		colType := it.iter.Schema[i].Type
		if i < numFeatures {
			featureVals[i] = it.query.castTableItemType(value, colType)
		} else {
			label = it.query.castTableItemType(value, colType)
		}
	}
	it.currentFeatures = featureVals
	it.currentLabel = label

	return true
}

func (it *bqTrainingRowsIterator) Err() error {
	return it.err
}

func (it *bqTrainingRowsIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *bqTrainingRowsIterator) Label() interface{} {
	return it.currentLabel
}

func (store *bqOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *bqOfflineStore) tableExists(id ResourceID) (bool, error) {
	var n []bigquery.Value
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
		return false, err
	}

	query := store.query.tableExists(tableName)
	bqQ := store.client.Query(query)

	iter, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return false, err
	}

	err = iter.Next(&n)
	if n != nil && n[0].(int64) > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, err
	}

	query = store.query.viewExists(tableName)
	bqQ = store.client.Query(query)

	iter, err = bqQ.Read(store.query.getContext())
	if err != nil {
		return false, err
	}

	err = iter.Next(&n)
	if n != nil && n[0].(int64) > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, nil
}

func (store *bqOfflineStore) newBigQueryPrimaryTable(client *bigquery.Client, name string, schema TableSchema) (*bqPrimaryTable, error) {
	query, err := store.createBigQueryPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}

	qry := client.Query(query)
	_, err = qry.Read(store.query.getContext())
	if err != nil {
		return nil, err
	}
	return &bqPrimaryTable{
		client: client,
		name:   name,
		schema: schema,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) createBigQueryPrimaryTableQuery(name string, schema TableSchema) (string, error) {
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

func (store *bqOfflineStore) getResourceTableName(id ResourceID) (string, error) {
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

func (store *bqOfflineStore) getTrainingSetName(id ResourceID) (string, error) {
	if err := checkName(id); err != nil {
		return "", err
	}
	return fmt.Sprintf("featureform_trainingset__%s__%s", id.Name, id.Variant), nil
}
