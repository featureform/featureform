// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"

	types "github.com/featureform/fftypes"
	"github.com/featureform/helpers"
	"github.com/featureform/provider/clickhouse"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

type clickHouseOfflineStoreTester struct {
	defaultDbName string
	conn          *sql.DB
	*clickHouseOfflineStore
}

func (ch *clickHouseOfflineStoreTester) GetTestDatabase() string {
	return ch.defaultDbName
}

func (ch *clickHouseOfflineStoreTester) CreateDatabase(name string) error {
	return createOrReplaceClickHouseDatabase(ch.conn, name)
}

func (ch *clickHouseOfflineStoreTester) DropDatabase(name string) error {
	_, err := ch.db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", SanitizeClickHouseIdentifier(name)))
	if err != nil {
		ch.logger.Errorw("dropping database", "error", err)
		return err
	}
	return nil
}

func (ch *clickHouseOfflineStoreTester) CreateSchema(database, schema string) error {
	// ClickHouse doesn't have a concept like schemas.
	return nil
}

type WritableClickHouseDataset struct {
	*dataset.SqlDataset
	db *sql.DB
}

func (w WritableClickHouseDataset) WriteBatch(ctx context.Context, rows []types.Row) error {
	if len(rows) == 0 {
		return nil
	}

	schema := w.Schema()
	columns := schema.ColumnNames()
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col
	}

	sqlLocation, ok := w.Location().(*pl.SQLLocation)
	if !ok {
		return fmt.Errorf("invalid location type")
	}

	// Build base query
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		sanitizeClickHouseTableName(sqlLocation.TableLocation()),
		strings.Join(columnNames, ", "))

	valueRows := make([]string, len(rows))
	var args []any

	for i, row := range rows {
		valueItems := make([]string, len(columns))

		for j, value := range row {
			// Handle specific ClickHouse value formatting
			switch v := value.Value.(type) {
			case string:
				// Special function handling for ClickHouse
				if strings.HasPrefix(v, "to") || strings.HasPrefix(v, "[") {
					valueItems[j] = v // Direct value insertion for functions
				} else {
					valueItems[j] = "?" // Use parameter for regular strings
					args = append(args, v)
				}
			case bool:
				valueItems[j] = "?"
				args = append(args, v)
			case nil:
				valueItems[j] = "NULL" // Direct NULL insertion
			default:
				valueItems[j] = "?"
				args = append(args, v)
			}
		}

		valueRows[i] = "(" + strings.Join(valueItems, ", ") + ")"
	}

	query += strings.Join(valueRows, ", ")

	// Execute the query
	_, err = w.db.ExecContext(ctx, query, args...)
	return err
}

func (ch *clickHouseOfflineStoreTester) CreateWritableDataset(loc pl.Location, schema types.Schema) (dataset.WriteableDataset, error) {
	sqlLocation, ok := loc.(*pl.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	db, err := ch.sqlOfflineStore.getDb(sqlLocation.GetDatabase(), "")
	if err != nil {
		return nil, err
	}

	ds, err := ch.CreateTableFromSchema(loc, schema)
	if err != nil {
		return nil, err
	}

	return WritableClickHouseDataset{
		SqlDataset: ds,
		db:         db,
	}, nil
}

func (ch *clickHouseOfflineStoreTester) CreateTableFromSchema(loc pl.Location, schema types.Schema) (*dataset.SqlDataset, error) {
	logger := ch.logger.With("location", loc, "schema", schema)

	sqlLocation, ok := loc.(*pl.SQLLocation)
	sqlLocation.SetSanitizer(sanitizeClickHouseTableName)

	if !ok {
		errMsg := fmt.Sprintf("invalid location type, expected SQLLocation, got %T", loc)
		logger.Errorw(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	db, err := ch.sqlOfflineStore.getDb(sqlLocation.GetDatabase(), "")
	if err != nil {
		logger.Errorw("could not get db", "error", err)
		return nil, err
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", sanitizeClickHouseTableName(sqlLocation.TableLocation())))

	for i, column := range schema.Fields {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(fmt.Sprintf("%s %s", column.Name, column.NativeType))
	}
	queryBuilder.WriteString(") ENGINE=MergeTree ORDER BY ()")

	query := queryBuilder.String()
	_, err = db.Exec(query)
	if err != nil {
		logger.Errorw("error executing query", "query", query, "error", err)
		return nil, err
	}
	// create the table
	_, err = db.Exec(query)
	if err != nil {
		logger.Errorw("error creating table", "error", err)
		return nil, err
	}

	sqlDataset, err := dataset.NewSqlDataset(db, sqlLocation, schema, clickhouse.ChConverter, -1)
	if err != nil {
		return nil, err
	}

	return &sqlDataset, nil
}

func (ch *clickHouseOfflineStoreTester) CreateTable(loc pl.Location, schema TableSchema) (PrimaryTable, error) {
	logger := ch.logger.With("location", loc, "schema", schema)

	sqlLocation, ok := loc.(*pl.SQLLocation)
	sqlLocation.SetSanitizer(sanitizeClickHouseTableName)
	if !ok {
		errMsg := fmt.Sprintf("invalid location type, expected SQLLocation, got %T", loc)
		logger.Errorw(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	db, err := ch.sqlOfflineStore.getDb(sqlLocation.GetDatabase(), "")
	if err != nil {
		logger.Errorw("could not get db", "error", err)
		return nil, err
	}

	// don't need string builder here
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", sanitizeClickHouseTableName(sqlLocation.TableLocation())))

	// do strings .join after
	for i, column := range schema.Columns {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		columnType, err := ch.sqlOfflineStore.query.determineColumnType(column.ValueType)
		if err != nil {
			logger.Errorw("determining column type", "valueType", column.Type, "error", err)
			return nil, err
		}
		queryBuilder.WriteString(fmt.Sprintf("%s %s", column.Name, columnType))
	}
	queryBuilder.WriteString(") ENGINE=MergeTree ORDER BY ()")

	query := queryBuilder.String()
	_, err = db.Exec(query)
	if err != nil {
		logger.Errorw("error executing query", "query", query, "error", err)
		return nil, err
	}

	newDb, err := ch.getDb(sqlLocation.GetDatabase(), "")
	if err != nil {
		logger.Errorw("error connecting to new database", "error", err)
		return nil, err
	}

	return &clickhousePrimaryTable{
		db:     newDb,
		name:   sqlLocation.GetTable(),
		query:  ch.sqlOfflineStore.query,
		schema: schema,
	}, nil
}

func TestOfflineStoreClickHouse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	clickHouseDb := ""
	ok := true
	if clickHouseDb, ok = os.LookupEnv("CLICKHOUSE_DB"); !ok {
		clickHouseDb = fmt.Sprintf("feature_form_%d", time.Now().UnixMilli())
	}

	username, ok := os.LookupEnv("CLICKHOUSE_USER")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_USER variable")
	}
	password, ok := os.LookupEnv("CLICKHOUSE_PASSWORD")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_PASSWORD variable")
	}
	host, ok := os.LookupEnv("CLICKHOUSE_HOST")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_HOST variable")
	}
	portStr, ok := os.LookupEnv("CLICKHOUSE_PORT")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_PORT variable")
	}
	ssl := helpers.GetEnvBool("CLICKHOUSE_SSL", false)

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Failed to parse port to numeric: %v", portStr)
	}

	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     host,
		Port:     uint16(port),
		Username: username,
		Password: password,
		Database: clickHouseDb,
		SSL:      ssl,
	}

	if err := createClickHouseDatabaseFromConfig(t, clickHouseConfig); err != nil {
		t.Fatalf("%v", err)
	}

	_, err = GetOfflineStore(pt.ClickHouseOffline, clickHouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	// No longer using offline tests for ClickHouse, moved to correctness tests.
	// Remove this at some point.
	//test := OfflineStoreTest{
	//	t:     t,
	//	store: store,
	//}
	// test.Run()
	// test.RunSQL()
}

func createOrReplaceClickHouseDatabase(conn *sql.DB, dbName string) error {
	if _, err := conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", SanitizeClickHouseIdentifier(dbName))); err != nil {
		return err
	}

	if _, err := conn.Exec(fmt.Sprintf("CREATE DATABASE %s", SanitizeClickHouseIdentifier(dbName))); err != nil {
		return err
	}
	return nil
}

func createClickHouseDatabaseFromConfig(t *testing.T, c pc.ClickHouseConfig) error {
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=%t", c.Host, c.Port, c.Username, c.Password, c.SSL))
	if err != nil {
		return err
	}
	t.Cleanup(func() {
		conn.Close()
	})

	return createOrReplaceClickHouseDatabase(conn, c.Database)
}

func TestTrainingSet(t *testing.T) {
	t.Skip()
	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     "127.0.0.1",
		Port:     uint16(9000),
		Username: "default",
		Password: "",
		Database: "ff",
		SSL:      false,
	}

	store, err := NewClickHouseOfflineStore(clickHouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	tsDef := TrainingSetDef{
		ID: ResourceID{
			Name:    "ts_alice",
			Variant: "v5",
			Type:    TrainingSet,
		},
		Label: ResourceID{
			Name:    "fraudulent",
			Variant: "2024-02-16t11-11-50",
			Type:    Label,
		},
		Features: []ResourceID{
			{
				Name:    "avg_transactions",
				Variant: "2024-02-16t11-11-50",
				Type:    Feature,
			},
		},
		LagFeatures: nil,
	}
	err = store.CreateTrainingSet(tsDef)
	if err != nil {
		t.Fatalf("could not create training set: %s\n", err)
	}
	set, err := store.GetTrainingSet(tsDef.ID)
	if err != nil {
		return
	}
	for set.Next() {
		t.Logf("features %v and labels %v\n", set.Features(), set.Label())
	}
}

func TestSplit(t *testing.T) {
	t.Skip()
	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     "127.0.0.1",
		Port:     uint16(9000),
		Username: "default",
		Password: "",
		Database: "ff",
		SSL:      false,
	}

	store, err := NewClickHouseOfflineStore(clickHouseConfig.Serialize())
	if err != nil {
		fmt.Printf("could not initialize store: %s\n", err)
	}

	resourceId := ResourceID{
		Name:    "ts_alice",
		Variant: "v5",
		Type:    TrainingSet,
	}

	trainTestSplitDef := TrainTestSplitDef{
		TrainingSetName:    resourceId.Name,
		TrainingSetVariant: resourceId.Variant,
		TestSize:           .5,
		Shuffle:            true,
		RandomState:        1,
	}

	closeFunc, err := store.CreateTrainTestSplit(trainTestSplitDef)
	if err != nil {
		t.Fatalf("could not create split: %s\n", err)
	}
	defer closeFunc()
	train, test, err := store.GetTrainTestSplit(trainTestSplitDef)
	if err != nil {
		t.Fatalf("could not get split: %s\n", err)
	}

	if err != nil {
		t.Fatalf("could not get split: %s\n", err)
	}
	// loop through train
	var trainCount int
	for train.Next() {
		// print features and labels
		t.Logf("features %v and labels %v\n", train.Features(), train.Label())
		trainCount++
	}
	t.Logf("train count: %d", trainCount)

	var testCount int
	for test.Next() {
		t.Logf("features %v and labels %v\n", test.Features(), test.Label())
		testCount++
	}
	t.Logf("test count: %d", testCount)

	health, err := store.CheckHealth()
	if err != nil {
		t.Fatalf("health check failed: %s", err)
	}
	if !health {
		t.Fatalf("health check failed")
	}
}

func TestClickHouseCastTableItemType(t *testing.T) {
	q := clickhouseSQLQueries{}

	var (
		maxInt = int(^uint(0) >> 1)
	)

	testTime := time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		input    interface{}
		typeSpec interface{}
		expected interface{}
	}{
		{
			name:     "Nil input returns nil",
			input:    nil,
			typeSpec: chInt,
			expected: nil,
		},
		{
			name:     "chInt simple conversion",
			input:    42,
			typeSpec: chInt,
			expected: 42,
		},
		{
			name:     "chInt with Nullable wrapper",
			input:    42,
			typeSpec: "Nullable(" + chInt + ")",
			expected: 42,
		},
		{
			name:     "chInt32 conversion",
			input:    int32(42),
			typeSpec: chInt32,
			expected: 42,
		},
		{
			name:     "chUInt32 in range conversion",
			input:    uint32(100),
			typeSpec: chUInt32,
			expected: 100, // converted to int
		},
		{
			name:     "chUInt32 out of range returns original",
			input:    uint32(3000000000), // 3e9 > 2^31-1 (2147483647)
			typeSpec: chUInt32,
			expected: uint32(3000000000),
		},
		{
			name:     "chInt64 in range conversion",
			input:    int64(42),
			typeSpec: chInt64,
			expected: 42,
		},
		{
			name:     "chUInt64 in range conversion",
			input:    uint64(100),
			typeSpec: chUInt64,
			expected: 100, // converted to int
		},
		{
			name:     "chUInt64 out of range returns original",
			input:    uint64(maxInt) + 1,
			typeSpec: chUInt64,
			expected: uint64(maxInt) + 1,
		},
		{
			name:     "chDateTime valid time",
			input:    testTime,
			typeSpec: chDateTime,
			expected: testTime,
		},
		{
			name:     "chDateTime invalid type returns default UnixMilli",
			input:    "not a time",
			typeSpec: chDateTime,
			expected: time.UnixMilli(0).UTC(),
		},
	}

	for _, tc := range testCases {
		tc := tc // capture loop variable
		t.Run(tc.name, func(t *testing.T) {
			result := q.castTableItemType(tc.input, tc.typeSpec)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func getClickHouseConfig(t *testing.T) (pc.ClickHouseConfig, error) {
	clickHouseDb := helpers.GetEnv("CLICKHOUSE_DB", fmt.Sprintf("feature_form_%d", time.Now().UnixMilli()))
	username := helpers.MustGetTestingEnv(t, "CLICKHOUSE_USER")
	password := helpers.MustGetTestingEnv(t, "CLICKHOUSE_PASSWORD")
	host := helpers.MustGetTestingEnv(t, "CLICKHOUSE_HOST")
	port := helpers.GetEnvInt("CLICKHOUSE_PORT", 9001)
	ssl := helpers.GetEnvBool("CLICKHOUSE_SSL", false)

	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     host,
		Port:     uint16(port),
		Username: username,
		Password: password,
		Database: clickHouseDb,
		SSL:      ssl,
	}

	return clickHouseConfig, nil
}

func sanitizeClickHouseTableName(obj pl.FullyQualifiedObject) string {
	name := ""
	if obj.Database != "" {
		name = SanitizeClickHouseIdentifier(obj.Database) + "."
	}
	name += SanitizeClickHouseIdentifier(obj.Table)
	return name
}

func getConfiguredClickHouseTester(t *testing.T) OfflineSqlTest {
	clickHouseConfig, err := getClickHouseConfig(t)
	if err != nil {
		t.Fatalf("could not get clickhouse config: %s\n", err)
	}

	if err := createClickHouseDatabaseFromConfig(t, clickHouseConfig); err != nil {
		t.Fatalf("%v", err)
	}

	store, err := GetOfflineStore(pt.ClickHouseOffline, clickHouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=%t",
		clickHouseConfig.Host,
		clickHouseConfig.Port,
		clickHouseConfig.Username,
		clickHouseConfig.Password,
		clickHouseConfig.SSL,
	))
	t.Cleanup(func() {
		conn.Close()
	})

	storeTester := clickHouseOfflineStoreTester{
		conn:                   conn,
		defaultDbName:          clickHouseConfig.Database,
		clickHouseOfflineStore: store.(*clickHouseOfflineStore),
	}

	return OfflineSqlTest{
		storeTester: &storeTester,
		testConfig: OfflineSqlTestConfig{
			sanitizeTableName:        sanitizeClickHouseTableName,
			removeSchemaFromLocation: true,
		},
	}
}
