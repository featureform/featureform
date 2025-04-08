// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	bqlib "cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"

	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	ffbq "github.com/featureform/provider/bigquery"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

type bigQueryOfflineStoreTester struct {
	*bqOfflineStore
}

func (bq *bigQueryOfflineStoreTester) GetTestDatabase() string {
	return bq.query.ProjectId
}

func (bq *bigQueryOfflineStoreTester) SupportsDbCreation() bool {
	return true
}

func (bq *bigQueryOfflineStoreTester) TestDbName() string {
	return bq.query.ProjectId
}

func (bq *bigQueryOfflineStoreTester) CreateDatabase(_ string) error {
	return nil
}

func (bq *bigQueryOfflineStoreTester) DropDatabase(_ string) error {
	return nil
}

func (bq *bigQueryOfflineStoreTester) CreateSchema(_, schema string) error {
	metadata := &bqlib.DatasetMetadata{
		Name: schema,
	}
	return bq.client.Dataset(schema).Create(context.TODO(), metadata)
}

func (bq *bigQueryOfflineStoreTester) CreateTable(loc pl.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	var tableSchema bqlib.Schema

	for _, col := range schema.Columns {
		columnType, err := bq.query.determineColumnType(col.ValueType)
		if err != nil {
			return nil, err
		}
		tableSchema = append(tableSchema, &bqlib.FieldSchema{
			Name:     col.Name,
			Type:     columnType,
			Required: false,
		})
	}

	var datasetName = sqlLocation.GetSchema()
	// There are a few tests that don't particularly care to create (or set)
	// schemas to use. In this case, just default to the testing dataset.
	if datasetName == "" {
		datasetName = bq.query.DatasetId
	}

	var tableName = sqlLocation.GetTable()

	dataset := bq.client.Dataset(datasetName)

	tableMetadata := &bqlib.TableMetadata{
		Name:   tableName,
		Schema: tableSchema,
	}

	table := dataset.Table(tableName)
	if err = table.Create(context.TODO(), tableMetadata); err != nil {
		return nil, err
	}

	var newQueryClient = defaultBQQueries{
		ProjectId: bq.query.ProjectId,
		DatasetId: datasetName,
		Ctx:       bq.query.Ctx,
	}

	return &bqPrimaryTable{
		client: bq.client,
		table:  table,
		name:   tableName,
		query:  newQueryClient,
		schema: schema,
	}, nil
}

type WritableBigQueryDataset struct {
	dataset.Dataset
	client *bqlib.Client
	table  *bqlib.Table
}

func (w WritableBigQueryDataset) WriteBatch(ctx context.Context, rows []types.Row) error {
	if len(rows) == 0 {
		return nil
	}

	schema := w.Schema()
	columnNames := schema.ColumnNames()

	// Create inserter
	inserter := w.table.Inserter()

	// Create BigQuery rows using mapSaver which already implements ValueSaver
	var saveValues []*mapSaver
	for _, row := range rows {
		// Create a map of column name to value
		item := make(map[string]interface{})

		for i, col := range columnNames {
			if i < len(row) {
				item[col] = row[i].Value
			}
		}

		// Add the row to our batch using mapSaver
		saveValues = append(saveValues, &mapSaver{record: item})
	}

	// Insert the rows
	return inserter.Put(ctx, saveValues)
}

func (bq *bigQueryOfflineStoreTester) CreateWritableDataset(loc pl.Location, schema types.Schema) (dataset.WriteableDataset, error) {
	sqlLocation, ok := loc.(*pl.SQLLocation)
	sqlLocation.SetSanitizer(SanitizeBigQueryTableName)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	// Ensure we have a client
	if bq.client == nil {
		return nil, fmt.Errorf("BigQuery client not initialized")
	}

	// Create the table if it doesn't exist
	ds, err := bq.CreateTableFromSchema(loc, schema)
	if err != nil {
		return nil, err
	}

	// Get the BigQuery dataset and table
	dataset := bq.client.Dataset(sqlLocation.GetSchema())
	table := dataset.Table(sqlLocation.GetTable())

	return WritableBigQueryDataset{
		Dataset: ds,
		client:  bq.client,
		table:   table,
	}, nil
}

func (bq *bigQueryOfflineStoreTester) CreateTableFromSchema(loc pl.Location, schema types.Schema) (dataset.Dataset, error) {
	logger := bq.logger.With("location", loc, "schema", schema)

	sqlLocation, ok := loc.(*pl.SQLLocation)
	if !ok {
		errMsg := fmt.Sprintf("invalid location type, expected SQLLocation, got %T", loc)
		logger.Errorw(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	// Build BigQuery schema
	var bqSchema bqlib.Schema
	for _, field := range schema.Fields {
		fieldType, err := ffbq.GetBigQueryType(field.Type)
		if err != nil {
			logger.Errorw("determining column type", "valueType", field.Type, "error", err)
			return nil, err
		}

		bqSchema = append(bqSchema, &bqlib.FieldSchema{
			Name:     string(field.Name),
			Type:     fieldType,
			Required: false, // All fields are nullable by default
		})
	}

	// Create the table
	datasetRef := bq.client.Dataset(sqlLocation.GetSchema())
	tableRef := datasetRef.Table(sqlLocation.GetTable())

	tableMetadata := &bqlib.TableMetadata{
		Schema: bqSchema,
	}

	if err := tableRef.Create(bq.query.Ctx, tableMetadata); err != nil {
		// If the table already exists, that's fine
		if !strings.Contains(err.Error(), "already exists") {
			logger.Errorw("error creating table", "error", err)
			return nil, err
		}
	}

	// Create the SQL dataset
	bqDataset, err := ffbq.NewDataset(bq.client, sqlLocation, schema, ffbq.BqConverter, -1)
	if err != nil {
		return nil, err
	}

	return bqDataset, nil
}

func getBigQueryConfig(t *testing.T) (pc.BigQueryConfig, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	credentials, ok := os.LookupEnv("BIGQUERY_CREDENTIALS")
	if !ok {
		t.Fatalf("missing BIGQUERY_CREDENTIALS variable")
	}
	projectID, ok := os.LookupEnv("BIGQUERY_PROJECT_ID")
	if !ok {
		t.Fatalf("missing BIGQUERY_PROJECT_ID variable")
	}

	JSONCredentials, err := ioutil.ReadFile(credentials)
	if err != nil {
		panic(fmt.Errorf("cannot find big query credentials: %v", err))
	}

	var credentialsDict map[string]interface{}
	err = json.Unmarshal(JSONCredentials, &credentialsDict)
	if err != nil {
		panic(fmt.Errorf("cannot unmarshal big query credentials: %v", err))
	}

	bigQueryDatasetId := strings.Replace(strings.ToUpper(uuid.NewString()), "-", "_", -1)
	os.Setenv("BIGQUERY_DATASET_ID", bigQueryDatasetId)
	t.Log("BigQuery Dataset: ", bigQueryDatasetId)

	var bigQueryConfig = pc.BigQueryConfig{
		ProjectId:   projectID,
		DatasetId:   bigQueryDatasetId,
		Credentials: credentialsDict,
	}

	return bigQueryConfig, nil
}

func createBigQueryDataset(c pc.BigQueryConfig) error {
	sCreds, err := json.Marshal(c.Credentials)
	if err != nil {
		return err
	}

	client, err := bqlib.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	meta := &bqlib.DatasetMetadata{
		Location:               "US",
		DefaultTableExpiration: 24 * time.Hour,
	}
	err = client.Dataset(c.DatasetId).Create(context.TODO(), meta)

	return err
}

func destroyBigQueryDataset(c pc.BigQueryConfig) error {
	sCreds, err := json.Marshal(c.Credentials)
	if err != nil {
		return err
	}

	time.Sleep(10 * time.Second)

	client, err := bqlib.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Dataset(c.DatasetId).DeleteWithContents(context.TODO())

	return err
}

func SanitizeBigQueryTableName(obj pl.FullyQualifiedObject) string {
	return "`" + obj.String() + "`"
}

func getConfiguredBigQueryTester(t *testing.T) OfflineSqlTest {
	logger := logging.NewTestLogger(t)

	bigQueryConfig, err := getBigQueryConfig(t)
	if err != nil {
		t.Fatalf("could not get BigQuery config: %s", err)
	}

	if err := createBigQueryDataset(bigQueryConfig); err != nil {
		t.Fatalf("Cannot create BigQuery Dataset: %v", err)
	}

	t.Cleanup(func() {
		err := destroyBigQueryDataset(bigQueryConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.BigQueryOffline, bigQueryConfig.Serialize())

	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &bigQueryOfflineStoreTester{store.(*bqOfflineStore)}
	offlineStoreTester.logger = logger

	sanitizeTableNameFunc := func(obj pl.FullyQualifiedObject) string {
		// BigQuery requires a fully qualified location of a source table.
		if obj.Database == "" || obj.Schema == "" {
			datasetId := store.(*bqOfflineStore).query.DatasetId
			srcLoc := pl.NewSQLLocationFromParts(offlineStoreTester.GetTestDatabase(), datasetId, obj.Table)
			return "`" + srcLoc.TableLocation().String() + "`"
		}
		return "`" + obj.String() + "`"
	}

	return OfflineSqlTest{
		storeTester: offlineStoreTester,
		testConfig: OfflineSqlTestConfig{
			sanitizeTableName: sanitizeTableNameFunc,
		},
	}
}
