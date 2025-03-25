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

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"

	"github.com/featureform/logging"
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

func (bq *bigQueryOfflineStoreTester) CreateDatabase(_ string) offlineSqlStoreCreateDb {
	return nil
}

func (bq *bigQueryOfflineStoreTester) DropDatabase(_ string) error {
	return nil
}

func (bq *bigQueryOfflineStoreTester) CreateSchema(_, schema string) error {
	metadata := &bigquery.DatasetMetadata{
		Name: schema,
	}
	return bq.client.Dataset(schema).Create(context.TODO(), metadata)
}

func (bq *bigQueryOfflineStoreTester) CreateTable(loc pl.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	var tableSchema bigquery.Schema

	for _, col := range schema.Columns {
		columnType, err := bq.query.determineColumnType(col.ValueType)
		if err != nil {
			return nil, err
		}
		tableSchema = append(tableSchema, &bigquery.FieldSchema{
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

	tableMetadata := &bigquery.TableMetadata{
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

	client, err := bigquery.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
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

	client, err := bigquery.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Dataset(c.DatasetId).DeleteWithContents(context.TODO())

	return err
}

func getConfiguredBigQueryTester(t *testing.T) offlineSqlTest {
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

	return offlineSqlTest{
		storeTester: offlineStoreTester,
		testConfig: offlineSqlTestConfig{
			sanitizeTableName: sanitizeTableNameFunc,
		},
	}
}
