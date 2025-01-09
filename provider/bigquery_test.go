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
	pl "github.com/featureform/provider/location"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
)

type bigQueryOfflineStoreTester struct {
	*bqOfflineStore
}

func (bq *bigQueryOfflineStoreTester) CreateDatabase(name string) error {
	metadata := &bigquery.DatasetMetadata{
		Name: name,
	}

	bq.query.setTablePrefix(bq.client.Project() + "." + name)
	return bq.client.Dataset(name).Create(context.TODO(), metadata)
}

func (bq *bigQueryOfflineStoreTester) DropDatabase(name string) error {
	bq.query.setTablePrefix("")
	return bq.client.Dataset(name).DeleteWithContents(context.TODO())
}

func (bq *bigQueryOfflineStoreTester) CreateSchema(database, schema string) error {
	// Intentional no-op. There's really nothing analogous to a schema in
	// BigQuery, so we ignore it.
	return nil
}

func (bq *bigQueryOfflineStoreTester) CreateTable(loc pl.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	var tableSchema bigquery.Schema

	for _, col := range schema.Columns {
		columnType, err := bq.query.determineNativeColumnType(col.ValueType)
		if err != nil {
			return nil, err
		}
		tableSchema = append(tableSchema, &bigquery.FieldSchema{
			Name:     col.Name,
			Type:     columnType,
			Required: false,
		})
	}

	var datasetName = sqlLocation.GetDatabase()
	var tableName = sqlLocation.GetTable()

	metadata := &bigquery.TableMetadata{
		Name:   tableName,
		Schema: tableSchema,
	}

	table := bq.client.Dataset(datasetName).Table(tableName)
	if err = table.Create(context.TODO(), metadata); err != nil {
		return nil, err
	}

	return &bqPrimaryTable{
		client: bq.client,
		name:   tableName,
		query:  bq.query,
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
		DatasetId:   os.Getenv("BIGQUERY_DATASET_ID"),
		Credentials: credentialsDict,
	}

	return bigQueryConfig, nil
}

func TestOfflineStoreBigQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	bigQueryConfig, err := getBigQueryConfig(t)
	if err != nil {
		t.Fatal(err)
	}

	serialBQConfig := bigQueryConfig.Serialize()

	if err := createBigQueryDataset(bigQueryConfig); err != nil {
		t.Fatalf("Cannot create BigQuery Dataset: %v", err)
	}

	t.Cleanup(func() {
		err := destroyBigQueryDataset(bigQueryConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.BigQueryOffline, serialBQConfig)
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	test.RunSQL()
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

func TestBigQueryTransformations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTransformationOnPrimaryDatasetTest": RegisterTransformationOnPrimaryDatasetTest,
		"RegisterChainedTransformationsTest":         RegisterChainedTransformationsTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestBigQueryMaterializations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterMaterializationNoTimestampTest":            RegisterMaterializationNoTimestampTest,
		"RegisterMaterializationWithDefaultTargetLagTest":   RegisterMaterializationWithDefaultTargetLagTest,
		"RegisterMaterializationTimestampTest":              RegisterMaterializationTimestampTest,
		"RegisterMaterializationWithDifferentWarehouseTest": RegisterMaterializationWithDifferentWarehouseTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestBigQueryTrainingSets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, false)

	tsDatasetTypes := []trainingSetDatasetType{
		tsDatasetFeaturesLabelTS,
		tsDatasetFeaturesTSLabelNoTS,
		tsDatasetFeaturesNoTSLabelTS,
		tsDatasetFeaturesLabelNoTS,
	}

	for _, testCase := range tsDatasetTypes {
		constName := string(testCase)
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			RegisterTrainingSet(t, tester, constTestCase)
		})
	}
}

func TestBigQuerySchemas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, true)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTableInDifferentDatabaseTest": RegisterTableInDifferentDatabaseTest,
		//"RegisterTableInSameDatabaseDifferentSchemaTest": RegisterTableInSameDatabaseDifferentSchemaTest,
		//"RegisterTwoTablesInSameSchemaTest":              RegisterTwoTablesInSameSchemaTest,
		//"CrossDatabaseJoinTest": CrossDatabaseJoinTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func getConfiguredBigQueryTester(t *testing.T, useCrossDBJoins bool) offlineSqlTest {
	bigQueryConfig, err := getBigQueryConfig(t)
	if err != nil {
		t.Fatalf("could not get BigQuery config: %s", err)
	}

	store, err := GetOfflineStore(pt.BigQueryOffline, bigQueryConfig.Serialize())

	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &bigQueryOfflineStoreTester{store.(*bqOfflineStore)}

	return offlineSqlTest{
		storeTester:      offlineStoreTester,
		testCrossDbJoins: useCrossDBJoins,
	}
}
