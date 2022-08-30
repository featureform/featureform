//go:build offline
// +build offline

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
)

var provider = flag.String("provider", "all", "provider to perform test on")

func TestOfflineStores(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}

	os.Setenv("TZ", "UTC")

	checkEnv := func(envVar string) string {
		value, has := os.LookupEnv(envVar)
		if !has {
			panic(fmt.Sprintf("Environment variable not found: %s", envVar))
		}
		return value
	}

	postgresInit := func() SerializedConfig {
		db := checkEnv("POSTGRES_DB")
		user := checkEnv("POSTGRES_USER")
		password := checkEnv("POSTGRES_PASSWORD")
		var postgresConfig = PostgresConfig{
			Host:     "localhost",
			Port:     "5432",
			Database: db,
			Username: user,
			Password: password,
		}
		return postgresConfig.Serialize()
	}

	snowflakeInit := func() (SerializedConfig, SnowflakeConfig) {
		snowFlakeDatabase := strings.ToUpper(uuid.NewString())
		t.Log("Snowflake Database: ", snowFlakeDatabase)
		username := checkEnv("SNOWFLAKE_USERNAME")
		password := checkEnv("SNOWFLAKE_PASSWORD")
		org := checkEnv("SNOWFLAKE_ORG")
		account := checkEnv("SNOWFLAKE_ACCOUNT")
		var snowflakeConfig = SnowflakeConfig{
			Username:     username,
			Password:     password,
			Organization: org,
			Account:      account,
			Database:     snowFlakeDatabase,
		}
		if err := createSnowflakeDatabase(snowflakeConfig); err != nil {
			t.Fatalf("%v", err)
		}
		return snowflakeConfig.Serialize(), snowflakeConfig
	}

	redshiftInit := func() (SerializedConfig, RedshiftConfig) {
		redshiftDatabase := fmt.Sprintf("ff%s", strings.ToLower(uuid.NewString()))
		endpoint := checkEnv("REDSHIFT_ENDPOINT")
		port := checkEnv("REDSHIFT_PORT")
		username := checkEnv("REDSHIFT_USERNAME")
		password := checkEnv("REDSHIFT_PASSWORD")
		var redshiftConfig = RedshiftConfig{
			Endpoint: endpoint,
			Port:     port,
			Database: redshiftDatabase,
			Username: username,
			Password: password,
		}
		serialRSConfig := redshiftConfig.Serialize()
		if err := createRedshiftDatabase(redshiftConfig); err != nil {
			t.Fatalf("%v", err)
		}
		return serialRSConfig, redshiftConfig
	}

	bqInit := func() (SerializedConfig, BigQueryConfig) {
		bigqueryCredentials := os.Getenv("BIGQUERY_CREDENTIALS")
		JSONCredentials, err := ioutil.ReadFile(bigqueryCredentials)
		if err != nil {
			panic(fmt.Errorf("cannot find big query credentials: %v", err))
		}

		bigQueryDatasetId := strings.Replace(strings.ToUpper(uuid.NewString()), "-", "_", -1)
		os.Setenv("BIGQUERY_DATASET_ID", bigQueryDatasetId)
		t.Log("BigQuery Dataset: ", bigQueryDatasetId)

		var bigQueryConfig = BigQueryConfig{
			ProjectId:   os.Getenv("BIGQUERY_PROJECT_ID"),
			DatasetId:   os.Getenv("BIGQUERY_DATASET_ID"),
			Credentials: JSONCredentials,
		}
		serialBQConfig := bigQueryConfig.Serialize()

		if err := createBigQueryDataset(bigQueryConfig); err != nil {
			t.Fatalf("Cannot create BigQuery Dataset: %v", err)
		}
		return serialBQConfig, bigQueryConfig
	}

	type testMember struct {
		t               Type
		c               SerializedConfig
		integrationTest bool
	}
	testList := []testMember{}

	if *provider == "memory" || *provider == "" {
		testList = append(testList, testMember{MemoryOffline, []byte{}, false})
	}
	if *provider == "postgres" || *provider == "" {
		testList = append(testList, testMember{PostgresOffline, postgresInit(), true})
	}
	if *provider == "snowflake" || *provider == "" {
		serialSFConfig, snowflakeConfig := snowflakeInit()
		defer destroySnowflakeDatabase(snowflakeConfig)
		testList = append(testList, testMember{SnowflakeOffline, serialSFConfig, true})
	}
	if *provider == "redshift" || *provider == "" {
		serialRSConfig, redshiftConfig := redshiftInit()
		defer destroyRedshiftDatabase(redshiftConfig)
		testList = append(testList, testMember{RedshiftOffline, serialRSConfig, true})
	}
	if *provider == "bigquery" || *provider == "" {
		serialBQConfig, bigQueryConfig := bqInit()
		defer destroyBigQueryDataset(bigQueryConfig)
		testList = append(testList, testMember{BigQueryOffline, serialBQConfig, true})
	}

	testFns := map[string]func(*testing.T, OfflineStore){
		"CreateGetTable":          testCreateGetOfflineTable,
		"TableAlreadyExists":      testOfflineTableAlreadyExists,
		"TableNotFound":           testOfflineTableNotFound,
		"InvalidResourceIDs":      testInvalidResourceIDs,
		"Materializations":        testMaterializations,
		"MaterializationUpdate":   testMaterializationUpdate,
		"InvalidResourceRecord":   testWriteInvalidResourceRecord,
		"InvalidMaterialization":  testInvalidMaterialization,
		"MaterializeUnknown":      testMaterializeUnknown,
		"MaterializationNotFound": testMaterializationNotFound,
		"TrainingSets":            testTrainingSet,
		"TrainingSetUpdate":       testTrainingSetUpdate,
		"TrainingSetInvalidID":    testGetTrainingSetInvalidResourceID,
		"GetUnknownTrainingSet":   testGetUnkonwnTrainingSet,
		"InvalidTrainingSetDefs":  testInvalidTrainingSetDefs,
		"LabelTableNotFound":      testLabelTableNotFound,
		"FeatureTableNotFound":    testFeatureTableNotFound,
		"TrainingDefShorthand":    testTrainingSetDefShorthand,
	}
	testSQLFns := map[string]func(*testing.T, OfflineStore){
		"PrimaryTableCreate":           testPrimaryCreateTable,
		"PrimaryTableWrite":            testPrimaryTableWrite,
		"Transformation":               testTransform,
		"TransformationUpdate":         testTransformUpdate,
		"CreateDuplicatePrimaryTable":  testCreateDuplicatePrimaryTable,
		"ChainTransformations":         testChainTransform,
		"CreateResourceFromSource":     testCreateResourceFromSource,
		"CreateResourceFromSourceNoTS": testCreateResourceFromSourceNoTS,
		"CreatePrimaryFromSource":      testCreatePrimaryFromSource,
	}

	psqlInfo := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), "localhost", "5432", os.Getenv("POSTGRES_DB"))
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	for _, testItem := range testList {
		if testing.Short() && testItem.integrationTest {
			t.Logf("Skipping %s, because it is an integration test", testItem.t)
			continue
		}
		var connections_start, connections_end int
		if testItem.t == PostgresOffline {
			err = db.QueryRow("SELECT Count(*) FROM pg_stat_activity WHERE pid!=pg_backend_pid()").Scan(&connections_start)
			if err != nil {
				panic(err)
			}
		}
		for name, fn := range testFns {
			provider, err := Get(testItem.t, testItem.c)
			if err != nil {
				t.Fatalf("Failed to get provider %s: %s", testItem.t, err)
			}
			store, err := provider.AsOfflineStore()
			if err != nil {
				t.Fatalf("Failed to use provider %s as OfflineStore: %s", testItem.t, err)
			}
			testName := fmt.Sprintf("%s_%s", testItem.t, name)
			t.Run(testName, func(t *testing.T) {
				fn(t, store)
			})
			if err := store.Close(); err != nil {
				t.Errorf("%v:%v - %v\n", testItem.t, name, err)
			}
		}
		for name, fn := range testSQLFns {
			if testItem.t == MemoryOffline {
				continue
			}
			provider, err := Get(testItem.t, testItem.c)
			if err != nil {
				t.Fatalf("Failed to get provider %s: %s", testItem.t, err)
			}
			store, err := provider.AsOfflineStore()
			if err != nil {
				t.Logf("Cannot use provider %s as SQLOfflineStore: %s", testItem.t, err)
				continue
			}
			testName := fmt.Sprintf("%s_%s", testItem.t, name)
			t.Run(testName, func(t *testing.T) {
				fn(t, store)
			})
			if err := store.Close(); err != nil {
				t.Errorf("%v:%v - %v\n", testItem.t, name, err)
			}
		}
		if testItem.t == PostgresOffline {
			err = db.QueryRow("SELECT Count(*) FROM pg_stat_activity WHERE pid!=pg_backend_pid()").Scan(&connections_end)
			if err != nil {
				panic(err)
			}
			t.Run("POSTGRES_ConnectionCheck", func(t *testing.T) {
				if connections_start+2 <= connections_end {
					t.Errorf("Started with %d connections, ended with %d connections", connections_start, connections_end)
				}
			})
		}
	}
}

func createRedshiftDatabase(c RedshiftConfig) error {
	url := fmt.Sprintf("sslmode=require user=%v password=%s host=%v port=%v dbname=%v", c.Username, c.Password, c.Endpoint, c.Port, "dev")
	db, err := sql.Open("postgres", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("CREATE DATABASE %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	fmt.Printf("Created Redshift Database %s\n", c.Database)
	return nil
}

func destroyRedshiftDatabase(c RedshiftConfig) error {
	url := fmt.Sprintf("sslmode=require user=%v password=%s host=%v port=%v dbname=%v", c.Username, c.Password, c.Endpoint, c.Port, "dev")
	db, err := sql.Open("postgres", url)
	if err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	disconnectQuery := fmt.Sprintf("SELECT pg_terminate_backend(pg_stat_activity.procpid) FROM pg_stat_activity WHERE datid=(SELECT oid from pg_database where datname = '%s');", c.Database)
	if _, err := db.Exec(disconnectQuery); err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	var deleteErr error
	retries := 5
	databaseQuery := fmt.Sprintf("DROP DATABASE %s", sanitize(c.Database))
	for {
		if _, err := db.Exec(databaseQuery); err != nil {
			deleteErr = err
			time.Sleep(time.Second)
			retries--
			if retries == 0 {
				fmt.Errorf(err.Error())
				return deleteErr
			}
		} else {
			continue
		}
	}

	return nil
}

func createSnowflakeDatabase(c SnowflakeConfig) error {
	url := fmt.Sprintf("%s:%s@%s-%s", c.Username, c.Password, c.Organization, c.Account)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	return nil
}

func destroySnowflakeDatabase(c SnowflakeConfig) error {
	url := fmt.Sprintf("%s:%s@%s-%s", c.Username, c.Password, c.Organization, c.Account)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	databaseQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	return nil
}

func createBigQueryDataset(c BigQueryConfig) error {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, c.ProjectId, option.WithCredentialsJSON(c.Credentials))
	if err != nil {
		return err
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location:               "US",
		DefaultTableExpiration: 24 * time.Hour,
	}
	err = client.Dataset(c.DatasetId).Create(ctx, meta)

	return err
}

func destroyBigQueryDataset(c BigQueryConfig) error {
	ctx := context.Background()

	time.Sleep(10 * time.Second)

	client, err := bigquery.NewClient(ctx, c.ProjectId, option.WithCredentialsJSON(c.Credentials))
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Dataset(c.DatasetId).DeleteWithContents(ctx)

	return err
}

func randomID(types ...OfflineResourceType) ResourceID {
	var t OfflineResourceType
	if len(types) == 0 {
		t = NoType
	} else if len(types) == 1 {
		t = types[0]
	} else {
		t = types[rand.Intn(len(types))]
	}
	return ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    t,
	}
}

func randomFeatureID() ResourceID {
	return ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Feature,
	}
}

func randomLabelID() ResourceID {
	return ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Label,
	}
}

func testCreateGetOfflineTable(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	if tab, err := store.CreateResourceTable(id, schema); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if tab, err := store.GetResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func testOfflineTableAlreadyExists(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "", ValueType: String},
			{Name: "", ValueType: Int},
			{Name: "", ValueType: Timestamp},
		},
	}
	if _, err := store.CreateResourceTable(id, schema); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateResourceTable(id, schema); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*TableAlreadyExists); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func testOfflineTableNotFound(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	if _, err := store.GetResourceTable(id); err == nil {
		t.Fatalf("Succeeded in getting non-existant table")
	} else if casted, valid := err.(*TableNotFound); !valid {
		t.Fatalf("Wrong error for table not found: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

func testMaterializations(t *testing.T, store OfflineStore) {
	type TestCase struct {
		WriteRecords             []ResourceRecord
		Schema                   TableSchema
		ExpectedRows             int64
		SegmentStart, SegmentEnd int64
		ExpectedSegment          []ResourceRecord
	}

	schemaInt := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	tests := map[string]TestCase{
		"Empty": {
			WriteRecords:    []ResourceRecord{},
			Schema:          schemaInt,
			SegmentStart:    0,
			SegmentEnd:      0,
			ExpectedSegment: []ResourceRecord{},
		},
		"NoOverlap": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
			},
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			// Have to expect time.UnixMilli(0).UTC() as it is the default value
			// if a resource does not have a set timestamp
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
		},
		"SubSegmentNoOverlap": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
			},
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 1,
			SegmentEnd:   2,
			ExpectedSegment: []ResourceRecord{
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
			},
		},
		"SimpleOverwrite": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
				{Entity: "a", Value: 4},
			},
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 4, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
		},
		// Added .UTC() b/c DeepEqual checks the timezone field of time.Time which can vary, resulting in false failures
		// during tests even if time is correct
		"SimpleChanges": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
			},
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
		},
		"OutOfOrderWrites": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
				{Entity: "c", Value: 9, TS: time.UnixMilli(5).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
			},
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
			},
		},
		"OutOfOrderOverwrites": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
				{Entity: "c", Value: 9, TS: time.UnixMilli(5).UTC()},
				{Entity: "b", Value: 12, TS: time.UnixMilli(2).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3).UTC()},
			},
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
			},
		},
	}
	testMaterialization := func(t *testing.T, mat Materialization, test TestCase) {
		if numRows, err := mat.NumRows(); err != nil {
			t.Fatalf("Failed to get num rows: %s", err)
		} else if numRows != test.ExpectedRows {
			t.Fatalf("Num rows not equal %d %d", numRows, test.ExpectedRows)
		}
		seg, err := mat.IterateSegment(test.SegmentStart, test.SegmentEnd)
		if err != nil {
			t.Fatalf("Failed to create segment: %s", err)
		}
		i := 0
		expectedRows := test.ExpectedSegment
		for seg.Next() {
			actual := seg.Value()

			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(actual, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1

					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					expectedRows[i], expectedRows[lastIdx] = expectedRows[lastIdx], expectedRows[i]
					expectedRows = expectedRows[:lastIdx]
					break
				}
			}

			if !found {
				t.Fatalf("Value %v not found in materialization %v", actual, expectedRows)
			}
			i++
		}
		if err := seg.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d. Expected: %d", i, len(test.ExpectedSegment))
		}
		if err := seg.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
	}
	runTestCase := func(t *testing.T, test TestCase) {
		id := randomID(Feature)
		table, err := store.CreateResourceTable(id, test.Schema)

		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}

		for _, rec := range test.WriteRecords {
			if err := table.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v: %s", rec, err)
			}
		}
		mat, err := store.CreateMaterialization(id)
		if err != nil {
			t.Fatalf("Failed to create materialization: %s", err)
		}

		testMaterialization(t, mat, test)
		getMat, err := store.GetMaterialization(mat.ID())

		if err != nil {
			t.Fatalf("Failed to get materialization: %s", err)
		}
		testMaterialization(t, getMat, test)
		if err := store.DeleteMaterialization(mat.ID()); err != nil {
			t.Fatalf("Failed to delete materialization: %s", err)
		}
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}

}

func testMaterializationUpdate(t *testing.T, store OfflineStore) {
	type TestCase struct {
		WriteRecords                           []ResourceRecord
		UpdateRecords                          []ResourceRecord
		Schema                                 TableSchema
		ExpectedRows                           int64
		UpdatedRows                            int64
		SegmentStart, SegmentEnd               int64
		UpdatedSegmentStart, UpdatedSegmentEnd int64
		ExpectedSegment                        []ResourceRecord
		ExpectedUpdate                         []ResourceRecord
	}

	schemaInt := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	tests := map[string]TestCase{
		"Empty": {
			WriteRecords:    []ResourceRecord{},
			UpdateRecords:   []ResourceRecord{},
			Schema:          schemaInt,
			SegmentStart:    0,
			SegmentEnd:      0,
			UpdatedRows:     0,
			ExpectedSegment: []ResourceRecord{},
			ExpectedUpdate:  []ResourceRecord{},
		},
		"NoOverlap": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
			},
			UpdateRecords: []ResourceRecord{
				{Entity: "d", Value: 4},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   4,
			UpdatedRows:         4,
			// Have to expect time.UnixMilli(0).UTC() as it is the default value
			// if a resource does not have a set timestamp
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
			ExpectedUpdate: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
				{Entity: "d", Value: 4, TS: time.UnixMilli(0).UTC()},
			},
		},
		"SimpleOverwrite": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
				{Entity: "a", Value: 4},
			},
			UpdateRecords: []ResourceRecord{
				{Entity: "a", Value: 3},
				{Entity: "b", Value: 4},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 4, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
			ExpectedUpdate: []ResourceRecord{
				{Entity: "a", Value: 3, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 4, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
		},
		// Added .UTC() b/c DeepEqual checks the timezone field of time.Time which can vary, resulting in false failures
		// during tests even if time is correct
		"SimpleChanges": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
			},
			UpdateRecords: []ResourceRecord{
				{Entity: "a", Value: 4, TS: time.UnixMilli(4).UTC()},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
			},
			ExpectedUpdate: []ResourceRecord{
				{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(4).UTC()},
			},
		},
		"OutOfOrderWrites": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
				{Entity: "c", Value: 9, TS: time.UnixMilli(5).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
			},
			UpdateRecords: []ResourceRecord{
				{Entity: "a", Value: 6, TS: time.UnixMilli(12).UTC()},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
			},
			ExpectedUpdate: []ResourceRecord{
				{Entity: "a", Value: 6, TS: time.UnixMilli(12).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
			},
		},
		"OutOfOrderOverwrites": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
				{Entity: "c", Value: 9, TS: time.UnixMilli(5).UTC()},
				{Entity: "b", Value: 12, TS: time.UnixMilli(2).UTC()},
				{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3).UTC()},
			},
			UpdateRecords: []ResourceRecord{
				{Entity: "a", Value: 5, TS: time.UnixMilli(20).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(4).UTC()},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10).UTC()},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3).UTC()},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
			},
			ExpectedUpdate: []ResourceRecord{
				{Entity: "c", Value: 3, TS: time.UnixMilli(7).UTC()},
				{Entity: "a", Value: 5, TS: time.UnixMilli(20).UTC()},
				{Entity: "b", Value: 2, TS: time.UnixMilli(4).UTC()},
			},
		},
	}
	testMaterialization := func(t *testing.T, mat Materialization, test TestCase) {
		if numRows, err := mat.NumRows(); err != nil {
			t.Fatalf("Failed to get num rows: %s", err)
		} else if numRows != test.ExpectedRows {
			t.Fatalf("Num rows not equal %d %d", numRows, test.ExpectedRows)
		}
		seg, err := mat.IterateSegment(test.SegmentStart, test.SegmentEnd)
		if err != nil {
			t.Fatalf("Failed to create segment: %s", err)
		}
		i := 0
		expectedRows := test.ExpectedSegment
		for seg.Next() {
			actual := seg.Value()

			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is ineffecient, but these test
			// cases should all be small enough not to matter.
			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(actual, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					expectedRows[i], expectedRows[lastIdx] = expectedRows[lastIdx], expectedRows[i]
					expectedRows = expectedRows[:lastIdx]
					break
				}
			}
			if !found {
				t.Fatalf("Value %v not found in materialization %v", actual, expectedRows)
			}
			i++
		}
		if err := seg.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d", i)
		}
		if err := seg.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
	}
	testUpdate := func(t *testing.T, mat Materialization, test TestCase) {
		if numRows, err := mat.NumRows(); err != nil {
			t.Fatalf("Failed to get num rows: %s", err)
		} else if numRows != test.UpdatedRows {
			t.Fatalf("Num rows not equal %d %d", numRows, test.UpdatedRows)
		}
		seg, err := mat.IterateSegment(test.UpdatedSegmentStart, test.UpdatedSegmentEnd)
		if err != nil {
			t.Fatalf("Failed to create segment: %s", err)
		}
		i := 0
		for seg.Next() {
			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is ineffecient, but these test
			// cases should all be small enough not to matter.
			found := false
			for i, expRow := range test.ExpectedUpdate {
				if reflect.DeepEqual(seg.Value(), expRow) {
					found = true
					lastIdx := len(test.ExpectedUpdate) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					test.ExpectedUpdate[i], test.ExpectedUpdate[lastIdx] = test.ExpectedUpdate[lastIdx], test.ExpectedUpdate[i]
					test.ExpectedUpdate = test.ExpectedUpdate[:lastIdx]
					break
				}
			}
			if !found {
				t.Fatalf("Unexpected materialization row: %v, expected %v", seg.Value(), test.ExpectedUpdate)
			}
			i++
		}
		if err := seg.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d", i)
		}
		if err := seg.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
	}
	runTestCase := func(t *testing.T, test TestCase) {
		id := randomID(Feature)
		table, err := store.CreateResourceTable(id, test.Schema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		for _, rec := range test.WriteRecords {
			if err := table.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v: %s", rec, err)
			}
		}
		mat, err := store.CreateMaterialization(id)
		if err != nil {
			t.Fatalf("Failed to create materialization: %s", err)
		}
		testMaterialization(t, mat, test)

		if err != nil {
			t.Fatalf("Failed to get materialization: %s", err)
		}
		for _, rec := range test.UpdateRecords {
			if err := table.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v: %s", rec, err)
			}
		}
		mat, err = store.UpdateMaterialization(id)
		if err != nil {
			t.Fatalf("Failed to update materialization: %s", err)
		}
		testUpdate(t, mat, test)
		if err := store.DeleteMaterialization(mat.ID()); err != nil {
			t.Fatalf("Failed to delete materialization: %s", err)
		}
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}

}

func testWriteInvalidResourceRecord(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	table, err := store.CreateResourceTable(id, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := table.Write(ResourceRecord{}); err == nil {
		t.Fatalf("Succeeded in writing invalid resource record")
	}
}

func testInvalidMaterialization(t *testing.T, store OfflineStore) {
	id := randomID(Label)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	if _, err := store.CreateResourceTable(id, schema); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateMaterialization(id); err == nil {
		t.Fatalf("Succeeded in materializing label")
	}
}

func testMaterializeUnknown(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	if _, err := store.CreateMaterialization(id); err == nil {
		t.Fatalf("Succeeded in materializing uninitialized resource")
	}
}

func testMaterializationNotFound(t *testing.T, store OfflineStore) {
	id := MaterializationID(uuid.NewString())
	_, err := store.GetMaterialization(id)
	if err == nil {
		t.Fatalf("Succeeded in getting uninitialized materialization")
	}
	err = store.DeleteMaterialization(id)
	if err == nil {
		t.Fatalf("Succeeded in deleting uninitialized materialization")
	}
	var notFoundErr *MaterializationNotFound
	if validCast := errors.As(err, &notFoundErr); !validCast {
		t.Fatalf("Wrong Error type for materialization not found: %T", err)
	}
	if notFoundErr.Error() == "" {
		t.Fatalf("MaterializationNotFound Error not implemented")
	}
}

func testInvalidResourceIDs(t *testing.T, store OfflineStore) {
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	invalidIds := []ResourceID{
		{Type: Feature},
		{Name: uuid.NewString()},
	}
	for _, id := range invalidIds {
		if _, err := store.CreateResourceTable(id, schema); err == nil {
			t.Fatalf("Succeeded in creating invalid ResourceID: %v", id)
		}
	}
}

func testTrainingSet(t *testing.T, store OfflineStore) {
	type expectedTrainingRow struct {
		Features []interface{}
		Label    interface{}
	}
	type TestCase struct {
		FeatureRecords [][]ResourceRecord
		LabelRecords   []ResourceRecord
		ExpectedRows   []expectedTrainingRow
		FeatureSchema  []TableSchema
		LabelSchema    TableSchema
	}

	tests := map[string]TestCase{
		"Empty": {
			FeatureRecords: [][]ResourceRecord{
				// One feature with no records.
				{},
			},
			LabelRecords:  []ResourceRecord{},
			FeatureSchema: []TableSchema{{}},
			// No rows expected
			ExpectedRows: []expectedTrainingRow{},
		},
		"SimpleJoin": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "c", Value: "blue"},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: Int},
						{Name: "label", ValueType: Bool},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
						{Name: "label", ValueType: Bool},
					},
				},
			},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: true},
				{Entity: "b", Value: false},
				{Entity: "c", Value: true},
			},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Bool},
				},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						1,
						"red",
					},
					Label: true,
				},
				{
					Features: []interface{}{
						2,
						"green",
					},
					Label: false,
				},
				{
					Features: []interface{}{
						3,
						"blue",
					},
					Label: true,
				},
			},
		},
		"ComplexJoin": {
			FeatureRecords: [][]ResourceRecord{
				// Overwritten feature.
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
					{Entity: "a", Value: 4},
				},
				// Feature didn't exist before label
				{
					{Entity: "a", Value: "doesnt exist", TS: time.UnixMilli(11)},
				},
				// Feature didn't change after label
				{
					{Entity: "c", Value: "real value first", TS: time.UnixMilli(5)},
					{Entity: "c", Value: "real value second", TS: time.UnixMilli(5)},
					{Entity: "c", Value: "overwritten", TS: time.UnixMilli(4)},
				},
				// Different feature values for different TS.
				{
					{Entity: "b", Value: "first", TS: time.UnixMilli(3)},
					{Entity: "b", Value: "second", TS: time.UnixMilli(4)},
					{Entity: "b", Value: "third", TS: time.UnixMilli(8)},
				},
				// Empty feature.
				{},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
					},
				},
				{
					Columns: []TableColumn{
						{},
					},
				},
			},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10)},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3)},
				{Entity: "b", Value: 5, TS: time.UnixMilli(5)},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7)},
			},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Int},
				},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						4, nil, nil, nil, nil,
					},
					Label: 1,
				},
				{
					Features: []interface{}{
						2, nil, nil, "first", nil,
					},
					Label: 9,
				},
				{
					Features: []interface{}{
						2, nil, nil, "second", nil,
					},
					Label: 5,
				},
				{
					Features: []interface{}{
						3, nil, "real value second", nil, nil,
					},
					Label: 3,
				},
			},
		},
	}
	runTestCase := func(t *testing.T, test TestCase) {
		featureIDs := make([]ResourceID, len(test.FeatureRecords))

		for i, recs := range test.FeatureRecords {
			id := randomID(Feature)
			featureIDs[i] = id
			table, err := store.CreateResourceTable(id, test.FeatureSchema[i])
			if err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
			for _, rec := range recs {
				if err := table.Write(rec); err != nil {
					t.Fatalf("Failed to write record %v: %v", rec, err)
				}
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, test.LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		for _, rec := range test.LabelRecords {
			if err := labelTable.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v", rec)
			}
		}
		def := TrainingSetDef{
			ID:       randomID(TrainingSet),
			Label:    labelID,
			Features: featureIDs,
		}
		if err := store.CreateTrainingSet(def); err != nil {
			t.Fatalf("Failed to create training set: %s", err)
		}
		iter, err := store.GetTrainingSet(def.ID)
		if err != nil {
			t.Fatalf("Failed to get training set: %s", err)
		}
		i := 0
		expectedRows := test.ExpectedRows
		for iter.Next() {
			realRow := expectedTrainingRow{
				Features: iter.Features(),
				Label:    iter.Label(),
			}

			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is ineffecient, but these test
			// cases should all be small enough not to matter.
			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(realRow, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					expectedRows[i], expectedRows[lastIdx] = expectedRows[lastIdx], expectedRows[i]
					expectedRows = expectedRows[:lastIdx]
					break
				}
			}
			if !found {
				for i, v := range realRow.Features {
					fmt.Printf("Got %T Expected %T\n", v, expectedRows[0].Features[i])
				}
				t.Fatalf("Unexpected training row: %v, expected %v", realRow, expectedRows)
			}
			i++
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("Failed to iterate training set: %s", err)
		}
		if len(test.ExpectedRows) != i {
			t.Fatalf("Training set has different number of rows %d %d", len(test.ExpectedRows), i)
		}
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func testTrainingSetUpdate(t *testing.T, store OfflineStore) {
	type expectedTrainingRow struct {
		Features []interface{}
		Label    interface{}
	}
	type TestCase struct {
		FeatureRecords        [][]ResourceRecord
		UpdatedFeatureRecords [][]ResourceRecord
		LabelRecords          []ResourceRecord
		UpdatedLabelRecords   []ResourceRecord
		ExpectedRows          []expectedTrainingRow
		UpdatedExpectedRows   []expectedTrainingRow
		FeatureSchema         []TableSchema
		LabelSchema           TableSchema
	}

	tests := map[string]TestCase{
		"Empty": {
			FeatureRecords: [][]ResourceRecord{
				// One feature with no records.
				{},
			},
			UpdatedFeatureRecords: [][]ResourceRecord{
				// One feature with no records.
				{},
			},
			LabelRecords:        []ResourceRecord{},
			UpdatedLabelRecords: []ResourceRecord{},
			FeatureSchema:       []TableSchema{{}},
			// No rows expected
			ExpectedRows:        []expectedTrainingRow{},
			UpdatedExpectedRows: []expectedTrainingRow{},
		},
		"SimpleJoin": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "c", Value: "blue"},
				},
			},
			UpdatedFeatureRecords: [][]ResourceRecord{
				{
					{Entity: "d", Value: 4},
				},
				{
					{Entity: "d", Value: "purple"},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: Int},
						{Name: "label", ValueType: Bool},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
						{Name: "label", ValueType: Bool},
					},
				},
			},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: true},
				{Entity: "b", Value: false},
				{Entity: "c", Value: true},
			},
			UpdatedLabelRecords: []ResourceRecord{
				{Entity: "a", Value: true},
				{Entity: "b", Value: false},
				{Entity: "c", Value: true},
				{Entity: "c", Value: false},
			},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Bool},
				},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						1,
						"red",
					},
					Label: true,
				},
				{
					Features: []interface{}{
						2,
						"green",
					},
					Label: false,
				},
				{
					Features: []interface{}{
						3,
						"blue",
					},
					Label: true,
				},
			},
			UpdatedExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						1,
						"red",
					},
					Label: true,
				},
				{
					Features: []interface{}{
						2,
						"green",
					},
					Label: false,
				},
				{
					Features: []interface{}{
						3,
						"blue",
					},
					Label: true,
				},
				{
					Features: []interface{}{
						4,
						"purple",
					},
					Label: false,
				},
			},
		},
		"ComplexJoin": {
			FeatureRecords: [][]ResourceRecord{
				// Overwritten feature.
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
					{Entity: "a", Value: 4},
				},
				// Feature didn't exist before label
				{
					{Entity: "a", Value: "doesnt exist", TS: time.UnixMilli(11)},
				},
				// Feature didn't change after label
				{
					{Entity: "c", Value: "real value first", TS: time.UnixMilli(5)},
					{Entity: "c", Value: "real value second", TS: time.UnixMilli(5)},
					{Entity: "c", Value: "overwritten", TS: time.UnixMilli(4)},
				},
				// Different feature values for different TS.
				{
					{Entity: "b", Value: "first", TS: time.UnixMilli(3)},
					{Entity: "b", Value: "second", TS: time.UnixMilli(4)},
					{Entity: "b", Value: "third", TS: time.UnixMilli(8)},
				},
				// Empty feature.
				{},
			},
			UpdatedFeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 5},
				},
				{},
				{},
				{
					{Entity: "b", Value: "zeroth", TS: time.UnixMilli(3)},
				},
				{},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: String},
						{Name: "value", ValueType: String},
					},
				},
				{
					Columns: []TableColumn{
						{},
					},
				},
			},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10)},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3)},
				{Entity: "b", Value: 5, TS: time.UnixMilli(5)},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7)},
			},
			UpdatedLabelRecords: []ResourceRecord{},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Int},
				},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						4, nil, nil, nil, nil,
					},
					Label: 1,
				},
				{
					Features: []interface{}{
						2, nil, nil, "first", nil,
					},
					Label: 9,
				},
				{
					Features: []interface{}{
						2, nil, nil, "second", nil,
					},
					Label: 5,
				},
				{
					Features: []interface{}{
						3, nil, "real value second", nil, nil,
					},
					Label: 3,
				},
			},
			UpdatedExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						5, nil, nil, nil, nil,
					},
					Label: 1,
				},
				{
					Features: []interface{}{
						2, nil, nil, "zeroth", nil,
					},
					Label: 9,
				},
				{
					Features: []interface{}{
						2, nil, nil, "second", nil,
					},
					Label: 5,
				},
				{
					Features: []interface{}{
						3, nil, "real value second", nil, nil,
					},
					Label: 3,
				},
			},
		},
	}
	runTestCase := func(t *testing.T, test TestCase) {
		featureIDs := make([]ResourceID, len(test.FeatureRecords))
		featureTables := make([]OfflineTable, 0)
		for i, recs := range test.FeatureRecords {
			id := randomID(Feature)
			featureIDs[i] = id
			table, err := store.CreateResourceTable(id, test.FeatureSchema[i])
			featureTables = append(featureTables, table)
			if err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
			for _, rec := range recs {
				if err := table.Write(rec); err != nil {
					t.Fatalf("Failed to write record %v: %v", rec, err)
				}
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, test.LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		for _, rec := range test.LabelRecords {
			if err := labelTable.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v", rec)
			}
		}
		def := TrainingSetDef{
			ID:       randomID(TrainingSet),
			Label:    labelID,
			Features: featureIDs,
		}
		if err := store.CreateTrainingSet(def); err != nil {
			t.Fatalf("Failed to create training set: %s", err)
		}
		iter, err := store.GetTrainingSet(def.ID)
		if err != nil {
			t.Fatalf("Failed to get training set: %s", err)
		}
		i := 0
		expectedRows := test.ExpectedRows
		for iter.Next() {
			realRow := expectedTrainingRow{
				Features: iter.Features(),
				Label:    iter.Label(),
			}
			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is ineffecient, but these test
			// cases should all be small enough not to matter.
			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(realRow, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					expectedRows[i], expectedRows[lastIdx] = expectedRows[lastIdx], expectedRows[i]
					expectedRows = expectedRows[:lastIdx]
					break
				}
			}
			if !found {
				for i, v := range realRow.Features {
					fmt.Printf("Got %T Expected %T\n", v, expectedRows[0].Features[i])
				}
				t.Fatalf("Unexpected training row: %v, expected %v", realRow, expectedRows)
			}
			i++
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("Failed to iterate training set: %s", err)
		}
		if len(test.ExpectedRows) != i {
			t.Fatalf("Training set has different number of rows %d %d", len(test.ExpectedRows), i)
		}
		for i, table := range featureTables {
			for _, rec := range test.UpdatedFeatureRecords[i] {
				if err := table.Write(rec); err != nil {
					t.Errorf("Could not write update record: %v", rec)
				}
			}
		}
		if err := store.UpdateTrainingSet(def); err != nil {
			t.Fatalf("Failed to update training set: %s", err)
		}
		iter, err = store.GetTrainingSet(def.ID)
		if err != nil {
			t.Fatalf("Failed to get updated training set: %s", err)
		}
		i = 0
		expectedRows = test.UpdatedExpectedRows
		for iter.Next() {
			realRow := expectedTrainingRow{
				Features: iter.Features(),
				Label:    iter.Label(),
			}
			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is ineffecient, but these test
			// cases should all be small enough not to matter.
			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(realRow, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					expectedRows[i], expectedRows[lastIdx] = expectedRows[lastIdx], expectedRows[i]
					expectedRows = expectedRows[:lastIdx]
					break
				}
			}
			if !found {
				for i, v := range realRow.Features {
					fmt.Printf("Got %T Expected %T\n", v, expectedRows[0].Features[i])
				}
				t.Fatalf("Unexpected updated training row: %v, expected %v", realRow, expectedRows)
			}
			i++
		}
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func testGetTrainingSetInvalidResourceID(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting invalid training set ResourceID")
	}
}

func testGetUnkonwnTrainingSet(t *testing.T, store OfflineStore) {
	// This should default to TrainingSet
	id := randomID(NoType)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting unknown training set ResourceID")
	} else if _, valid := err.(*TrainingSetNotFound); !valid {
		t.Fatalf("Wrong error for training set not found: %T", err)
	} else if err.Error() == "" {
		t.Fatalf("Training set not found error msg not set")
	}
}

func testInvalidTrainingSetDefs(t *testing.T, store OfflineStore) {
	invalidDefs := map[string]TrainingSetDef{
		"WrongTSType": TrainingSetDef{
			ID:    randomID(Feature),
			Label: randomID(Label),
			Features: []ResourceID{
				randomID(Feature),
				randomID(Feature),
				randomID(Feature),
			},
		},
		"WrongLabelType": TrainingSetDef{
			ID:    randomID(TrainingSet),
			Label: randomID(Feature),
			Features: []ResourceID{
				randomID(Feature),
				randomID(Feature),
				randomID(Feature),
			},
		},
		"WrongFeatureType": TrainingSetDef{
			ID:    randomID(TrainingSet),
			Label: randomID(Label),
			Features: []ResourceID{
				randomID(Feature),
				randomID(Label),
				randomID(Feature),
			},
		},
		"NoFeatures": TrainingSetDef{
			ID:       randomID(TrainingSet),
			Label:    randomID(Label),
			Features: []ResourceID{},
		},
	}
	for name, def := range invalidDefs {
		t.Run(name, func(t *testing.T) {
			if err := store.CreateTrainingSet(def); err == nil {
				t.Fatalf("Succeeded to create invalid def")
			}
		})
	}
}

func testLabelTableNotFound(t *testing.T, store OfflineStore) {
	featureID := randomID(Feature)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	if _, err := store.CreateResourceTable(featureID, schema); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	def := TrainingSetDef{
		ID:    randomID(TrainingSet),
		Label: randomID(Label),
		Features: []ResourceID{
			featureID,
		},
	}
	if err := store.CreateTrainingSet(def); err == nil {
		t.Fatalf("Succeeded in creating training set with unknown label")
	}
}

func testFeatureTableNotFound(t *testing.T, store OfflineStore) {
	labelID := randomID(Label)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	if _, err := store.CreateResourceTable(labelID, schema); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	def := TrainingSetDef{
		ID:    randomID(TrainingSet),
		Label: labelID,
		Features: []ResourceID{
			randomID(Feature),
		},
	}
	if err := store.CreateTrainingSet(def); err == nil {
		t.Fatalf("Succeeded in creating training set with unknown feature")
	}
}

func testTrainingSetDefShorthand(t *testing.T, store OfflineStore) {
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: String},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	fId := randomID(Feature)
	fTable, err := store.CreateResourceTable(fId, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	fTable.Write(ResourceRecord{Entity: "a", Value: "feature"})
	lId := randomID(Label)
	lTable, err := store.CreateResourceTable(lId, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	lTable.Write(ResourceRecord{Entity: "a", Value: "label"})
	// TrainingSetDef can be done in shorthand without types. Their types should
	// be set automatically by the check() function.
	lId.Type = NoType
	fId.Type = NoType
	def := TrainingSetDef{
		ID:       randomID(NoType),
		Label:    lId,
		Features: []ResourceID{fId},
	}
	if err := store.CreateTrainingSet(def); err != nil {
		t.Fatalf("Failed to create training set: %s", err)
	}
}

func testPrimaryCreateTable(t *testing.T, store OfflineStore) {
	type TestCreateCase struct {
		Rec         ResourceID
		Schema      TableSchema
		ExpectError bool
		ExpectValue PrimaryTable
	}
	testCreate := map[string]TestCreateCase{
		"InvalidLabelResource": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Label,
			},
			Schema: TableSchema{
				Columns: []TableColumn{},
			},
			ExpectError: true,
			ExpectValue: nil,
		},
		"InvalidFeatureResource": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Feature,
			},
			Schema: TableSchema{
				Columns: []TableColumn{},
			},
			ExpectError: true,
			ExpectValue: nil,
		},
		"InvalidTrainingSetResource": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: TrainingSet,
			},
			Schema: TableSchema{
				Columns: []TableColumn{},
			},
			ExpectError: true,
			ExpectValue: nil,
		},
		"InvalidColumns": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{},
			},
			ExpectError: true,
			ExpectValue: nil,
		},
		"ValidPrimaryTable": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int", ValueType: Int},
					{Name: "bool", ValueType: Bool},
					{Name: "string", ValueType: String},
					{Name: "float", ValueType: Float32},
				},
			},
			ExpectError: false,
			ExpectValue: nil,
		},
	}

	testPrimary := func(t *testing.T, c TestCreateCase, store OfflineStore) {
		_, err := store.CreatePrimaryTable(c.Rec, c.Schema)
		if err != nil && c.ExpectError == false {
			t.Fatalf("Did not expected error, received: %v", err)
		}
	}
	for name, test := range testCreate {
		t.Run(name, func(t *testing.T) {
			testPrimary(t, test, store)
		})
	}
}

func testPrimaryTableWrite(t *testing.T, store OfflineStore) {
	type TestCase struct {
		Rec         ResourceID
		Schema      TableSchema
		Records     []GenericRecord
		ExpectError bool
		Expected    []GenericRecord
	}

	tests := map[string]TestCase{
		"NoColumnEmpty": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Int},
					{Name: "timestamp", ValueType: Timestamp},
				},
			},
			Records:     []GenericRecord{},
			ExpectError: false,
			Expected:    []GenericRecord{},
		},
		"SimpleColumnEmpty": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Int},
					{Name: "timestamp", ValueType: Timestamp},
				},
			},
			Records:     []GenericRecord{},
			ExpectError: false,
			Expected:    []GenericRecord{},
		},
		"SimpleWrite": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Int},
					{Name: "timestamp", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, time.UnixMilli(0)},
				[]interface{}{"b", 2, time.UnixMilli(0)},
				[]interface{}{"c", 3, time.UnixMilli(0)},
			},
			ExpectError: false,
			Expected:    []GenericRecord{},
		},
	}

	testTableWrite := func(t *testing.T, test TestCase) {
		table, err := store.CreatePrimaryTable(test.Rec, test.Schema)
		if err != nil {
			t.Fatalf("Could not create table: %v", err)
		}
		_, err = store.GetPrimaryTable(test.Rec) // Need To Fix Schema Here
		if err != nil {
			t.Fatalf("Could not get Primary table: %v", err)
		}
		for _, record := range test.Records {
			if err := table.Write(record); err != nil {
				t.Fatalf("Could not write: %v", err)
			}
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testTableWrite(t, test)
		})
	}

}

func testTransform(t *testing.T, store OfflineStore) {

	type TransformTest struct {
		PrimaryTable ResourceID
		Schema       TableSchema
		Records      []GenericRecord
		Config       TransformationConfig
		Expected     []GenericRecord
	}

	tests := map[string]TransformTest{
		"Simple": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int", ValueType: Int},
					{Name: "flt", ValueType: Float64},
					{Name: "str", ValueType: String},
					{Name: "bool", ValueType: Bool},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0)},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0)},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0)},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: uuid.NewString(),
					Type: Transformation,
				},
				Query: "SELECT * FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0).UTC()},
			},
		},
		"Count": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int", ValueType: Int},
					{Name: "str", ValueType: String},
					{Name: "bool", ValueType: Bool},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, "test string", true, time.UnixMilli(0)},
				[]interface{}{"b", 2, "second string", false, time.UnixMilli(0)},
				[]interface{}{"c", 3, "third string", nil, time.UnixMilli(0)},
				[]interface{}{"d", 4, "fourth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 5, "fifth string", true, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: uuid.NewString(),
					Type: Transformation,
				},
				Query: "SELECT COUNT(*) as total_count FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{5},
			},
		},
	}

	testTransform := func(t *testing.T, test TransformTest) {
		table, err := store.CreatePrimaryTable(test.PrimaryTable, test.Schema)
		if err != nil {
			t.Fatalf("Could not initialize table: %v", err)
		}

		for _, value := range test.Records {
			if err := table.Write(value); err != nil {
				t.Fatalf("Could not write value: %v: %v", err, value)
			}
		}

		tableName := getTableName(t.Name(), table.GetName())
		test.Config.Query = strings.Replace(test.Config.Query, "tb", tableName, 1)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}

		rows, err := table.NumRows()
		if err != nil {
			t.Fatalf("could not get NumRows of table: %v", err)
		}
		if int(rows) != len(test.Records) {
			t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(test.Records), rows)
		}

		table, err = store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get transformation table: %v", err)
		}

		iterator, err := table.IterateSegment(100)
		if err != nil {
			t.Fatalf("Could not get generic iterator: %v", err)
		}

		tableSize := 0
		for iterator.Next() {
			if iterator.Err() != nil {
				t.Fatalf("could not iterate rows: %v", iterator.Err())
			}

			found := false
			tableSize += 1
			for i := range test.Expected {
				if reflect.DeepEqual(iterator.Values(), test.Expected[i]) {
					found = true
				}
			}

			tableColumns := iterator.Columns()
			if len(tableColumns) == 0 {
				t.Fatalf("The table doesn't have any columns.")
			}

			if !found {
				t.Fatalf("The %v value was not found in Expected Values: %v", iterator.Values(), test.Expected)
			}
		}

		if tableSize != len(test.Expected) {
			t.Fatalf("The number of records do not match for received (%v) and expected (%v)", tableSize, len(test.Expected))
		}
		if err := iterator.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testTransform(t, test)
		})
	}

}

func testTransformUpdate(t *testing.T, store OfflineStore) {

	type TransformTest struct {
		PrimaryTable    ResourceID
		Schema          TableSchema
		Records         []GenericRecord
		UpdatedRecords  []GenericRecord
		Config          TransformationConfig
		Expected        []GenericRecord
		UpdatedExpected []GenericRecord
	}

	tests := map[string]TransformTest{
		"Simple": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int", ValueType: Int},
					{Name: "flt", ValueType: Float64},
					{Name: "str", ValueType: String},
					{Name: "bool", ValueType: Bool},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0)},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0)},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0)},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0)},
			},
			UpdatedRecords: []GenericRecord{
				[]interface{}{"d", 6, 1.6, "sixth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 7, 1.7, "seventh string", true, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: uuid.NewString(),
					Type: Transformation,
				},
				Query: "SELECT * FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0).UTC()},
			},
			UpdatedExpected: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 6, 1.6, "sixth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 7, 1.7, "seventh string", true, time.UnixMilli(0).UTC()},
			},
		},
		"Count": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int", ValueType: Int},
					{Name: "str", ValueType: String},
					{Name: "bool", ValueType: Bool},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, "test string", true, time.UnixMilli(0)},
				[]interface{}{"b", 2, "second string", false, time.UnixMilli(0)},
				[]interface{}{"c", 3, "third string", nil, time.UnixMilli(0)},
				[]interface{}{"d", 4, "fourth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 5, "fifth string", true, time.UnixMilli(0)},
			},
			UpdatedRecords: []GenericRecord{
				[]interface{}{"d", 6, "sixth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 7, "seventh string", true, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: uuid.NewString(),
					Type: Transformation,
				},
				Query: "SELECT COUNT(*) as total_count FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{5},
			},
			UpdatedExpected: []GenericRecord{
				[]interface{}{7},
			},
		},
	}

	testTransform := func(t *testing.T, test TransformTest) {
		table, err := store.CreatePrimaryTable(test.PrimaryTable, test.Schema)
		if err != nil {
			t.Fatalf("Could not initialize table: %v", err)
		}
		for _, value := range test.Records {
			if err := table.Write(value); err != nil {
				t.Fatalf("Could not write value: %v: %v", err, value)
			}
		}

		tableName := getTableName(t.Name(), table.GetName())
		test.Config.Query = strings.Replace(test.Config.Query, "tb", tableName, 1)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}
		rows, err := table.NumRows()
		if err != nil {
			t.Fatalf("could not get NumRows of table: %v", err)
		}
		if int(rows) != len(test.Records) {
			t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(test.Records), rows)
		}
		table, err = store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get transformation table: %v", err)
		}
		iterator, err := table.IterateSegment(100)
		if err != nil {
			t.Fatalf("Could not get generic iterator: %v", err)
		}
		i := 0
		for iterator.Next() {
			found := false
			for i, expRow := range test.Expected {
				if reflect.DeepEqual(iterator.Values(), expRow) {
					found = true
					lastIdx := len(test.Expected) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					test.Expected[i], test.Expected[lastIdx] = test.Expected[lastIdx], test.Expected[i]
					test.Expected = test.Expected[:lastIdx]
					break
				}
			}
			if !found {
				t.Fatalf("Unexpected training row: %v, expected %v", iterator.Values(), test.Expected)
			}
			i++
		}
		if err := iterator.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
		table, err = store.GetPrimaryTable(test.PrimaryTable)
		if err != nil {
			t.Fatalf("Could not get primary table: %v", err)
		}
		for _, rec := range test.UpdatedRecords {
			if err := table.Write(rec); err != nil {
				t.Errorf("could not write to table: %v", err)
			}
		}
		if err := store.UpdateTransformation(test.Config); err != nil {
			t.Errorf("could not update transformation: %v", err)
		}
		table, err = store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get updated transformation table: %v", err)
		}

		iterator, err = table.IterateSegment(100)
		if err != nil {
			t.Fatalf("Could not get generic iterator: %v", err)
		}

		i = 0
		for iterator.Next() {
			found := false
			for i, expRow := range test.UpdatedExpected {
				if reflect.DeepEqual(iterator.Values(), expRow) {
					found = true
					lastIdx := len(test.UpdatedExpected) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation expect that it re-orders the slice.
					test.UpdatedExpected[i], test.UpdatedExpected[lastIdx] = test.UpdatedExpected[lastIdx], test.UpdatedExpected[i]
					test.UpdatedExpected = test.UpdatedExpected[:lastIdx]
					break
				}
			}
			if !found {
				t.Fatalf("Unexpected training row: %v, expected %v", iterator.Values(), test.UpdatedExpected)
			}
			i++
		}
		if err := iterator.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testTransform(t, test)
		})
	}

}

func testTransformCreateFeature(t *testing.T, store OfflineStore) {
	type TransformTest struct {
		PrimaryTable ResourceID
		Schema       TableSchema
		Records      []GenericRecord
		Config       TransformationConfig
		Expected     []GenericRecord
	}

	tests := map[string]TransformTest{
		"Simple": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int", ValueType: Int},
					{Name: "flt", ValueType: Float64},
					{Name: "str", ValueType: String},
					{Name: "bool", ValueType: Bool},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0)},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0)},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0)},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: uuid.NewString(),
					Type: Feature,
				},
				Query: "SELECT entity, int, ts FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0).UTC()},
			},
		},
	}

	testTransform := func(t *testing.T, test TransformTest) {
		table, err := store.CreatePrimaryTable(test.PrimaryTable, test.Schema)
		if err != nil {
			t.Fatalf("Could not initialize table: %v", err)
		}
		for _, value := range test.Records {
			if err := table.Write(value); err != nil {
				t.Fatalf("Could not write value: %v: %v", err, value)
			}
		}

		tableName := getTableName(t.Name(), table.GetName())
		test.Config.Query = strings.Replace(test.Config.Query, "tb", tableName, 1)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}
		rows, err := table.NumRows()
		if err != nil {
			t.Fatalf("could not get NumRows of table: %v", err)
		}
		if int(rows) != len(test.Records) {
			t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(test.Records), rows)
		}
		_, err = store.GetResourceTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get transformation table: %v", err)
			return
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testTransform(t, test)
		})
	}
	// Test if can materialized a transformed table
}

func testCreateDuplicatePrimaryTable(t *testing.T, store OfflineStore) {
	table := uuid.NewString()
	rec := ResourceID{
		Name: table,
		Type: Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{
				Name:      "entity",
				ValueType: Int,
			},
		},
	}
	_, err := store.CreatePrimaryTable(rec, schema)
	if err != nil {
		t.Fatalf("Could not create initial table: %v", err)
	}
	_, err = store.CreatePrimaryTable(rec, schema)
	if err == nil {
		t.Fatalf("Successfully create duplicate tables")
	}
}

func testChainTransform(t *testing.T, store OfflineStore) {
	type TransformTest struct {
		PrimaryTable ResourceID
		Schema       TableSchema
		Records      []GenericRecord
		Config       TransformationConfig
		Expected     []GenericRecord
	}

	firstTransformName := uuid.NewString()
	tests := map[string]TransformTest{
		"First": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int_col", ValueType: Int},
					{Name: "flt_col", ValueType: Float64},
					{Name: "str_col", ValueType: String},
					{Name: "bool_col", ValueType: Bool},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0)},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0)},
				[]interface{}{"c", 3, 1.3, "third string", nil, time.UnixMilli(0)},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0)},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: firstTransformName,
					Type: Transformation,
				},
				Query: "SELECT entity, int_col, flt_col, str_col FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string"},
				[]interface{}{"b", 2, 1.2, "second string"},
				[]interface{}{"c", 3, 1.3, "third string"},
				[]interface{}{"d", 4, 1.4, "fourth string"},
				[]interface{}{"e", 5, 1.5, "fifth string"},
			},
		},
		"Second": {
			PrimaryTable: ResourceID{
				Name: firstTransformName,
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "int_col", ValueType: Int},
					{Name: "str_col", ValueType: String},
				},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: uuid.NewString(),
					Type: Transformation,
				},
				Query: "SELECT COUNT(*) as total_count FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{5},
			},
		},
	}

	table, err := store.CreatePrimaryTable(tests["First"].PrimaryTable, tests["First"].Schema)
	if err != nil {
		t.Fatalf("Could not initialize table: %v", err)
	}
	for _, value := range tests["First"].Records {
		if err := table.Write(value); err != nil {
			t.Fatalf("Could not write value: %v: %v", err, value)
		}
	}

	tableName := getTableName(t.Name(), table.GetName())
	config := TransformationConfig{
		Type: SQLTransformation,
		TargetTableID: ResourceID{
			Name: firstTransformName,
			Type: Transformation,
		},
		Query: fmt.Sprintf("SELECT entity, int_col, flt_col, str_col FROM %s", tableName),
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: tableName,
				Source:   tableName,
			},
		},
	}
	if err := store.CreateTransformation(config); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}
	rows, err := table.NumRows()
	if err != nil {
		t.Fatalf("could not get NumRows of table: %v", err)
	}
	if int(rows) != len(tests["First"].Records) {
		t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(tests["First"].Records), rows)
	}
	table, err = store.GetTransformationTable(tests["First"].Config.TargetTableID)
	if err != nil {
		t.Errorf("Could not get transformation table: %v", err)
	}
	iterator, err := table.IterateSegment(100)
	if err != nil {
		t.Fatalf("Could not get generic iterator: %v", err)
	}
	tableSize := 0
	for iterator.Next() {
		found := false
		tableSize += 1

		for i := range tests["First"].Expected {
			if reflect.DeepEqual(iterator.Values(), tests["First"].Expected[i]) {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("The %v value was not found in Expected Values: %v", iterator.Values(), tests["First"].Expected)
		}
	}
	if err := iterator.Close(); err != nil {
		t.Fatalf("Could not close iterator: %v", err)
	}

	if tableSize != len(tests["First"].Expected) {
		t.Fatalf("The number of records do not match for received (%v) and expected (%v)", tableSize, len(tests["First"].Expected))
	}

	tableName = getTableName(t.Name(), table.GetName())
	secondTransformName := uuid.NewString()
	config = TransformationConfig{
		Type: SQLTransformation,
		TargetTableID: ResourceID{
			Name: secondTransformName,
			Type: Transformation,
		},
		Query: fmt.Sprintf("SELECT Count(*) as total_count FROM %s", tableName),
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: tableName,
				Source:   tableName,
			},
		},
	}
	if err := store.CreateTransformation(config); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}

	table, err = store.GetTransformationTable(config.TargetTableID)
	if err != nil {
		t.Errorf("Could not get transformation table: %v", err)
	}
	iterator, err = table.IterateSegment(100)
	if err != nil {
		t.Fatalf("Could not get generic iterator: %v", err)
	}
	i := 0
	for iterator.Next() {
		if !reflect.DeepEqual(iterator.Values(), tests["Second"].Expected[i]) {
			t.Fatalf("Expected: %#v, Received %#v", tests["Second"].Expected[i], iterator.Values())
		}
		i++
	}
	if err := iterator.Close(); err != nil {
		t.Fatalf("Could not close iterator: %v", err)
	}

}

func testTransformToMaterialize(t *testing.T, store OfflineStore) {

	type TransformTest struct {
		PrimaryTable ResourceID
		Schema       TableSchema
		Records      []GenericRecord
		Config       TransformationConfig
		Expected     []GenericRecord
	}

	firstTransformName := uuid.NewString()
	tests := map[string]TransformTest{
		"First": {
			PrimaryTable: ResourceID{
				Name: uuid.NewString(),
				Type: Feature,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: String},
					{Name: "value", ValueType: Int},
					{Name: "ts", ValueType: Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, time.UnixMilli(0)},
				[]interface{}{"b", 2, time.UnixMilli(0)},
				[]interface{}{"c", 3, time.UnixMilli(0)},
				[]interface{}{"d", 4, time.UnixMilli(0)},
				[]interface{}{"e", 5, time.UnixMilli(0)},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name: firstTransformName,
					Type: Transformation,
				},
				Query: "SELECT entity, int, flt, str FROM tb",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "tb",
						Source:   "TBD",
					},
				},
			},
			Expected: []GenericRecord{
				[]interface{}{"a", 1, time.UnixMilli(0)},
				[]interface{}{"b", 2, time.UnixMilli(0)},
				[]interface{}{"c", 3, time.UnixMilli(0)},
				[]interface{}{"d", 4, time.UnixMilli(0)},
				[]interface{}{"e", 5, time.UnixMilli(0)},
			},
		},
	}

	table, err := store.CreatePrimaryTable(tests["First"].PrimaryTable, tests["First"].Schema)
	if err != nil {
		t.Fatalf("Could not initialize table: %v", err)
	}
	for _, value := range tests["First"].Records {
		if err := table.Write(value); err != nil {
			t.Fatalf("Could not write value: %v: %v", err, value)
		}
	}

	tableName := getTableName(t.Name(), table.GetName())
	config := TransformationConfig{
		Type: SQLTransformation,
		TargetTableID: ResourceID{
			Name: firstTransformName,
			Type: Transformation,
		},
		Query: fmt.Sprintf("SELECT entity, int, flt, str FROM %s", tableName),
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: tableName,
				Source:   tableName,
			},
		},
	}
	if err := store.CreateTransformation(config); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}
	rows, err := table.NumRows()
	if err != nil {
		t.Fatalf("could not get NumRows of table: %v", err)
	}
	if int(rows) != len(tests["First"].Records) {
		t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(tests["First"].Records), rows)
	}
	mat, err := store.CreateMaterialization(tests["First"].Config.TargetTableID)
	if err != nil {
		t.Fatalf("Could not create materialization: %v", err)
	}
	iterator, err := mat.IterateSegment(0, 10)
	if err != nil {
		t.Fatalf("Could not get iterator: %v", err)
	}
	i := 0
	for iterator.Next() {
		if !reflect.DeepEqual(iterator.Value(), tests["First"].Expected[i]) {
			t.Fatalf("Expected: %#v, Got: %#v", tests["First"].Expected[i], iterator.Value())
		}
		i++
	}
	if err := iterator.Close(); err != nil {
		t.Fatalf("Could not close iterator: %v", err)
	}
}

func testCreateResourceFromSource(t *testing.T, store OfflineStore) {
	primaryID := ResourceID{
		Name: uuid.NewString(),
		Type: Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: String},
			{Name: "col2", ValueType: Int},
			{Name: "col3", ValueType: String},
			{Name: "col4", ValueType: Timestamp},
		},
	}
	table, err := store.CreatePrimaryTable(primaryID, schema)
	if err != nil {
		t.Fatalf("Could not create primary table: %v", err)
	}
	records := []GenericRecord{
		{"a", 1, "one", time.UnixMilli(0)},
		{"b", 2, "two", time.UnixMilli(1)},
		{"c", 3, "three", time.UnixMilli(2)},
		{"d", 4, "four", time.UnixMilli(3)},
		{"e", 5, "five", time.UnixMilli(4)},
	}
	for _, record := range records {
		if err := table.Write(record); err != nil {
			t.Fatalf("Could not write record: %v", err)
		}
	}
	featureID := ResourceID{
		Name: uuid.NewString(),
		Type: Feature,
	}
	recSchema := ResourceSchema{
		Entity:      "col1",
		Value:       "col2",
		TS:          "col4",
		SourceTable: table.GetName(),
	}
	t.Log("Resource Name: ", featureID.Name)
	_, err = store.RegisterResourceFromSourceTable(featureID, recSchema)
	if err != nil {
		t.Fatalf("Could not register from Primary Table: %s", err)
	}
	_, err = store.GetResourceTable(featureID)
	if err != nil {
		t.Fatalf("Could not get resource table: %v", err)
	}
	mat, err := store.CreateMaterialization(featureID)
	if err != nil {
		t.Fatalf("Could not create materialization: %v", err)
	}
	updatedRecords := []GenericRecord{
		{"f", 6, "six", time.UnixMilli(0)},
		{"g", 7, "seven", time.UnixMilli(1)},
		{"h", 8, "eight", time.UnixMilli(2)},
		{"i", 9, "nine", time.UnixMilli(3)},
		{"j", 10, "ten", time.UnixMilli(4)},
	}
	for _, record := range updatedRecords {
		if err := table.Write(record); err != nil {
			t.Fatalf("Could not write record: %v", err)
		}
	}
	err = store.DeleteMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("Could not delete materialization: %v", err)
	}
	mat, err = store.CreateMaterialization(featureID)
	if err != nil {
		t.Fatalf("Could not recreate materialization: %v", err)
	}
	expected, err := table.NumRows()
	if err != nil {
		t.Fatalf("Could not get resource table rows: %v", err)
	}
	actual, err := mat.NumRows()
	if err != nil {
		t.Fatalf("Could not get materialization rows: %v", err)
	}
	if expected != actual {
		t.Errorf("Expected %d Row, Got %d Rows", expected, actual)
	}

}

func testCreateResourceFromSourceNoTS(t *testing.T, store OfflineStore) {
	type expectedTrainingRow struct {
		Features []interface{}
		Label    interface{}
	}

	primaryID := ResourceID{
		Name: uuid.NewString(),
		Type: Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: String},
			{Name: "col2", ValueType: Int},
			{Name: "col3", ValueType: String},
			{Name: "col4", ValueType: Bool},
		},
	}
	table, err := store.CreatePrimaryTable(primaryID, schema)
	if err != nil {
		t.Fatalf("Could not create primary table: %v", err)
	}
	records := []GenericRecord{
		{"a", 1, "one", true},
		{"b", 2, "two", false},
		{"c", 3, "three", false},
		{"d", 4, "four", true},
		{"e", 5, "five", false},
	}
	for _, record := range records {
		if err := table.Write(record); err != nil {
			t.Fatalf("Could not write record: %v", err)
		}
	}
	featureID := ResourceID{
		Name: uuid.NewString(),
		Type: Feature,
	}
	recSchema := ResourceSchema{
		Entity:      "col1",
		Value:       "col2",
		SourceTable: table.GetName(),
	}
	t.Log("Resource Name: ", featureID.Name)
	_, err = store.RegisterResourceFromSourceTable(featureID, recSchema)
	if err != nil {
		t.Fatalf("Could not register from feature Source Table: %s", err)
	}
	_, err = store.GetResourceTable(featureID)
	if err != nil {
		t.Fatalf("Could not get feature resource table: %v", err)
	}
	labelID := ResourceID{
		Name: uuid.NewString(),
		Type: Label,
	}
	labelSchema := ResourceSchema{
		Entity:      "col1",
		Value:       "col4",
		SourceTable: table.GetName(),
	}
	t.Log("Label Name: ", labelID.Name)
	_, err = store.RegisterResourceFromSourceTable(labelID, labelSchema)
	if err != nil {
		t.Fatalf("Could not register label from Source Table: %s", err)
	}
	_, err = store.GetResourceTable(labelID)
	if err != nil {
		t.Fatalf("Could not get label resource table: %v", err)
	}
	tsetID := ResourceID{
		Name: uuid.NewString(),
		Type: TrainingSet,
	}
	def := TrainingSetDef{
		ID: tsetID,
		Features: []ResourceID{
			featureID,
		},
		Label: labelID,
	}
	err = store.CreateTrainingSet(def)
	if err != nil {
		t.Fatalf("Could not get create training set: %v", err)
	}

	train, err := store.GetTrainingSet(tsetID)
	if err != nil {
		t.Fatalf("Could not get get training set: %v", err)
	}
	i := 0
	for train.Next() {
		realRow := expectedTrainingRow{
			Features: train.Features(),
			Label:    train.Label(),
		}
		expectedRows := []expectedTrainingRow{
			{
				Features: []interface{}{records[0][1]},
				Label:    records[0][3],
			},
			{
				Features: []interface{}{records[1][1]},
				Label:    records[1][3],
			},
			{
				Features: []interface{}{records[2][1]},
				Label:    records[2][3],
			},
			{
				Features: []interface{}{records[3][1]},
				Label:    records[3][3],
			},
			{
				Features: []interface{}{records[4][1]},
				Label:    records[4][3],
			},
		}
		found := false
		for i, expRow := range expectedRows {
			if reflect.DeepEqual(realRow, expRow) {
				found = true
				lastIdx := len(expectedRows) - 1
				// Swap the record that we've found to the end, then shrink the slice to not include it.
				// This is essentially a delete operation expect that it re-orders the slice.
				expectedRows[i], expectedRows[lastIdx] = expectedRows[lastIdx], expectedRows[i]
				expectedRows = expectedRows[:lastIdx]
				break
			}
		}
		if !found {
			for i, v := range realRow.Features {
				fmt.Printf("Got %T Expected %T\n", v, expectedRows[0].Features[i])
			}
			t.Fatalf("Unexpected training row: %v, expected %v", realRow, expectedRows)
		}
		i++
	}
}

func testCreatePrimaryFromSource(t *testing.T, store OfflineStore) {
	primaryID := ResourceID{
		Name: uuid.NewString(),
		Type: Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: String},
			{Name: "col2", ValueType: Int},
			{Name: "col3", ValueType: String},
			{Name: "col4", ValueType: Timestamp},
		},
	}
	table, err := store.CreatePrimaryTable(primaryID, schema)
	if err != nil {
		t.Fatalf("Could not create primary table: %v", err)
	}
	records := []GenericRecord{
		{"a", 1, "one", time.UnixMilli(0)},
		{"b", 2, "two", time.UnixMilli(1)},
		{"c", 3, "three", time.UnixMilli(2)},
		{"d", 4, "four", time.UnixMilli(3)},
		{"e", 5, "five", time.UnixMilli(4)},
	}
	for _, record := range records {
		if err := table.Write(record); err != nil {
			t.Fatalf("Could not write record: %v", err)
		}
	}
	primaryCopyID := ResourceID{
		Name: uuid.NewString(),
		Type: Primary,
	}

	t.Log("Primary Name: ", primaryCopyID.Name)
	// Need to sanitize name here b/c the the xxx-xxx format of the uuid. Cannot do it within
	// register function because precreated tables do not necessarily use double quotes
	tableName := sanitizeTableName(string(store.Type()), table.GetName())
	_, err = store.RegisterPrimaryFromSourceTable(primaryCopyID, tableName)
	if err != nil {
		t.Fatalf("Could not register from Source Table: %s", err)
	}
	_, err = store.GetPrimaryTable(primaryCopyID)
	if err != nil {
		t.Fatalf("Could not get primary table: %v", err)
	}
}

func Test_snowflakeOfflineTable_checkTimestamp(t *testing.T) {
	type fields struct {
		db   *sql.DB
		name string
	}
	type args struct {
		rec ResourceRecord
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ResourceRecord
	}{
		{"Nil TimeStamp", fields{nil, ""}, args{rec: ResourceRecord{}}, ResourceRecord{TS: time.UnixMilli(0).UTC()}},
		{"Non Nil TimeStamp", fields{nil, ""}, args{rec: ResourceRecord{TS: time.UnixMilli(10)}}, ResourceRecord{TS: time.UnixMilli(10)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkTimestamp(tt.args.rec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplaceSourceName(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		sourceMap       []SourceMapping
		sanitize        sanitization
		expectedQuery   string
		expectedFailure bool
	}{
		{
			"TwoReplacementsPass",
			"SELECT * FROM {{name1.variant1}} and more {{name2.variant2}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "table1",
				},
				SourceMapping{
					Template: "{{name2.variant2}}",
					Source:   "table2",
				},
			},
			sanitize,
			"SELECT * FROM \"table1\" and more \"table2\"",
			false,
		},
		{
			"OneReplacementPass",
			"SELECT * FROM {{name1.variant1}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "table1",
				},
			},
			sanitize,
			"SELECT * FROM \"table1\"",
			false,
		},
		{
			"OneReplacementWithThreeSourceMappingPass",
			"SELECT * FROM {{name1.variant1}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "table1",
				},
				SourceMapping{
					Template: "{{name2.variant2}}",
					Source:   "table2",
				},
				SourceMapping{
					Template: "{{name3.variant3}}",
					Source:   "table3",
				},
			},
			sanitize,
			"SELECT * FROM \"table1\"",
			false,
		},
		{
			"ReplacementExpectedFailure",
			"SELECT * FROM {{name1.variant1}} and more {{name2.variant2}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "table1",
				},
			},
			sanitize,
			"SELECT * FROM \"table1\" and more {{name2.variant2}}",
			true,
		},
		{
			"TwoReplacementsSparkSQLSanitizePass",
			"SELECT * FROM {{name1.variant1}} and more {{name2.variant2}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "table1",
				},
				SourceMapping{
					Template: "{{name2.variant2}}",
					Source:   "table2",
				},
			},
			sanitizeSparkSQL,
			"SELECT * FROM table1 and more table2",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retreivedQuery, err := replaceSourceName(tt.query, tt.sourceMap, tt.sanitize)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("Could not replace the template query: %v", err)
			}

			if !tt.expectedFailure && !reflect.DeepEqual(retreivedQuery, tt.expectedQuery) {
				t.Fatalf("ReplaceSourceName did not replace the query correctly. Expected \" %v \", got \" %v \".", tt.expectedQuery, retreivedQuery)
			}
		})
	}

}

func getTableName(testName string, tableName string) string {
	if strings.Contains(testName, "BIGQUERY") {
		prefix := fmt.Sprintf("%s.%s", os.Getenv("BIGQUERY_PROJECT_ID"), os.Getenv("BIGQUERY_DATASET_ID"))
		tableName = fmt.Sprintf("`%s.%s`", prefix, tableName)
	} else {
		tableName = sanitize(tableName)
	}
	return tableName
}

func sanitizeTableName(testName string, tableName string) string {
	if !strings.Contains(testName, "BIGQUERY") {
		tableName = sanitize(tableName)
	}
	return tableName
}
