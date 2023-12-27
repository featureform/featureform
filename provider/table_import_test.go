// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/featureform/filestore"
	fs "github.com/featureform/filestore"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

var importProvider = flag.String("provider", "", "provider to perform test on")

type importTestMember struct {
	onlineType      pt.Type
	onlineConfig    pc.SerializedConfig
	offlineType     pt.Type
	offlineConfig   pc.SerializedConfig
	integrationTest bool
}

type materializationTestOption struct {
	storeType  pt.Type
	outputType fs.FileType
}

func (o materializationTestOption) Output() filestore.FileType {
	return o.outputType
}

func (o materializationTestOption) StoreType() pt.Type {
	return o.storeType
}

func (o materializationTestOption) ShouldIncludeHeaders() bool {
	return false
}

func TestImportableOnlineStore(t *testing.T) {
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

	testFns := map[string]func(*testing.T, OfflineStore, ImportableOnlineStore){
		"Import Table": testImportTable,
	}

	dynamoInit := func() *pc.DynamodbConfig {
		dynamoAccessKey := checkEnv("DYNAMO_ACCESS_KEY")
		dynamoSecretKey := checkEnv("DYNAMO_SECRET_KEY")
		dynamoConfig := &pc.DynamodbConfig{
			Region:       checkEnv("DYNAMODB_REGION"),
			AccessKey:    dynamoAccessKey,
			SecretKey:    dynamoSecretKey,
			ImportFromS3: true,
		}
		return dynamoConfig
	}

	awsDatabricksS3Init := func() *pc.SparkConfig {
		executorConfig := &pc.DatabricksConfig{
			Host:    checkEnv("DATABRICKS_HOST"),
			Token:   checkEnv("DATABRICKS_TOKEN"),
			Cluster: checkEnv("DATABRICKS_CLUSTER"),
		}
		fileStoreConfig := &pc.S3FileStoreConfig{
			Credentials: pc.AWSCredentials{
				AWSAccessKeyId: checkEnv("AWS_ACCESS_KEY_ID"),
				AWSSecretKey:   checkEnv("AWS_SECRET_KEY"),
			},
			BucketRegion: checkEnv("S3_BUCKET_REGION"),
			BucketPath:   checkEnv("S3_BUCKET_PATH"),
			Path:         "",
		}

		return &pc.SparkConfig{
			ExecutorType:   pc.Databricks,
			ExecutorConfig: executorConfig,
			StoreType:      fs.S3,
			StoreConfig:    fileStoreConfig,
		}
	}

	testList := []importTestMember{}

	if *importProvider == "dynamo" || *importProvider == "" {
		offlineConfig, err := awsDatabricksS3Init().Serialize()
		if err != nil {
			t.Fatal(err)
		}

		testList = append(testList, importTestMember{
			offlineType:     pt.SparkOffline,
			offlineConfig:   offlineConfig,
			onlineType:      pt.DynamoDBOnline,
			onlineConfig:    dynamoInit().Serialized(),
			integrationTest: true,
		})

		for _, testItem := range testList {
			if testing.Short() && testItem.integrationTest {
				t.Logf("Skipping %s, because it is an integration test", testItem.onlineType)
				continue
			}
			for name, fn := range testFns {
				offlineProvider, err := Get(testItem.offlineType, testItem.offlineConfig)
				if err != nil {
					t.Fatal(err)
				}
				onlineProvider, err := Get(testItem.onlineType, testItem.onlineConfig)
				if err != nil {
					t.Fatal(err)
				}
				offlineStore, err := offlineProvider.AsOfflineStore()
				if err != nil {
					t.Fatal(err)
				}
				onlineStore, err := onlineProvider.AsOnlineStore()
				if err != nil {
					t.Fatal(err)
				}
				importableOnlineStore, isImportableOnlineStore := onlineStore.(ImportableOnlineStore)
				if !isImportableOnlineStore {
					t.Fatalf("%s is not an ImportableOnlineStore", testItem.onlineType)
				}
				t.Run(fmt.Sprintf("%s/%s", testItem.onlineType, name), func(t *testing.T) {
					fn(t, offlineStore, importableOnlineStore)
				})
			}
		}
	}
}

func testImportTable(t *testing.T, offlineStore OfflineStore, importableOnlineStore ImportableOnlineStore) {
	resourceID := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Feature,
	}

	schemaInt := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}

	writeRecords := []ResourceRecord{
		{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
		{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
		{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
		{Entity: "a", Value: 4, TS: time.UnixMilli(1).UTC()},
	}

	table, err := offlineStore.CreateResourceTable(resourceID, schemaInt)
	if err != nil {
		t.Fatal(err)
	}

	if err := table.WriteBatch(writeRecords); err != nil {
		t.Fatal(err)
	}

	options := materializationTestOption{
		storeType:  pt.SparkOffline,
		outputType: fs.CSV,
	}

	mat, err := offlineStore.CreateMaterialization(resourceID, options)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Created materialization %s\n", mat.ID())

	defer func() {
		if err := offlineStore.DeleteMaterialization(mat.ID()); err != nil {
			t.Fatal(err)
		}

		if err := importableOnlineStore.DeleteTable(resourceID.Name, resourceID.Variant); err != nil {
			t.Fatal(err)
		}
	}()

	importSourcePath := getImportSourcePath(t, offlineStore, mat.ID())

	importID, err := importableOnlineStore.ImportTable(resourceID.Name, resourceID.Variant, Int, importSourcePath)
	if err != nil {
		t.Fatalf("failed to import table: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for import to complete")
		case <-ticker.C:
			importStatus, err := importableOnlineStore.GetImport(importID)
			if err != nil {
				t.Fatalf("failed to get import status: %v", err)
			}
			t.Logf("Import status: %s", importStatus.Status())
			if importStatus.Status() == "COMPLETED" {
				return
			}
			if importStatus.Status() == "FAILED" {
				t.Fatalf("import failed: %s", importStatus.ErrorMessage())
			}
		}
	}
}

func getImportSourcePath(t *testing.T, offlineStore OfflineStore, materializationID MaterializationID) fs.Filepath {
	switch offlineStore.Type() {
	case pt.SparkOffline:
		sparkOffline, ok := offlineStore.(*SparkOfflineStore)
		if !ok {
			t.Fatalf("offline store is not a SparkOfflineStore")
		}

		sourceDirPath, err := sparkOffline.Store.CreateDirPath(fmt.Sprintf("featureform/%s", materializationID))
		if err != nil {
			t.Fatalf("failed to create source dir path for resource: %v", err)
		}

		files, err := sparkOffline.Store.List(sourceDirPath, fs.CSV)
		if err != nil {
			t.Fatalf("failed to list files in source dir path %s: %v", sourceDirPath, err)
		}

		if len(files) == 0 {
			t.Fatalf("no files found in source dir path %s", sourceDirPath)
		}

		return files[0]
	default:
		t.Fatalf("Unsupported offline store type: %s", offlineStore.Type())
	}
	return nil
}
