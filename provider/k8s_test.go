//go:build k8s
// +build k8s

package provider

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func uuidWithoutDashes() string {
	return fmt.Sprintf("a%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func TestBlobInterfaces(t *testing.T) {
	blobTests := map[string]func(*testing.T, BlobStore){
		"Test Blob Read and Write": testBlobReadAndWrite,
		"Test Blob CSV Serve":      testBlobCSVServe,
		"Test Blob Parquet Serve":  testBlobParquetServe,
	}
	localBlobStore, err := NewMemoryBlobStore(Config([]byte("")))
	if err != nil {
		t.Fatalf("Failed to create memory blob store")
	}
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}
	fmt.Println(mydir)

	fileStoreConfig := FileBlobStoreConfig{DirPath: fmt.Sprintf(`file:////%s/tests/file_tests`, mydir)}
	serializedFileConfig, err := fileStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize file store config: %v", err)
	}
	fileBlobStore, err := NewFileBlobStore(serializedFileConfig)
	if err != nil {
		t.Fatalf("failed to create new file blob store: %v", err)
	}
	azureStoreConfig := AzureBlobStoreConfig{
		AccountName: "featureformtesting",
		AccountKey:  os.Getenv("AZURE_ACCOUNT_KEY"),
		BucketName:  "testcontainer",
	}
	serializedAzureConfig, err := azureStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("dailed to serialize azure store config: %v", err)
	}
	azureBlobStore, err := NewAzureBlobStore(serializedAzureConfig)
	if err != nil {
		t.Fatalf("failed to create new azure blob store: %v", err)
	}

	blobProviders := map[string]BlobStore{
		"Local": localBlobStore,
		"File":  fileBlobStore,
		"Azure": azureBlobStore,
	}
	for testName, blobTest := range blobTests {
		blobTest = blobTest
		testName = testName
		for blobName, blobProvider := range blobProviders {
			blobName = blobName
			blobProvider = blobProvider
			t.Run(fmt.Sprintf("%s: %s", testName, blobName), func(t *testing.T) {
				blobTest(t, blobProvider)
			})
		}
	}
	for blobName, blobProvider := range blobProviders {
		fmt.Printf("Closing %s blob store\n", blobName)
		blobProvider.Close()
	}
}

func testBlobReadAndWrite(t *testing.T, store BlobStore) {
	testWrite := []byte("example data")
	testKey := uuidWithoutDashes()
	if err := store.Write(testKey, testWrite); err != nil {
		t.Fatalf("Failure writing data %s to key %s: %v", string(testWrite), testKey, err)
	}
	exists, err := store.Exists(testKey)
	if err != nil {
		t.Fatalf("Failure checking existence of key %s: %v", testKey, err)
	}
	if !exists {
		t.Fatalf("Test key %s does not exist: %v", testKey, err)
	}
	readData, err := store.Read(testKey)
	if err != nil {
		t.Fatalf("Could not read key %s from store: %v", testKey, err)
	}
	if string(readData) != string(testWrite) {
		t.Fatalf("Read data does not match written data: %s != %s", readData, testWrite)
	}
	if err := store.Delete(testKey); err != nil {
		t.Fatalf("Failed to delete test file with key %s: %v", testKey, err)
	}
}

func testBlobCSVServe(t *testing.T, store BlobStore) {
	//write csv file, then iterate all data types
	csvBytes := []byte(`1,2,3,4,5
	6,7,8,9,10`)
	testKey := fmt.Sprintf("%s.csv", uuidWithoutDashes())
	if err := store.Write(testKey, csvBytes); err != nil {
		t.Fatalf("Failure writing csv data %s to key %s: %v", string(csvBytes), testKey, err)
	}
	iterator, err := store.Serve(testKey)
	if err != nil {
		t.Fatalf("Failure getting serving iterator with key %s: %v", testKey, err)
	}
	for row, err := iterator.Next(); err != nil; row, err = iterator.Next() {
		fmt.Println(row)
	}
	fmt.Println(err)
}

func testBlobParquetServe(t *testing.T, store BlobStore) {
	testKey := fmt.Sprintf("input/transactions.snappy.parquet")
	iterator, err := store.Serve(testKey)
	if err != nil {
		t.Fatalf("Failure getting serving iterator with key %s: %v", testKey, err)
	}
	for row, err := iterator.Next(); err == nil; row, err = iterator.Next() {
		if err != nil {
			break
		}
		fmt.Printf("%v, %T\n", reflect.ValueOf(row), row)
	}

}

func TestExecutorRunLocal(t *testing.T) {
	localConfig := LocalExecutorConfig{
		ScriptPath: "./scripts/k8s/offline_store_pandas_runner.py",
	}
	serialized, err := localConfig.Serialize()
	if err != nil {
		t.Fatalf("Error serializing local executor configuration: %v", err)
	}
	executor, err := NewLocalExecutor(serialized)
	if err != nil {
		t.Fatalf("Error creating new Local Executor: %v", err)
	}
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}
	fmt.Println(mydir)
	sqlEnvVars := map[string]string{
		"MODE":                "local",
		"OUTPUT_URI":          fmt.Sprintf(`%s/scripts/k8s/tests/test_files/output/`, mydir),
		"SOURCES":             fmt.Sprintf("%s/scripts/k8s/tests/test_files/inputs/transaction_short/part-00000-9d3cb5a3-4b9c-4109-afa3-a75759bfcf89-c000.snappy.parquet", mydir),
		"TRANSFORMATION_TYPE": "sql",
		"TRANSFORMATION":      "SELECT * FROM source_0 LIMIT 1",
	}
	if err := executor.ExecuteScript(sqlEnvVars); err != nil {
		t.Fatalf("Failed to execute pandas script: %v", err)
	}
}

func TestOfflineStoreBasic(t *testing.T) {
	localConfig := LocalExecutorConfig{
		ScriptPath: "./scripts/k8s/offline_store_pandas_runner.py",
	}
	serializedExecutorConfig, err := localConfig.Serialize()
	if err != nil {
		t.Fatalf("Error serializing local executor configuration: %v", err)
	}
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}
	fileStoreConfig := FileBlobStoreConfig{DirPath: fmt.Sprintf(`file:////%s/scripts/k8s/tests/test_files/`, mydir)}
	serializedFileConfig, err := fileStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize file store config: %v", err)
	}
	k8sConfig := K8sConfig{
		ExecutorType:   GoProc,
		ExecutorConfig: ExecutorConfig(serializedExecutorConfig),
		StoreType:      FileSystem,
		StoreConfig:    BlobStoreConfig(serializedFileConfig),
	}
	serializedK8sConfig, err := k8sConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize k8s config: %v", err)
	}
	provider, err := k8sOfflineStoreFactory(serializedK8sConfig)
	if err != nil {
		t.Fatalf("failed to create new k8s offline store: %v", err)
	}
	offlineStore, err := provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("failed to convert store to offline store: %v", err)
	}

	// Register Primary
	fmt.Println("Registering primary table")
	primaryTableName := uuidWithoutDashes()
	primaryID := ResourceID{Name: primaryTableName, Variant: "default", Type: Primary}
	transactionsURI := "inputs/transaction_short/part-00000-9d3cb5a3-4b9c-4109-afa3-a75759bfcf89-c000.snappy.parquet"
	primaryTable, err := offlineStore.RegisterPrimaryFromSourceTable(primaryID, transactionsURI)
	if err != nil {
		t.Fatalf("failed to register primary table: %v", err)
	}
	fmt.Println(primaryTable.GetName())
	// Getting Primary
	fmt.Println("Getting primary table")
	fetchedPrimary, err := offlineStore.GetPrimaryTable(primaryID)
	if err != nil {
		t.Fatalf("failed to fetch primary table: %v", err)
	}
	fmt.Println(fetchedPrimary)
	transformationName := uuidWithoutDashes()
	transformationID := ResourceID{
		Name:    transformationName,
		Type:    Transformation,
		Variant: "default",
	}
	transformConfig := TransformationConfig{
		Type:          SQLTransformation,
		TargetTableID: transformationID,
		Query:         fmt.Sprintf("SELECT * FROM {{%s.default}}", primaryTableName),
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: fmt.Sprintf("{{%s.default}}", primaryTableName),
				Source:   fmt.Sprintf("featureform_primary__%s__default", primaryTableName),
			},
		},
	}
	if err := offlineStore.CreateTransformation(transformConfig); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}
	// Get transformation
	tsTable, err := offlineStore.GetTransformationTable(transformationID)
	if err != nil {
		t.Fatalf("could not fetch transformation table: %v", err)
	}
	fmt.Println(tsTable)
	firstResID := ResourceID{Name: uuidWithoutDashes(), Variant: "default", Type: Feature}
	schema := ResourceSchema{"CustomerID", "CustAccountBalance", "Timestamp", transactionsURI}
	firstResTable, err := offlineStore.RegisterResourceFromSourceTable(firstResID, schema)
	if err != nil {
		t.Fatalf("failed to register resource from source table: %v", err)
	}
	fmt.Println(firstResTable)
	fetchedFirstResTable, err := offlineStore.GetResourceTable(firstResID)
	if err != nil {
		t.Fatalf("failed to fetch resource table: %v", err)
	}
	fmt.Println(fetchedFirstResTable)

	secondResID := ResourceID{Name: uuidWithoutDashes(), Variant: "default", Type: Label}
	secondSchema := ResourceSchema{"CustomerID", "IsFraud", "Timestamp", transactionsURI}
	secondResTable, err := offlineStore.RegisterResourceFromSourceTable(secondResID, secondSchema)
	if err != nil {
		t.Fatalf("failed to register resource from source table: %v", err)
	}
	fmt.Println(secondResTable)

	trainingSetID := ResourceID{Name: uuidWithoutDashes(), Variant: "default", Type: TrainingSet}
	testTrainingSet := TrainingSetDef{
		ID:       trainingSetID,
		Label:    secondResID,
		Features: []ResourceID{firstResID},
	}
	if err := offlineStore.CreateTrainingSet(testTrainingSet); err != nil {
		t.Fatalf("failed to create trainingset: %v", err)
	}

	fmt.Println("fetching training set")
	ts, err := offlineStore.GetTrainingSet(trainingSetID)
	if err != nil {
		t.Fatalf("failed to fetch training set: %v", err)
	}
	fmt.Println(ts)
	for ts.Next() {
		fmt.Println(ts.Features())
		fmt.Println(ts.Label())
	}
	materialization, err := offlineStore.CreateMaterialization(firstResID)
	if err != nil {
		t.Fatalf("failed to create materialization: %v", err)
	}
	fmt.Println(materialization)
	fmt.Println(materialization.NumRows())

}
