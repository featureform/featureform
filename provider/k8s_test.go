//go:build k8s
// +build k8s

package provider

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/featureform/helpers"

	"github.com/google/uuid"
)

func uuidWithoutDashes() string {
	return fmt.Sprintf("a%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func TestBlobInterfaces(t *testing.T) {
	blobTests := map[string]func(*testing.T, BlobStore){
		"Test Blob Read and Write": testBlobReadAndWrite,
		// "Test Blob Parquet Serve":  testBlobParquetServe,
	}
	localBlobStore, err := NewMemoryBlobStore(Config([]byte("")))
	if err != nil {
		t.Fatalf("Failed to create memory blob store")
	}
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}

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
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
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
	for _, blobProvider := range blobProviders {
		blobProvider.Close()
	}
}

func testBlobReadAndWrite(t *testing.T, store BlobStore) {
	testWrite := []byte("example data")
	testKey := uuidWithoutDashes()
	exists, err := store.Exists(testKey)
	if exists {
		t.Fatalf("Exists when not yet written")
	}
	if err := store.Write(testKey, testWrite); err != nil {
		t.Fatalf("Failure writing data %s to key %s: %v", string(testWrite), testKey, err)
	}
	exists, err = store.Exists(testKey)
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

func testBlobParquetServe(t *testing.T, store BlobStore) {
	testKey := fmt.Sprintf("input/transactions.snappy.parquet")
	iterator, err := store.Serve(testKey)
	if err != nil {
		t.Fatalf("Failure getting serving iterator with key %s: %v", testKey, err)
	}
	for _, err := iterator.Next(); err == nil; _, err = iterator.Next() {
		if err != nil {
			break
		}
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
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}
	localConfig := LocalExecutorConfig{
		ScriptPath: "./scripts/k8s/offline_store_pandas_runner.py",
	}
	serializedExecutorConfig, err := localConfig.Serialize()
	if err != nil {
		t.Fatalf("Error serializing local executor configuration: %v", err)
	}

	azureStoreConfig := AzureBlobStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
	}
	serializedAzureConfig, err := azureStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("dailed to serialize azure store config: %v", err)
	}
	// fileStoreConfig := FileBlobStoreConfig{DirPath: fmt.Sprintf(`file:////%s/tests/file_tests`, mydir)}
	// serializedFileConfig, err := fileStoreConfig.Serialize()
	// if err != nil {
	// 	t.Fatalf("failed to serialize file store config: %v", err)
	// }
	k8sConfig := K8sConfig{
		ExecutorType:   GoProc,
		ExecutorConfig: ExecutorConfig(serializedExecutorConfig),
		StoreType:      Azure,
		StoreConfig:    BlobStoreConfig(serializedAzureConfig),
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
	primaryTableName := uuidWithoutDashes()
	primaryID := ResourceID{Name: primaryTableName, Variant: "default", Type: Primary}
	transactionsURI := "featureform/testing/primary/name/variant/transactions_short.csv"
	_, err = offlineStore.RegisterPrimaryFromSourceTable(primaryID, transactionsURI)
	if err != nil {
		t.Fatalf("failed to register primary table: %v", err)
	}

	// Getting Primary
	_, err = offlineStore.GetPrimaryTable(primaryID)
	if err != nil {
		t.Fatalf("failed to fetch primary table: %v", err)
	}
	// create transformation
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
	_, err = offlineStore.GetTransformationTable(transformationID)
	if err != nil {
		t.Fatalf("could not fetch transformation table: %v", err)
	}

	f, err := os.Open(fmt.Sprintf("%s/scripts/k8s/.featureform/transformation.pkl", mydir))
	pkl_data := make([]byte, 1000)
	_, err = f.Read(pkl_data)
	//dataframe transformation
	dfTransformationName := uuidWithoutDashes()
	dfTransformationID := ResourceID{
		Name:    dfTransformationName,
		Type:    Transformation,
		Variant: "default",
	}
	dfTransformConfig := TransformationConfig{
		Type:          DFTransformation,
		TargetTableID: dfTransformationID,
		Code:          pkl_data,
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: "transaction",
				Source:   fmt.Sprintf("featureform_primary__%s__default", primaryTableName),
			},
		},
	}
	if err := offlineStore.CreateTransformation(dfTransformConfig); err != nil {
		t.Fatalf("Could not create df transformation: %v", err)
	}

	// Get df transformation
	df_table, err := offlineStore.GetTransformationTable(dfTransformationID)
	if err != nil {
		t.Fatalf("could not fetch df transformation table: %v", err)
	}
	fmt.Println(df_table.NumRows())

	segmentIterator, err := df_table.IterateSegment(10)
	if err != nil {
		t.Fatalf("could not create table iterator: %v", err)
	}
	fmt.Println(segmentIterator.Columns())
	for segmentIterator.Next() {
		fmt.Println(segmentIterator.Values())
	}

	firstResID := ResourceID{Name: uuidWithoutDashes(), Variant: "default", Type: Feature}
	schema := ResourceSchema{Entity: "CustomerID", Value: "CustAccountBalance", SourceTable: transactionsURI}
	_, err = offlineStore.RegisterResourceFromSourceTable(firstResID, schema)
	if err != nil {
		t.Fatalf("failed to register resource from source table: %v", err)
	}

	_, err = offlineStore.GetResourceTable(firstResID)
	if err != nil {
		t.Fatalf("failed to fetch resource table: %v", err)
	}

	secondResID := ResourceID{Name: uuidWithoutDashes(), Variant: "default", Type: Label}
	secondSchema := ResourceSchema{Entity: "CustomerID", Value: "IsFraud", TS: "Timestamp", SourceTable: transactionsURI}
	_, err = offlineStore.RegisterResourceFromSourceTable(secondResID, secondSchema)
	if err != nil {
		t.Fatalf("failed to register resource from source table: %v", err)
	}

	trainingSetID := ResourceID{Name: uuidWithoutDashes(), Variant: "default", Type: TrainingSet}
	testTrainingSet := TrainingSetDef{
		ID:       trainingSetID,
		Label:    secondResID,
		Features: []ResourceID{firstResID},
	}
	if err = offlineStore.CreateTrainingSet(testTrainingSet); err != nil {
		t.Fatalf("failed to create trainingset: %v", err)
	}

	_, err = offlineStore.GetTrainingSet(trainingSetID)
	if err != nil {
		t.Fatalf("failed to fetch training set: %v", err)
	}

	materialization, err := offlineStore.CreateMaterialization(firstResID)
	if err != nil {
		t.Fatalf("failed to create materialization: %v", err)
	}
	featureIterator, err := materialization.IterateSegment(0, 100)
	if err != nil {
		t.Fatalf("could not create materialization iterator")
	}
	i := 0
	for featureIterator.Next() {
		i += 1
	}
	if i != 100 {
		t.Fatalf("incorrect number of rows iterated over")
	}

}
func TestNewConfig(t *testing.T) {
	k8sConfig := K8sAzureConfig{
		ExecutorType:   K8s,
		ExecutorConfig: KubernetesExecutorConfig{},
		StoreType:      Azure,
		StoreConfig: AzureBlobStoreConfig{
			AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
			AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
			ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
			Path:          "",
		},
	}
	serialized, err := k8sConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize: %v", err)
	}
	provider, err := k8sAzureOfflineStoreFactory(SerializedConfig(serialized))
	if err != nil {
		t.Fatalf("could not get provider")
	}
	offlineStore, err := provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("failed to convert store to offline store: %v", err)
	}
	fmt.Println(offlineStore)
}
