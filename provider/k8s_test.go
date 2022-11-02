//go:build k8s
// +build k8s

package provider

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/featureform/helpers"

	"github.com/google/uuid"

	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/parquet-go"
)

func uuidWithoutDashes() string {
	return fmt.Sprintf("a%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func TestBlobInterfaces(t *testing.T) {
	fileStoreTests := map[string]func(*testing.T, FileStore){
		"Test Filestore Read and Write": testFilestoreReadAndWrite,
		"Test Blob Parquet Serve":       testBlobParquetServe,
		"Test Exists":                   testExists,
		"Test Not Exists":               testNotExists,
		"Test Serve":                    testServe,
		"Test Serve Directory":          testServeDirectory,
		"Test Delete":                   testDelete,
		"Test Delete All":               testDeleteAll,
		"Test Newest file":              testNewestFile,
		"Test Path with prefix":         testPathWithPrefix,
		"Test Num Rows":                 testNumRows,
	}
	blobTests := map[string]func(*testing.T, BlobStore){
		"Test Blob Read and Write": testBlobReadAndWrite,
		// "Test Blob Parquet Serve":  testBlobParquetServe,
	}
	if err != nil {
		t.Fatalf("Failed to create memory blob store")
	}
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}

	fileStoreConfig := FileFileStoreConfig{DirPath: fmt.Sprintf(`file:////%s/tests/file_tests`, mydir)}
	serializedFileConfig, err := fileStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize file store config: %v", err)
	}
	fileFileStore, err := NewFileFileStore(serializedFileConfig)
	if err != nil {
		t.Fatalf("failed to create new file blob store: %v", err)
	}
	azureStoreConfig := AzureFileStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
	}
	serializedAzureConfig, err := azureStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize azure store config: %v", err)
	}
	azureFileStore, err := NewAzureFileStore(serializedAzureConfig)
	if err != nil {
		t.Fatalf("failed to create new azure blob store: %v", err)
	}

	blobProviders := map[string]FileStore{
		"Local": localFileStore,
		"File":  fileFileStore,
		"Azure": azureFileStore,
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

func testBlobReadAndWrite(t *testing.T, store FileStore) {
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

func testBlobParquetServe(t *testing.T, store FileStore) {
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

	azureStoreConfig := AzureFileStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
	}
	serializedAzureConfig, err := azureStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("dailed to serialize azure store config: %v", err)
	}
	// fileStoreConfig := FileFileStoreConfig{DirPath: fmt.Sprintf(`file:////%s/tests/file_tests`, mydir)}
	// serializedFileConfig, err := fileStoreConfig.Serialize()
	// if err != nil {
	// 	t.Fatalf("failed to serialize file store config: %v", err)
	// }
	k8sConfig := K8sConfig{
		ExecutorType:   GoProc,
		ExecutorConfig: ExecutorConfig(serializedExecutorConfig),
		StoreType:      Azure,
		StoreConfig:    FileStoreConfig(serializedAzureConfig),
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
		StoreConfig: AzureFileStoreConfig{
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

func Test_parquetIteratorFromReader(t *testing.T) {
	rows := 1000000
	type RowType struct {
		Index  int
		SIndex string
	}

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf)
	var testRows []RowType
	for i := 1; i < rows; i++ {
		row := RowType{
			i,
			fmt.Sprintf("%d", i),
		}
		testRows = append(testRows, row)
		w.Write(row)
	}
	w.Close()
	iter, err := parquetIteratorFromReader(buf.Bytes())
	if err != nil {
		t.Fatalf(err.Error())
	}
	index := 0
	for {
		value, err := iter.Next()
		if err != nil {
			fmt.Println(err)
			break
		} else if value == nil && err == nil {
			break
		}
		var result RowType
		mapstructure.Decode(value, &result)
		if result != testRows[index] {
			t.Errorf("Rows not equal %v!=%v\n", value, testRows[index])
		}
		index += 1
	}
}

func testExists(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	randomData := uuid.New().String()
	if err := store.Write(randomKey, randomData); err != nil {
		t.Fatalf("Could not write key to filestore: %v", err)
	}
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if !exists {
		t.Fatalf("Key written to file store does not exist")
	}
	// cleanup test
	if store.Delete(randomKey); err != nil {
		t.Fatalf("error deleting random key: %v", err)
	}
}

func testNotExists(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if exists {
		t.Fatalf("Key not written to file store exists")
	}
}

func TestConvertToParquetBytes(t *testing.T) {
	parquetNumRows := int64(5)
	randomStructs := randomStructList(parquetNumRows)
	parquetBytes, err := convertToParquetBytes(randomStructs)
	if err != nil {
		t.Fatalf("could not convert struct list to parquet bytes: %v", err)
	}
	file := bytes.NewReader(parquetBytes)
	r := parquet.NewReader(file)
	value := make(map[string]interface{})
	//map will order the "columns" in alphabetical order
	for {
		err = r.Read(&value)
		if err != nil {
			if err == io.EOF {
				fmt.Println("end of file")
			} else {
				panic(err)
			}
		}
	}

}

func randomStructList(length int64) []any {
	type PersonEntry struct {
		ID         int64
		Name       string
		Points     float32
		Score      float64
		Registered bool
		Age        int
		Created    time.Time
	}
	personList := make([]any, length)
	for i := int64(0); i < length; i++ {
		personList[i] = PersonEntry{
			ID:         i,
			Name:       uuid.New().String(),
			Points:     float32(i + 0.1),
			Score:      float64(i + 0.0),
			Registered: false,
			Age:        int(i),
			Created:    time.Now(),
		}
	}
	return personList
}

func compareStructWithInterface(compareStruct any, compareInterface map[string]interface{}) (bool, error) {

}

func testServe(t *testing.T, store FileStore) {
	parquetNumRows := int64(5)
	randomStructs := randomStructList(parquetNumRows)
	parquetBytes, err := convertToParquetBytes(randomStructs)
	if err != nil {
		t.Fatalf("could not convert struct list to parquet bytes: %v", err)
	}
	randomkey := uuid.New().String()
	if err := store.Write(randomKey, parquetBytes); err != nil {
		t.Fatalf("Could not write parquet bytes to random key: %v", err)
	}
	iterator, err := store.Serve(randomKey)
	if err != nil {
		t.Fatalf("Could not get parquet iterator: %v", err)
	}
	idx := int64(0)
	for {
		parquetRow, err := iterator.Next()
		idx += 1
		if err != nil && err != io.EOF {
			t.Fatalf("Error iterating through parquet file: %v", err)
		} else if err == io.EOF {
			if idx != parquetNumRows {
				t.Fatalf("Incorrect number of rows in parquet file")
			}
		}
		identical, err := compareStructWithInterface(randomStructs[idx], parquetRow)
		if err != nil {
			t.Fatalf("Error comparing struct with interface: %v", err)
		}
		if !identical {
			t.Fatalf("Submitted row and returned struct not identical. Got %v, expected %v", parquetRow, randomStruct[idx])
		}
	}
	// TODO cleanup test
	if err := store.Delete(randomKey); err != nil {
		t.Fatalf("Could not delete parquet file: %v", err)
	}
}

func testServeDirectory(t *testing.T, store FileStore) {
	parquetNumRows := int64(5)
	parquetNumFiles := int65(5)
	randomDirectory := uuid.New().String()
	randomStructs := make([]any, parquetNumFiles)
	for i := int64(0); i < parquetNumFiles; i++ {
		randomStructs[i] = randomStructList(parquetNumRows)
		parquetBytes, err := convertToParquetBytes(randomStructs[i])
		if err != nil {
			t.Fatalf("error converting struct to parquet bytes")
		}
		randomKey := fmt.Sprintf("part000%d%s", i, uuid.New().String())
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKey)
		if err := store.Write(randomPath, parquetBytes); err != nil {
			t.Fatalf("Could not write parquet bytes to path: %v", err)
		}
	}
	storeGenericStore, ok := store.(genericFileStore)
	if !ok {
		t.Fatalf("Could not convert parquet store to generic file store")
	}
	iterator, err := storeGenericStore.ServeDirectory(randomDirectory)
	if err != nil {
		t.Fatalf("Could not get parquet iterator: %v", err)
	}
	totalRows := int64(parquetNumFiles * parquetNumRows)
	idx := int64(0)
	for {
		parquetRow, err := iterator.Next()
		idx += 1
		if err != nil && err != io.EOF {
			t.Fatalf("Error iterating through parquet file: %v", err)
		} else if err == io.EOF {
			if idx != totalRows {
				t.Fatalf("Incorrect number of rows in parquet file")
			}
		}
		numFile := int(idx / 5)
		numRow := idx % 5
		identical, err := compareStructWithInterface(randomStructs[numFile][numRow], parquetRow)
		if err != nil {
			t.Fatalf("Error comparing struct with interface: %v", err)
		}
		if !identical {
			t.Fatalf("Submitted row and returned struct not identical. Got %v, expected %v", parquetRow, randomStruct[idx])
		}
	}
	// TODO cleanup test
	if err := store.DeleteAll(randomRirectory); err != nil {
		t.Fatalf("Could not delete parquet directory: %v", err)
	}
}

func testDelete(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	randomData := uuid.New().String()
	if err := store.Write(randomKey, randomData); err != nil {
		t.Fatalf("Could not write key to filestore: %v", err)
	}
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if !exists {
		t.Fatalf("Key written to file store does not exist")
	}
	if err := store.Delete(randomKey); err != nil {
		t.Fatalf("Could not delete key from filestore: %v")
	}
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if exists {
		t.Fatalf("Key deleted from file store exists")
	}

}

func testDeleteAll(t *testing.T, store FileStore) {
	randomListLength := 5
	randomDirectory := uuid.New().String()
	randomKeyList := make([]string, randomListLength)
	for i := 0; i < randomListLength; i++ {
		randomKeyList[i] = uuid.New().String()
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		randomData := uuid.New().String()
		if err := store.Write(randomPath, randomData); err != nil {
			t.Fatalf("Could not write key to filestore: %v", err)
		}
	}
	for i := 0; i < randomListLength; i++ {
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		exists, err := store.Exists(randomPath)
		if err != nil {
			t.Fatalf("Could not check that key exists in filestore: %v", err)
		}
		if !exists {
			t.Fatalf("Key written to file store does not exist")
		}
	}
	if err := store.DeleteAll(randomDirectory); err != nil {
		t.Fatalf("Could not delete directory: %v", err)
	}
	for i := 0; i < randomListLength; i++ {
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		exists, err := store.Exists(randomPath)
		if err != nil {
			t.Fatalf("Could not check that key exists in filestore: %v", err)
		}
		if exists {
			t.Fatalf("Key deleted from filestore does exist")
		}
	}

}

func testNewestFile(t *testing.T, store FileStore) {
	// write a bunch of blobs with different timestamps
	randomListLength := 5
	randomDirectory := uuid.New().String()
	randomKeyList := make([]string, randomListLength)
	for i := 0; i < randomListLength; i++ {
		randomKeyList[i] = uuid.New().String()
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		randomData := uuid.New().String()
		if err := store.Write(randomPath, randomData); err != nil {
			t.Fatalf("Could not write key to filestore: %v", err)
		}
	}
	newestFile, err := store.NewestFile(randomDirectory)
	if err != nil {
		t.Fatalf("Error getting newest file from directory: %v", err)
	}
	expectedNewestFile := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[randomListLength-1])
	if newestFile != expectedNewestFile {
		t.Fatalf("Newest file did not retrieve actual newest file. Expected %s, got %s", expectedNewestFile, newestFile)
	}
	// cleanup test
	if err := store.DeleteAll(randomDirectory); err != nil {
		t.Fatalf("Could not delete directory: %v", err)
	}
}

// func testPathWithPrefix(t *testing.T, store FileStore) {
//TODO implement
// 	//test the path with prefix?????
// 	//test these I guess?
// 	// func (store genericFileStore) PathWithPrefix(path string) string {
// 	// 	if len(store.path) > 4 && store.path[0:4] == "file" {
// 	// 		return fmt.Sprintf("%s%s", store.path[len("file:////"):], path)
// 	// 	} else {
// 	// 		return path
// 	// 	}
// 	// }

// 	// func (store AzureFileStore) PathWithPrefix(path string) string {
// 	// 	if len(path) != 0 && path[0:len(store.Path)] != store.Path && store.Path != "" {
// 	// 		return fmt.Sprintf("%s/%s", store.Path, path)
// 	// 	}
// 	// 	return path
// 	// }
// }

func testNumRows(t *testing.T, store FileStore) {
	parquetNumRows := int64(5)
	randomStructList := randomStructList(parquetNumRows)
	parquetBytes, err := convertToParquetBytes(randomStructList)
	randomParquetPath := uuid.New().String()
	if err != nil {
		t.Fatalf("Could not convert struct list to parquet bytes: %v", err)
	}
	if err := store.Write(randomParquetPath, parquetBytes); err != nil {
		t.Fatalf("Could not write parquet bytes to path: %v")
	}
	numRows, err := store.NumRows(randomParquetPath)
	if err != nil {
		t.Fatalf("Could not get num rows from parquet file")
	}
	if numRows != parquetNumRows {
		t.Fatalf("Incorrect retrieved num rows from parquet file. Expected %d, got %d", numRows, parquetNumRows)
	}
	// cleanup test
	if err := store.Delete(randomParquetPath); err != nil {
		t.Fatalf("Could not delete parquet file: %v", err)
	}
}
