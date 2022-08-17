//go:build offline
// +build offline

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func testMaterializeResource(store *SparkOfflineStore) error {
	exampleStructArray := make([]exampleStruct, 10)
	for i := 0; i < 5; i++ {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: time.UnixMilli(int64(i)),
		}
	}
	for i := 5; i < 10; i++ {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i-5),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     true,
			Registered: time.UnixMilli(int64(i)),
		}
	}
	path := "featureform/tests/testFile2.parquet"
	if err := store.Store.UploadParquetTable(path, exampleStructArray); err != nil {
		return err
	}
	testResourceName := "test_name_materialize"
	testResourceVariant := "test_variant"
	testResource := ResourceID{testResourceName, testResourceVariant, Feature}
	testResourceSchema := ResourceSchema{"name", "age", "registered", path}
	table, err := store.RegisterResourceFromSourceTable(testResource, testResourceSchema)
	if err != nil {
		return err
	}
	fetchedTable, err := store.GetResourceTable(testResource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fetchedTable, table) {
		return fmt.Errorf("Did not properly register table")
	}
	testResourceMaterializationID := MaterializationID(fmt.Sprintf("%s/%s/%s", FeatureMaterialization, testResourceName, testResourceVariant))
	materialization, err := store.CreateMaterialization(testResource)
	if err != nil {
		return err
	}
	fetchedMaterialization, err := store.GetMaterialization(testResourceMaterializationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fetchedMaterialization, materialization) {
		return fmt.Errorf("get materialization and create materialization return different results")
	}
	correctMaterialization := map[string]ResourceRecord{
		"John Smith_0": ResourceRecord{"John Smith_0", reflect.ValueOf(35).Interface(), time.UnixMilli(int64(5))},
		"John Smith_1": ResourceRecord{"John Smith_1", reflect.ValueOf(36).Interface(), time.UnixMilli(int64(6))},
		"John Smith_2": ResourceRecord{"John Smith_2", reflect.ValueOf(37).Interface(), time.UnixMilli(int64(7))},
		"John Smith_3": ResourceRecord{"John Smith_3", reflect.ValueOf(38).Interface(), time.UnixMilli(int64(8))},
		"John Smith_4": ResourceRecord{"John Smith_4", reflect.ValueOf(39).Interface(), time.UnixMilli(int64(9))},
	}
	if fetchedMaterialization.ID() != "Materialization/test_name_materialize/test_variant" {
		return fmt.Errorf("materialization id not correct, expected Materialization/test_name_materialize/test_variant, got %s", fetchedMaterialization.ID())
	}
	numRows, err := fetchedMaterialization.NumRows()
	if err != nil {
		return err
	}
	if numRows != 5 {
		return fmt.Errorf("Num rows not correct, expected 5, got %d", numRows)
	}
	numRowsFirst := int64(2)
	iterator, err := fetchedMaterialization.IterateSegment(0, numRowsFirst)
	if err != nil {
		return err
	}
	comparisonList := make([]ResourceRecord, 0, 5)
	iterations := int64(0)
	for iterator.Next() {
		if iterator.Err() == nil {
			comparisonList = append(comparisonList, iterator.Value())
		}
		iterations += 1
	}
	if iterations != numRowsFirst {
		return fmt.Errorf("Feature iterator had wrong number of iterations. Expected %d, got %d", numRowsFirst, iterations)
	}
	numRowsSecond := int64(3)
	nextIterator, err := fetchedMaterialization.IterateSegment(numRowsFirst, numRowsFirst+numRowsSecond)
	if err != nil {
		return err
	}
	iterations = 0
	for nextIterator.Next() {
		if nextIterator.Err() == nil {
			comparisonList = append(comparisonList, nextIterator.Value())
		}
		iterations += 1
	}
	if iterations != numRowsSecond {
		return fmt.Errorf("Feature iterator had wrong number of iterations. Expected %d, got %d", numRowsSecond, iterations)
	}
	for _, rec := range comparisonList {
		val, err := strconv.Atoi(rec.Value.(string))
		if err != nil {
			return err
		}
		rec.Value = val
		if !reflect.DeepEqual(rec, correctMaterialization[rec.Entity]) {
			return fmt.Errorf("Wrong materialization entry: %T does not equal %T", rec.Value, correctMaterialization[rec.Entity].Value)
		}
	}
	return nil
}

func testResourcePath(store *SparkOfflineStore) error {
	bucketName := os.Getenv("S3_BUCKET_PATH")
	exampleResource := ResourceID{"test_resource", "test_variant", Primary}
	expectedPath := fmt.Sprintf("s3://%s/featureform/Primary/test_resource/test_variant/", bucketName)
	resultPath := store.Store.ResourcePath(exampleResource)
	if expectedPath != resultPath {
		return fmt.Errorf("%s does not equal %s", expectedPath, resultPath)
	}
	return nil
}

func testTableUploadCompare(store *SparkOfflineStore) error {
	testTable := "featureform/tests/testFile.parquet"
	testData := make([]ResourceRecord, 10)
	for i := range testData {
		testData[i].Entity = "a"
		testData[i].Value = i
		testData[i].TS = time.Now()
	}
	exists, err := store.Store.FileExists(testTable)
	if err != nil {
		return err
	}
	if exists {
		if err := store.Store.DeleteFile(testTable); err != nil {
			return err
		}
	}
	if err := store.Store.UploadParquetTable(testTable, testData); err != nil {
		return err
	}
	if err := store.Store.CompareParquetTable(testTable, testData); err != nil {
		return err
	}
	if err := store.Store.DeleteFile(testTable); err != nil {
		return err
	}
	exists, err = store.Store.FileExists(testTable)
	if err != nil {
		return err
	}
	if exists {
		return err
	}
	return nil
}

type exampleStruct struct {
	Name       string
	Age        int
	Score      float32
	Winner     bool
	Registered time.Time
}

func testRegisterResource(store *SparkOfflineStore) error {
	exampleStructArray := make([]exampleStruct, 5)
	for i := range exampleStructArray {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: time.UnixMilli(int64(i)),
		}
	}
	path := "featureform/tests/testFile.parquet"
	if err := store.Store.UploadParquetTable(path, exampleStructArray); err != nil {
		return err
	}
	testResource := ResourceID{"test_name", "test_variant", Feature}
	testResourceSchema := ResourceSchema{"name", "age", "registered", path}
	table, err := store.RegisterResourceFromSourceTable(testResource, testResourceSchema)
	if err != nil {
		return err
	}
	fetchedTable, err := store.GetResourceTable(testResource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fetchedTable, table) {
		return fmt.Errorf("Did not properly register table")
	}
	return nil
}

func unorderedEqual(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	exists := make(map[string]bool)
	for _, value := range first {
		exists[value] = true
	}
	for _, value := range second {
		if !exists[value] {
			return false
		}
	}
	return true
}

func testRegisterPrimary(store *SparkOfflineStore) error {
	exampleStructArray := make([]exampleStruct, 5)
	for i := range exampleStructArray {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: time.UnixMilli(int64(i)),
		}
	}

	path := "featureform/testprimary/testFile.parquet"
	if err := store.Store.UploadParquetTable(path, exampleStructArray); err != nil {
		return err
	}
	testResource := ResourceID{"test_name", "test_variant", Primary}
	table, err := store.RegisterPrimaryFromSourceTable(testResource, path)
	if err != nil {
		return err
	}
	fetchedTable, err := store.GetPrimaryTable(testResource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(table, fetchedTable) {
		return fmt.Errorf("Tables not equal")
	}
	numRows, err := fetchedTable.NumRows()
	if err != nil {
		return err
	}
	if numRows != 5 {
		return fmt.Errorf("Did not fetch the correct number of rows")
	}
	iterator, err := fetchedTable.IterateSegment(5)
	if err != nil {
		return err
	}
	expectedColumns := []string{"name", "age", "score", "winner", "registered"}

	if !unorderedEqual(iterator.Columns(), expectedColumns) {
		return fmt.Errorf("Not the correct columns returned")
	}
	idx := 0
	for iterator.Next() {
		jsonString := reflect.ValueOf(iterator.Values()).Index(0).Interface()
		var jsonMap map[string]interface{}
		json.Unmarshal([]byte(jsonString.(string)), &jsonMap)
		curStruct := reflect.ValueOf(exampleStructArray[idx])
		if curStruct.NumField() != 5 {
			return fmt.Errorf("incorrect number of fields")
		}
		idx += 1
	}
	if idx != 5 {
		return fmt.Errorf("incorrect number of rows written")
	}
	return nil
}

func TestParquetUpload(t *testing.T) {
	if testing.Short() {
		return
	}
	if err := RegisterFactory("SPARK_OFFLINE", SparkOfflineStoreFactory); err != nil {
		t.Fatalf("Could not register Spark factory: %s", err)
	}
	emrConf := EMRConfig{
		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
		ClusterRegion:  os.Getenv("AWS_EMR_CLUSTER_REGION"),
		ClusterName:    os.Getenv("AWS_EMR_CLUSTER_ID"),
	}
	emrSerializedConfig := emrConf.Serialize()
	s3Conf := S3Config{
		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
		BucketRegion:   os.Getenv("S3_BUCKET_REGION"),
		BucketPath:     os.Getenv("S3_BUCKET_PATH"),
	}
	s3SerializedConfig := s3Conf.Serialize()
	SparkOfflineConfig := SparkConfig{
		ExecutorType:   EMR,
		ExecutorConfig: string(emrSerializedConfig),
		StoreType:      S3,
		StoreConfig:    string(s3SerializedConfig),
	}
	sparkSerializedConfig := SparkOfflineConfig.Serialize()
	sparkProvider, err := Get("SPARK_OFFLINE", sparkSerializedConfig)
	if err != nil {
		t.Fatalf("Could not create spark provider: %s", err)
	}
	sparkStore, err := sparkProvider.AsOfflineStore()
	if err != nil {
		t.Fatalf("Could not convert spark provider to offline store: %s", err)
	}
	sparkOfflineStore := sparkStore.(*SparkOfflineStore)
	if err := testTableUploadCompare(sparkOfflineStore); err != nil {
		t.Fatalf("Upload test failed: %s", err)
	}
	if err := testResourcePath(sparkOfflineStore); err != nil {
		t.Fatalf("resource path test failed: %s", err)
	}
	if err := testRegisterResource(sparkOfflineStore); err != nil {
		t.Fatalf("register resource test failed: %s", err)
	}
	if err := testRegisterPrimary(sparkOfflineStore); err != nil {
		t.Fatalf("resource primary test failed: %s", err)
	}
	if err := testMaterializeResource(sparkOfflineStore); err != nil {
		t.Fatalf("resource materialize test failed: %s", err)
	}
}

func TestStringifyValue(t *testing.T) {
	testValueMap := map[interface{}]string{
		"test":            `"test"`,
		10:                "10",
		float64(10.1):     "10.1",
		float32(10.1):     "10.1",
		int32(10):         "10",
		false:             "false",
		time.UnixMilli(0): "0",
	}

	for k, v := range testValueMap {
		if stringifyValue(k) != v {
			t.Fatalf("%v does not equal %v\n", stringifyValue(k), v)
		}
	}
}

func TestStringifyStruct(t *testing.T) {
	exampleInstance := exampleStruct{
		Name:       "John Smith",
		Age:        30,
		Score:      100.4,
		Winner:     false,
		Registered: time.UnixMilli(0),
	}
	desiredOutput := `
	{"name":"John Smith",
"age":30,
"score":100.4,
"winner":false,
"registered":0
}`
	stringStruct := stringifyStruct(exampleInstance)
	if desiredOutput != stringStruct {
		t.Fatalf("%s\nis not equal to %s\n", desiredOutput, stringStruct)
	}
}

func TestStringifyStructArray(t *testing.T) {
	exampleStructArray := make([]exampleStruct, 5)
	for i := range exampleStructArray {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: time.UnixMilli(int64(i)),
		}
	}
	desiredOutput := []string{
		`
	{"name":"John Smith_0",
"age":30,
"score":100.4,
"winner":false,
"registered":0
}`,
		`
	{"name":"John Smith_1",
"age":31,
"score":101.4,
"winner":false,
"registered":1
}`,
		`
	{"name":"John Smith_2",
"age":32,
"score":102.4,
"winner":false,
"registered":2
}`,
		`
	{"name":"John Smith_3",
"age":33,
"score":103.4,
"winner":false,
"registered":3
}`,
		`
	{"name":"John Smith_4",
"age":34,
"score":104.4,
"winner":false,
"registered":4
}`}
	stringArray, _ := stringifyStructArray(exampleStructArray)
	if !reflect.DeepEqual(stringArray, desiredOutput) {
		t.Fatalf("returned structs are not equal")
	}
}

func TestStringifyStructField(t *testing.T) {
	exampleInstance := exampleStruct{
		Name:       "John Smith",
		Age:        30,
		Score:      100.4,
		Winner:     false,
		Registered: time.UnixMilli(0),
	}
	desiredResults := []string{
		`{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}`,
		`{"Tag": "name=age, type=INT32"}`,
		`{"Tag": "name=score, type=FLOAT"}`,
		`{"Tag": "name=winner, type=BOOLEAN"}`,
		`{"Tag": "name=registered, type=INT64"}`,
	}

	reflectedStruct := reflect.ValueOf(exampleInstance)
	for i := 0; i < reflectedStruct.NumField(); i++ {
		resultField := stringifyStructField(exampleInstance, i)
		if resultField != desiredResults[i] {
			t.Fatalf("%s does not equal %s", resultField, desiredResults[i])
		}
	}
}

func TestGenerateSchemaFromInterface(t *testing.T) {
	exampleStructArray := make([]exampleStruct, 5)
	for i := range exampleStructArray {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: time.UnixMilli(int64(i)),
		}
	}
	schema, _ := generateSchemaFromInterface(exampleStructArray)
	desiredOutput := `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
                    {"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
                                        {"Tag": "name=age, type=INT32"},
                                        {"Tag": "name=score, type=FLOAT"},
                                        {"Tag": "name=winner, type=BOOLEAN"},
                                        {"Tag": "name=registered, type=INT64"}
                                ]
                        }`
	var jsonMap map[string]interface{}
	json.Unmarshal([]byte(desiredOutput), &jsonMap)
	var resultMap map[string]interface{}
	json.Unmarshal([]byte(schema), &resultMap)
	if !reflect.DeepEqual(jsonMap, resultMap) {
		t.Fatalf("Marshalled json schemas are not equal")
	}
}
