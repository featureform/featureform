//go:build spark
// +build spark

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

	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func testCreateTrainingSet(store *SparkOfflineStore) error {
	exampleStructArray := make([]exampleStruct, 5)
	for i := 0; i < 5; i++ {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: time.UnixMilli(int64(i)),
		}
	}
	correctMapping := map[string]string{
		"30": "false",
		"31": "false",
		"32": "false",
		"33": "false",
		"34": "false",
	}
	path := "featureform/tests/trainingSetTest.parquet"
	if err := store.Store.UploadParquetTable(path, exampleStructArray); err != nil {
		return err
	}
	testFeatureName := "test_feature"
	testFeatureResource := ResourceID{testFeatureName, "default", Feature}
	testResourceSchema := ResourceSchema{"name", "age", "registered", path}
	table, err := store.RegisterResourceFromSourceTable(testFeatureResource, testResourceSchema)
	if err != nil {
		return err
	}
	fetchedTable, err := store.GetResourceTable(testFeatureResource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fetchedTable, table) {
		return fmt.Errorf("Did not properly register table")
	}
	testLabelName := "test_label"
	testLabelResource := ResourceID{testLabelName, "default", Label}
	testLabelResourceSchema := ResourceSchema{"name", "winner", "registered", path}
	labelTable, err := store.RegisterResourceFromSourceTable(testLabelResource, testLabelResourceSchema)
	fetchedLabel, err := store.GetResourceTable(testLabelResource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fetchedLabel, labelTable) {
		return fmt.Errorf("Did not properly register label")
	}
	trainingSetResource := ResourceID{"test_training_set", "default", TrainingSet}
	testTrainingSetDef := TrainingSetDef{
		ID:       trainingSetResource,
		Label:    testLabelResource,
		Features: []ResourceID{testFeatureResource},
	}
	if err := store.CreateTrainingSet(testTrainingSetDef); err != nil {
		return fmt.Errorf("failed to create training set: %v", err)
	}
	fetchedTrainingSet, err := store.GetTrainingSet(trainingSetResource)
	if err != nil {
		return fmt.Errorf("failed to fetch training set: %v", err)
	}
	i := 0
	for fetchedTrainingSet.Next() {
		if fetchedTrainingSet.Err() != nil {
			return fmt.Errorf("failure while iterating over training set: %v", err)
		}
		features := fetchedTrainingSet.Features()
		label := fetchedTrainingSet.Label()
		if len(features) != 1 {
			return fmt.Errorf("incorrect number of feature entries")
		}
		if correctMapping[reflect.ValueOf(features[0]).Interface().(string)] != label {
			return fmt.Errorf("incorrect feature value")
		}
		i += 1
	}
	if i != 5 {
		return fmt.Errorf("incorrect number of training set rows")
	}
	return nil
}

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
		"John Smith_0": ResourceRecord{"John Smith_0", 35, time.UnixMilli(int64(5))},
		"John Smith_1": ResourceRecord{"John Smith_1", 36, time.UnixMilli(int64(6))},
		"John Smith_2": ResourceRecord{"John Smith_2", 37, time.UnixMilli(int64(7))},
		"John Smith_3": ResourceRecord{"John Smith_3", 38, time.UnixMilli(int64(8))},
		"John Smith_4": ResourceRecord{"John Smith_4", 39, time.UnixMilli(int64(9))},
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
	sparkOfflineStore, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

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
	if err := testCreateTrainingSet(sparkOfflineStore); err != nil {
		t.Fatalf("resource training set test failed: %s", err)
	}
}

func TestStringifyValue(t *testing.T) {
	type randomStruct struct{}
	testValueMap := map[interface{}]string{
		"test":            `"test"`,
		10:                "10",
		1.1:               "1.1",
		float64(10.1):     "10.1",
		float32(10.1):     "10.1",
		int32(10):         "10",
		false:             "false",
		time.UnixMilli(0): "0",
		randomStruct{}:    "",
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
	type testStruct struct {
		Name       string
		Age        int
		Points     int32
		Score      float32
		Winner     bool
		Registered time.Time
		Offshoot   exampleStruct
	}
	exampleInstance := testStruct{
		Name:       "John Smith",
		Age:        30,
		Points:     int32(10),
		Score:      float32(100.4),
		Winner:     false,
		Registered: time.UnixMilli(0),
		Offshoot:   exampleStruct{},
	}
	desiredResults := []string{
		`{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}`,
		`{"Tag": "name=age, type=INT32"}`,
		`{"Tag": "name=points, type=INT32"}`,
		`{"Tag": "name=score, type=FLOAT"}`,
		`{"Tag": "name=winner, type=BOOLEAN"}`,
		`{"Tag": "name=registered, type=INT64"}`,
		"",
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

func TestGenerateSchemaNoData(t *testing.T) {
	emptyInterface := []interface{}{}
	if _, err := generateSchemaFromInterface(emptyInterface); err == nil {
		t.Fatalf("failed to trigger error on empty interface")
	}
}

func TestSparkSQLTransformation(t *testing.T) {
	cases := []struct {
		name            string
		config          TransformationConfig
		sourceID        ResourceID
		expectedFailure bool
	}{
		{
			"SimpleTransformation",
			TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name:    uuid.NewString(),
					Type:    Transformation,
					Variant: "test_variant",
				},
				Query: "SELECT * FROM {{test_name.test_variant}}",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "{{test_name.test_variant}}",
						Source:   "s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
					},
				},
			},
			ResourceID{"test_name", "test_variant", Primary},
			false,
		},
		{
			"FailedTransformation",
			TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name:    uuid.NewString(),
					Type:    Transformation,
					Variant: "test_variant",
				},
				Query: "SELECT * FROM {{test_name.test_variant}}",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "{{test_name.test_variant}}",
						Source:   "s3://featureform-spark-testing/featureform/Primary/test_fake_name/test_fake_variant",
					},
				},
			},
			ResourceID{"test_name", "test_variant", Primary},
			true,
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := store.CreateTransformation(tt.config)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("could not create transformation '%v' because %s", tt.config, err)
			}

			sourceTable, err := store.GetPrimaryTable(tt.sourceID)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("failed to get source table, %v,: %s", tt.sourceID, err)
			}

			transformationTable, err := store.GetTransformationTable(tt.config.TargetTableID)
			if err != nil {
				if tt.expectedFailure {
					return
				}
				t.Fatalf("failed to get the transformation, %s", err)
			}

			sourceCount, err := sourceTable.NumRows()
			transformationCount, err := transformationTable.NumRows()
			if !tt.expectedFailure && sourceCount != transformationCount {
				t.Fatalf("the source table and expected did not match: %v:%v", sourceCount, transformationCount)
			}

			sourcePath, err := store.Store.ResourceKey(tt.config.TargetTableID)
			if err != nil {
				t.Fatalf("failed to retrieve source key %s", err)
			}

			updateConfig := TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name:    tt.config.TargetTableID.Name,
					Type:    Transformation,
					Variant: tt.config.TargetTableID.Variant,
				},
				Query: tt.config.Query,
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: tt.config.SourceMapping[0].Template,
						Source:   fmt.Sprintf("%s%s", store.Store.BucketPrefix(), sourcePath),
					},
				},
			}

			err = store.UpdateTransformation(updateConfig)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("could not update transformation '%v' because %s", updateConfig, err)
			}

			updateTable, err := store.GetTransformationTable(updateConfig.TargetTableID)
			if err != nil {
				if tt.expectedFailure {
					return
				}
				t.Fatalf("failed to get the updated transformation, %s", err)
			}

			updateCount, err := updateTable.NumRows()
			if !tt.expectedFailure && updateCount != transformationCount {
				t.Fatalf("the source table and expected did not match: %v:%v", updateCount, transformationCount)
			}
		})
	}
}

func TestUpdateQuery(t *testing.T) {
	cases := []struct {
		name            string
		query           string
		sourceMap       []SourceMapping
		expectedQuery   string
		expectedSources []string
		expectedFailure bool
	}{
		{
			"TwoReplacementsPass",
			"SELECT * FROM {{name1.variant1}} and more {{name2.variant2}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
				},
				SourceMapping{
					Template: "{{name2.variant2}}",
					Source:   "s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant",
				},
			},
			"SELECT * FROM source_0 and more source_1",
			[]string{
				"s3://featureform-spark-testing/featureform/testprimary/testFile.parquet",
				"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384/part-00000-c93fe1fb-4ab0-45df-9292-b139e4043181-c000.snappy.parquet",
			},
			false,
		},
		{
			"OneReplacementPass",
			"SELECT * FROM {{name1.variant1}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant",
				},
			},
			"SELECT * FROM source_0",
			[]string{
				"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384/part-00000-c93fe1fb-4ab0-45df-9292-b139e4043181-c000.snappy.parquet",
			},
			false,
		},
		{
			"ReplacementExpectedFailure",
			"SELECT * FROM {{name1.variant1}} and more {{name2.variant2}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "s3://featureform-bucket/featureform/Transformation/name1/variant1/file",
				},
			},
			"SELECT * FROM source_0",
			[]string{
				"s3://featureform-bucket/featureform/Transformation/name1/variant1/file",
			},
			true,
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			retreivedQuery, sources, err := store.updateQuery(tt.query, tt.sourceMap)

			if !tt.expectedFailure && err != nil {
				t.Fatalf("Could not replace the template query: %v", err)
			}
			if !tt.expectedFailure && !reflect.DeepEqual(retreivedQuery, tt.expectedQuery) {
				t.Fatalf("updateQuery did not replace the query correctly. Expected \" %v \", got \" %v \".", tt.expectedQuery, retreivedQuery)
			}
			if !tt.expectedFailure && !reflect.DeepEqual(sources, tt.expectedSources) {
				t.Fatalf("updateQuery did not get the correct sources. Expected \" %v \", got \" %v \".", tt.expectedSources, sources)
			}
		})
	}
}

func TestGetTransformation(t *testing.T) {
	cases := []struct {
		name             string
		id               ResourceID
		expectedRowCount int64
	}{
		{
			"testTransformation",
			ResourceID{
				Name:    "12fdd4f9-023c-4c0e-99ae-35bdabd0a465",
				Type:    Transformation,
				Variant: "test_variant",
			},
			5,
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			table, err := store.GetTransformationTable(tt.id)
			if err != nil {
				t.Fatalf("Failed to get Transformation Table: %v", err)
			}

			caseNumRow, err := table.NumRows()
			if err != nil {
				t.Fatalf("Failed to get Transformation Table Num Rows: %v", err)
			}

			if caseNumRow != tt.expectedRowCount {
				t.Fatalf("Row count do not match. Expected \" %v \", got \" %v \".", caseNumRow, tt.expectedRowCount)
			}
		})
	}
}

func TestGetSourcePath(t *testing.T) {
	cases := []struct {
		name            string
		sourcePath      string
		expectedPath    string
		expectedFailure bool
	}{
		{
			"PrimaryPathSuccess",
			"s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
			"s3://featureform-spark-testing/featureform/testprimary/testFile.parquet",
			false,
		},
		{
			"TransformationPathSuccess",
			"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant",
			"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384/part-00000-c93fe1fb-4ab0-45df-9292-b139e4043181-c000.snappy.parquet",
			false,
		},
		{
			"PrimaryPathFailure",
			"s3://featureform-spark-testing/featureform/Primary/fake_name/fake_variant",
			"",
			true,
		},
		{
			"TransformationPathFailure",
			"s3://featureform-spark-testing/featureform/Transformation/fake_028f6213-77a8-43bb-9d91-dd7e9ee96102/fake_variant",
			"",
			true,
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			retreivedPath, err := store.getSourcePath(tt.sourcePath)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("getSourcePath could not get the path because %s.", err)
			}

			if !tt.expectedFailure && !reflect.DeepEqual(tt.expectedPath, retreivedPath) {
				t.Fatalf("getSourcePath could not find the expected path. Expected \"%s\", got \"%s\".", tt.expectedPath, retreivedPath)
			}
		})
	}
}

func getSparkOfflineStore(t *testing.T) (*SparkOfflineStore, error) {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println(err)
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

	return sparkOfflineStore, nil
}

// Unit tests

func TestSparkConfigDeserialize(t *testing.T) {
	correctSparkConfig := SparkConfig{
		ExecutorType:   "EMR",
		ExecutorConfig: "",
		StoreType:      "S3",
		StoreConfig:    "",
	}
	serializedConfig := correctSparkConfig.Serialize()
	reserializedConfig := SparkConfig{}
	if err := reserializedConfig.Deserialize(SerializedConfig(serializedConfig)); err != nil {
		t.Fatalf("error deserializing spark config")
	}
	invalidConfig := SerializedConfig("invalidConfig")
	invalidDeserialized := SparkConfig{}
	if err := invalidDeserialized.Deserialize(invalidConfig); err == nil {
		t.Fatalf("did not return error on deserializing improper config")
	}
}

func TestEMRConfigDeserialize(t *testing.T) {
	correctEMRConfig := EMRConfig{
		AWSAccessKeyId: "",
		AWSSecretKey:   "",
		ClusterRegion:  "us-east-1",
		ClusterName:    "example",
	}
	serializedConfig := correctEMRConfig.Serialize()
	reserializedConfig := EMRConfig{}
	if err := reserializedConfig.Deserialize(SerializedConfig(serializedConfig)); err != nil {
		t.Fatalf("error deserializing emr config")
	}
	invalidConfig := SerializedConfig("invalidConfig")
	invalidDeserialized := EMRConfig{}
	if err := invalidDeserialized.Deserialize(invalidConfig); err == nil {
		t.Fatalf("did not return error on deserializing improper config")
	}
}

func TestS3ConfigDeserialize(t *testing.T) {
	correctSparkConfig := S3Config{
		AWSAccessKeyId: "",
		AWSSecretKey:   "",
		BucketRegion:   "us-east-1",
		BucketPath:     "example",
	}
	serializedConfig := correctSparkConfig.Serialize()
	reserializedConfig := S3Config{}
	if err := reserializedConfig.Deserialize(SerializedConfig(serializedConfig)); err != nil {
		t.Fatalf("error deserializing spark config")
	}
	invalidConfig := SerializedConfig("invalidConfig")
	invalidDeserialized := S3Config{}
	if err := invalidDeserialized.Deserialize(invalidConfig); err == nil {
		t.Fatalf("did not return error on deserializing improper config")
	}
}

func TestMaterializationCreate(t *testing.T) {
	exampleSchemaWithTS := ResourceSchema{
		Entity: "entity",
		Value:  "value",
		TS:     "timestamp",
	}
	queries := defaultSparkOfflineQueries{}
	materializeQuery := queries.materializationCreate(exampleSchemaWithTS)
	correctQuery := "SELECT entity, value, ts, ROW_NUMBER() over (ORDER BY (SELECT NULL)) AS row_number FROM (SELECT entity, value, ts, rn FROM (SELECT entity AS entity, value AS value, timestamp AS ts, ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) AS rn FROM source_0) t WHERE rn=1) t2"
	if correctQuery != materializeQuery {
		t.Fatalf("Materialize create did not produce correct query")
	}

	exampleSchemaWithoutTS := ResourceSchema{
		Entity: "entity",
		Value:  "value",
	}
	materializeTSQuery := queries.materializationCreate(exampleSchemaWithoutTS)
	correctTSQuery := "SELECT entity, value, ts, ROW_NUMBER() over (ORDER BY (SELECT NULL)) AS row_number FROM (SELECT entity, value, ts, rn FROM (SELECT entity AS entity, value AS value, ts AS ts, ROW_NUMBER() OVER (PARTITION BY entity ORDER BY ts DESC) AS rn FROM source_0) t WHERE rn=1) t2"
	if correctTSQuery != materializeTSQuery {
		t.Fatalf("Materialize create did not produce correct query substituting timestamp")
	}
}

func TestTrainingSetCreate(t *testing.T) {
	// (def TrainingSetDef, featureSchemas []ResourceSchema, labelSchema ResourceSchema
	testTrainingSetDef := TrainingSetDef{
		ID: ResourceID{"test_training_set", "default", TrainingSet},
		Features: []ResourceID{
			{"test_feature_1", "default", Feature},
			{"test_feature_2", "default", Feature},
		},
		Label: ResourceID{"test_label", "default", Label},
	}
	testFeatureSchemas := []ResourceSchema{
		{
			Entity: "entity",
			Value:  "feature_value_1",
			TS:     "ts",
		},
		{
			Entity: "entity",
			Value:  "feature_value_2",
			TS:     "ts",
		},
	}
	testLabelSchema := ResourceSchema{
		Entity: "entity",
		Value:  "label_value",
		TS:     "ts",
	}
	queries := defaultSparkOfflineQueries{}
	trainingSetQuery := queries.trainingSetCreate(testTrainingSetDef, testFeatureSchemas, testLabelSchema)
	correctQuery := "SELECT Feature__test_feature_1__default, Feature__test_feature_2__default, value AS Label__test_label__default FROM ((SELECT * FROM (SELECT entity, value, ts, rn " +
		"FROM (SELECT entity AS entity, label_value AS value, ts AS ts, ROW_NUMBER() over (PARTITION BY entity, label_value, ts ORDER BY ts DESC) AS rn FROM source_0) t WHERE rn = 1) t0) LEFT OUTER JOIN (SELECT entity as t1_entity, feature_value_1 as Feature__test_feature_1__default, ts as t1_ts FROM source_1) t1 ON (t1_entity = entity AND t1_ts <= ts) LEFT OUTER JOIN (SELECT entity as t2_entity, feature_value_2 as Feature__test_feature_2__default, ts as t2_ts FROM source_2) t2 ON (t2_entity = entity AND t2_ts <= ts))"
	if trainingSetQuery != correctQuery {
		t.Fatalf("training set query not correct")
	}
}

func TestCompareStructsFail(t *testing.T) {
	type testStruct struct {
		Field string
	}
	firstStruct := testStruct{"first"}
	secondStruct := testStruct{"second"}
	if err := compareStructs(firstStruct, secondStruct); err == nil {
		t.Fatalf("failed to trigger error with unequal structs")
	}
	type similarStruct struct {
		Field int
	}
	firstStructSimilar := testStruct{"1"}
	secondStructSimilar := similarStruct{1}
	if err := compareStructs(firstStructSimilar, secondStructSimilar); err == nil {
		t.Fatalf("failed to trigger error when structs contain different types")
	}
	type testStructFields struct {
		Field      string
		OtherField int
	}
	firstStructFields := testStructFields{"1", 2}
	secondStructFields := testStruct{"1"}
	if err := compareStructs(firstStructFields, secondStructFields); err == nil {
		t.Fatalf("failed to trigger error when structs contain different types")
	}
}

func TestGenericTableIteratorError(t *testing.T) {
	iter := S3GenericTableIterator{}
	if err := iter.Err(); err != nil {
		t.Fatalf("triggered nonexistent error on iterator")
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("triggered nonexistent error on closing")
	}
}

func TestPrimaryTableError(t *testing.T) {
	table := S3PrimaryTable{sourcePath: "test_path"}
	rec := GenericRecord([]interface{}{"1"})
	if err := table.Write(rec); err == nil {
		t.Fatalf("did not trigger error on attempting to write")
	}
	if path := table.GetName(); path != "test_path" {
		t.Fatalf("did not return correct name")
	}
}

func TestOfflineTableError(t *testing.T) {
	table := S3OfflineTable{}
	rec := ResourceRecord{}
	if err := table.Write(rec); err == nil {
		t.Fatalf("did not trigger error on attempting to write")
	}
}

func TestFeatureIteratorError(t *testing.T) {
	iter := S3FeatureIterator{}
	if err := iter.Close(); err != nil {
		t.Fatalf("triggered error on trying to close feature iterator")
	}
}

func TestStreamRecordReadInt(t *testing.T) {
	intPayload := []byte("1")
	record := s3Types.SelectObjectContentEventStreamMemberRecords{Value: s3Types.RecordsEvent{Payload: intPayload}}
	if _, err := streamRecordReadInteger(&record); err != nil {
		t.Fatalf("triggered error trying to parse integer payload")
	}
	nonIntPayload := []byte("fail")
	failRecord := s3Types.SelectObjectContentEventStreamMemberRecords{Value: s3Types.RecordsEvent{Payload: nonIntPayload}}
	if _, err := streamRecordReadInteger(&failRecord); err == nil {
		t.Fatalf("did not trigger error reading invalid payload")
	}
}

func TestSparkExecutorFail(t *testing.T) {
	invalidConfig := SerializedConfig("invalid")
	invalidExecType := SparkExecutorType("invalid")
	if executor, err := NewSparkExecutor(invalidExecType, invalidConfig); !(executor == nil && err == nil) {
		t.Fatalf("did not return nil on invalid exec type")
	}
	validExecType := SparkExecutorType("EMR")
	if _, err := NewSparkExecutor(validExecType, invalidConfig); err == nil {
		t.Fatalf("did not trigger error with invalid config")
	}
}

func TestSparkStoreFail(t *testing.T) {
	invalidConfig := SerializedConfig("invalid")
	invalidExecType := SparkStoreType("invalid")
	if executor, err := NewSparkStore(invalidExecType, invalidConfig); !(executor == nil && err == nil) {
		t.Fatalf("did not return nil on invalid exec type")
	}
	validExecType := SparkStoreType("S3")
	if _, err := NewSparkStore(validExecType, invalidConfig); err == nil {
		t.Fatalf("did not trigger error with invalid config")
	}
}

func TestUnimplimentedFailures(t *testing.T) {
	store := SparkOfflineStore{}
	if table, err := store.CreatePrimaryTable(ResourceID{}, TableSchema{}); !(table == nil && err == nil) {
		t.Fatalf("did not return nil on calling unimplimented function")
	}
	if table, err := store.CreateResourceTable(ResourceID{}, TableSchema{}); !(table == nil && err == nil) {
		t.Fatalf("did not return nil on calling unimplimented function")
	}
}

func TestFeatureCSVToResource(t *testing.T) {
	validCSV := "entity,value,1"
	resource, err := featureCSVToResource(validCSV)
	if err != nil {
		t.Fatalf("triggered error creating valid resource: %v", err)
	}
	if resource.Entity != "entity" || resource.Value != "value" || resource.TS != time.UnixMilli(1) {
		t.Fatalf("did not properly convert valid csv to resource record")
	}
	invalidCSV := "entity,value,not a timestamp"
	if _, err := featureCSVToResource(invalidCSV); err == nil {
		t.Fatalf("did not trigger error converting invalid CSV to resource")
	}
	shortCSV := "entity,value"
	if _, err := featureCSVToResource(shortCSV); err == nil {
		t.Fatalf("did not trigger error converting short CSV to resource")
	}
}

func TestS3FeatureIteratorStreamFail(t *testing.T) {
	out := make(chan []byte)
	go func(out chan []byte) {
		defer close(out)
		out <- []byte("invalid csv")
	}(out)
	failingS3Iterator := S3FeatureIterator{stream: out}
	if result := failingS3Iterator.Next(); result != false {
		t.Fatalf("failing iterator stream did not trigger errror when passed invalid csv")
	}
}

func TestTrainingSetIteratorStreamFail(t *testing.T) {
	out := make(chan []byte)
	go func(out chan []byte) {
		defer close(out)
		out <- []byte("invalid csv")
	}(out)
	failingTrainingSet := S3TrainingSet{iter: out}
	if result := failingTrainingSet.Next(); result != false {
		t.Fatalf("failing iterator stream did not trigger errror when passed invalid csv")
	}
}

func TestStreamGetKeys(t *testing.T) {
	type testStruct struct {
		Name   string
		Value  int
		Failed bool
	}
	correctFields := map[string]bool{
		"Name":   true,
		"Value":  true,
		"Failed": true,
	}
	test := testStruct{"name", 1, true}
	payload, err := json.Marshal(test)
	if err != nil {
		t.Fatalf("Could not marshal into json: %v", err)
	}
	record := s3Types.SelectObjectContentEventStreamMemberRecords{Value: s3Types.RecordsEvent{Payload: payload}}
	records, err := streamGetKeys(&record)
	if err != nil {
		t.Fatalf("failed to parse json: %v", err)
	}
	for _, rec := range records {
		if correctFields[rec] != true {
			t.Fatalf("invalid record field returned")
		}
	}
	invalidPayload := []byte("invalid payload")
	invalidRecord := s3Types.SelectObjectContentEventStreamMemberRecords{Value: s3Types.RecordsEvent{Payload: invalidPayload}}
	if _, err := streamGetKeys(&invalidRecord); err == nil {
		t.Fatalf("failed to trigger error retrieving fields from invalid json byte string")
	}
}
