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

	"github.com/google/uuid"
	"github.com/joho/godotenv"
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

func TestGetResourceInformationFromFilePath(t *testing.T) {
	cases := []struct {
		name         string
		sourcePath   string
		expectedInfo []string
	}{
		{
			"PrimaryPathSuccess",
			"s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
			[]string{"Primary", "test_name", "test_variant"},
		},
		{
			"TransformationPathSuccess",
			"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant",
			[]string{"Transformation", "028f6213-77a8-43bb-9d91-dd7e9ee96102", "test_variant"},
		},
		{
			"IncorrectPrimaryPath",
			"s3://featureform-spark-testing/featureform/Primary/",
			[]string{"", "", ""},
		},
		{
			"IncorrectTransformationPath",
			"s3://featureform-spark-testing/featureform/Transformation/fake_028f6213",
			[]string{"", "", ""},
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			resourceType, resourceName, resourceVariant := store.getResourceInformationFromFilePath(tt.sourcePath)
			resourceInfo := []string{resourceType, resourceName, resourceVariant}

			if !reflect.DeepEqual(tt.expectedInfo, resourceInfo) {
				t.Fatalf("getSourcePath could not find the expected path. Expected \"%s\", got \"%s\".", tt.expectedInfo, resourceInfo)
			}
		})
	}
}

func TestGetDFArgs(t *testing.T) {
	cases := []struct {
		name            string
		outputURI       string
		code            string
		region          string
		mapping         []SourceMapping
		expectedArgs    []string
		expectedFailure bool
	}{
		{
			"PrimaryPathSuccess",
			"s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
			"code",
			"us-east-2",
			[]SourceMapping{
				SourceMapping{
					Template: "transaction",
					Source:   "s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
				},
			},
			[]string{
				"spark-submit",
				"--deploy-mode",
				"cluster",
				"s3://featureform-spark-testing/featureform/scripts/offline_store_spark_runner.py",
				"df",
				"--output_uri",
				"s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
				"--code",
				"code",
				"--aws_region",
				"us-east-2",
				"--source",
				"transaction=s3://featureform-spark-testing/featureform/testprimary/testFile.parquet",
			},
			false,
		},
		{
			"FakePrimaryPath",
			"s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
			"code",
			"us-east-2",
			[]SourceMapping{
				SourceMapping{
					Template: "transaction",
					Source:   "s3://featureform-spark-testing/featureform/Primary",
				},
			},
			nil,
			true,
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			args, err := store.getDFArgs(tt.outputURI, tt.code, tt.region, tt.mapping)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("could not get df args %s", err)
			}

			if !tt.expectedFailure && !reflect.DeepEqual(tt.expectedArgs, args) {
				t.Fatalf("getDFArgs could not generate the expected args. Expected \"%s\", got \"%s\".", tt.expectedArgs, args)
			}
		})
	}
}

func TestTransformation(t *testing.T) {
	cases := []struct {
		name            string
		config          TransformationConfig
		sourceID        ResourceID
		expectedFailure bool
	}{
		{
			"SQLTransformation",
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
			"DFTransformationType",
			TransformationConfig{
				Type: DFTransformation,
				TargetTableID: ResourceID{
					Name:    uuid.NewString(),
					Type:    Transformation,
					Variant: "test_variant",
				},
				Query: "s3://featureform-spark-testing/featureform/DFTransformations/test_name/test_variant/transformation.pkl",
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: "transaction",
						Source:   "s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
					},
				},
			},
			ResourceID{"test_name", "test_variant", Primary},
			false,
		},
		{
			"NoTransformationType",
			TransformationConfig{
				Type:          NoTransformationType,
				TargetTableID: ResourceID{},
				Query:         "SELECT * FROM {{test_name.test_variant}}",
				SourceMapping: []SourceMapping{},
			},
			ResourceID{},
			true,
		},
	}

	store, err := getSparkOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get SparkOfflineStore: %s", err)
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := store.transformation(tt.config, false)
			if !tt.expectedFailure && err != nil {
				t.Fatalf("could not run transformation %s", err)
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
		})
	}
}

func TestRegisterSparkAWSStore(t *testing.T) {
	sparkAWSConfig := SparkAWSConfig{
		EMRClusterId:       os.Getenv("AWS_EMR_CLUSTER_ID"),
		BucketPath:         os.Getenv("S3_BUCKET_PATH"),
		EMRClusterRegion:   os.Getenv("AWS_EMR_CLUSTER_REGION"),
		BucketRegion:       os.Getenv("S3_BUCKET_REGION"),
		AWSAccessKeyId:     SecretConfig(os.Getenv("AWS_ACCESS_KEY_ID")),
		AWSSecretAccessKey: SecretConfig(os.Getenv("AWS_SECRET_KEY")),
	}
	serialized := sparkAWSConfig.Serialize()
	newSparkAWSStore, err := SparkAWSOfflineStoreFactory(serialized)
	if err != nil {
		t.Fatalf("could not create new AWS spark store: %v", err)
	}
	offlineSparkStore, err := newSparkAWSStore.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not convert spark AWS store to offline store: %v", err)
	}
	sparkOfflineStore, ok := offlineSparkStore.(*SparkOfflineStore)
	if !ok {
		t.Fatalf("could not convert offline store to spark offline store")
	}
}

func getSparkOfflineStore(t *testing.T) (*SparkOfflineStore, error) {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println(err)
	}

	sparkAWSConfig := SparkAWSConfig{
		EMRClusterId:       os.Getenv("AWS_EMR_CLUSTER_ID"),
		BucketPath:         os.Getenv("S3_BUCKET_PATH"),
		EMRClusterRegion:   os.Getenv("AWS_EMR_CLUSTER_REGION"),
		BucketRegion:       os.Getenv("S3_BUCKET_REGION"),
		AWSAccessKeyId:     SecretConfig(os.Getenv("AWS_ACCESS_KEY_ID")),
		AWSSecretAccessKey: SecretConfig(os.Getenv("AWS_SECRET_KEY")),
	}
	sparkSerializedConfig := sparkAWSConfig.Serialize()

	sparkProvider, err := Get("SPARK_AWS_OFFLINE", sparkSerializedConfig)
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
