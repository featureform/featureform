//go:build spark
// +build spark

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	// "os"
	"bytes"
	"encoding/csv"
	"reflect"
	"strings"
	"testing"
	"time"

	// s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	pc "github.com/featureform/provider/provider_config"
)

// will replace all the upload parquet table functions
func uploadCSVTable(store FileStore, path string, tables interface{}) error {
	maxSlice := make([][]string, 0)
	array := reflect.ValueOf(tables)
	fieldSlice := make([]string, 0)
	structOne := reflect.ValueOf(array.Index(0).Interface())
	for i := 0; i < structOne.NumField(); i++ {
		fieldSlice = append(fieldSlice, structOne.Type().Field(i).Name)
	}
	maxSlice = append(maxSlice, fieldSlice)

	for i := 0; i < array.Len(); i++ {
		structArr := reflect.ValueOf(array.Index(i).Interface())
		values := make([]string, 0)
		for i := 0; i < structArr.NumField(); i++ {
			values = append(values, fmt.Sprintf("%v", structArr.Field(i).Interface()))
		}
		maxSlice = append(maxSlice, values)
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.WriteAll(maxSlice) // calls Flush internally

	if err := w.Error(); err != nil {
		return fmt.Errorf("error writing csv: %v", err)
	}
	if err := store.Write(path, buf.Bytes()); err != nil {
		return fmt.Errorf("could not write parquet file to path: %v", err)
	}
	return nil

}

func uploadParquetTable(store FileStore, path string, tables interface{}) error {
	//reflect the interface into an []any list and pass it
	array := reflect.ValueOf(tables)
	anyArray := make([]any, 0)
	for i := 0; i < array.Len(); i++ {
		anyArray = append(anyArray, array.Index(i).Interface())
	}
	fmt.Println("Parquet file to be written:")
	fmt.Println(anyArray)
	parquetBytes, err := convertToParquetBytes(anyArray)
	if err != nil {
		return fmt.Errorf("could not convert struct list to parquet bytes: %v", err)
	}
	fmt.Println("parquet bytes:")
	fmt.Println(parquetBytes)
	if err := store.Write(path, parquetBytes); err != nil {
		return fmt.Errorf("could not write parquet file to path: %v", err)
	}
	return nil
}

func testCreateTrainingSet(store *SparkOfflineStore) error {
	exampleStructArray := make([]any, 5)
	for i := 0; i < 5; i++ {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: int64(i),
		}
	}
	correctMapping := map[interface{}]bool{
		30: false,
		31: false,
		32: false,
		33: false,
		34: false,
	}
	path := "featureform/tests/trainingSetTest.csv"
	if err := uploadCSVTable(store.Store, path, exampleStructArray); err != nil {
		return err
	}
	testFeatureResource := sparkSafeRandomID(Feature)
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
	testLabelResource := sparkSafeRandomID(Label)
	testLabelResourceSchema := ResourceSchema{"name", "winner", "registered", path}
	labelTable, err := store.RegisterResourceFromSourceTable(testLabelResource, testLabelResourceSchema)
	fetchedLabel, err := store.GetResourceTable(testLabelResource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fetchedLabel, labelTable) {
		return fmt.Errorf("Did not properly register label")
	}
	trainingSetResource := sparkSafeRandomID(TrainingSet)
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
		if correctMapping[features[0]] != label {
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
			Registered: int64(i),
		}
	}
	for i := 5; i < 10; i++ {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i-5),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     true,
			Registered: int64(i),
		}
	}
	path := "featureform/tests/testFile2.csv"
	if err := uploadCSVTable(store.Store, path, exampleStructArray); err != nil {
		return err
	}
	testResourceName := "test_name_materialize"
	testResourceVariant := uuid.New().String()
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
	if fetchedMaterialization.ID() != MaterializationID(fmt.Sprintf("Materialization/%s/%s", testResourceName, testResourceVariant)) {
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
		if !reflect.DeepEqual(rec, correctMaterialization[rec.Entity]) {
			return fmt.Errorf("Wrong materialization entry: %v does not equal %v", rec.Value, correctMaterialization[rec.Entity].Value)
		}
	}
	return nil
}

type exampleStruct struct {
	Name       string
	Age        int
	Score      float32
	Winner     bool
	Registered int64
}

func testRegisterResource(store *SparkOfflineStore) error {
	exampleStructArray := make([]exampleStruct, 5)
	for i := range exampleStructArray {
		exampleStructArray[i] = exampleStruct{
			Name:       fmt.Sprintf("John Smith_%d", i),
			Age:        30 + i,
			Score:      100.4 + float32(i),
			Winner:     false,
			Registered: int64(i),
		}
	}
	path := "featureform/tests/testFile.csv"
	if err := uploadCSVTable(store.Store, path, exampleStructArray); err != nil {
		return err
	}
	resourceVariantName := uuid.New().String()
	testResource := ResourceID{"test_name", resourceVariantName, Feature}
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

func unorderedEqual(first, second []any) bool {
	if len(first) != len(second) {
		return false
	}
	exists := make(map[any]bool)
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

func unorderedEqualString(first, second []string) bool {
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
			Registered: int64(i),
		}
	}

	path := "featureform/testprimary/testFile.csv"
	if err := uploadCSVTable(store.Store, path, exampleStructArray); err != nil {
		return err
	}
	primaryVariantName := uuid.New().String()
	testResource := ResourceID{"test_name", primaryVariantName, Primary}
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
	expectedColumns := []string{"Name", "Age", "Score", "Winner", "Registered"}

	if !unorderedEqualString(iterator.Columns(), expectedColumns) {
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
	// emrSparkOfflineStore, err := getSparkOfflineStore(t)
	// if err != nil {
	// 	t.Fatalf("could not get SparkOfflineStore: %s", err)
	// }
	databricksSparkOfflineStore, err := getDatabricksOfflineStore(t)
	if err != nil {
		t.Fatalf("could not get databricks offline store: %s", err)
	}
	sparkStores := map[string]*SparkOfflineStore{
		// "EMR_SPARK_STORE":        emrSparkOfflineStore,
		"DATABRICKS_SPARK_STORE": databricksSparkOfflineStore,
	}

	testFns := map[string]func(*testing.T, *SparkOfflineStore){
		// FileStore Tests (Do not use spark executor)
		"sparkTestCreateGetOfflineTable":           sparkTestCreateGetOfflineTable,
		"sparkTestOfflineTableAlreadyExists":       sparkTestOfflineTableAlreadyExists,
		"sparkTestInvalidResourceIDs":              sparkTestInvalidResourceIDs,
		"sparkTestInvalidMaterialization":          sparkTestInvalidMaterialization,
		"sparkTestMaterializeUnknown":              sparkTestMaterializeUnknown,
		"sparkTestGetTrainingSetInvalidResourceID": sparkTestGetTrainingSetInvalidResourceID,
		"sparkTestInvalidTrainingSetDefs":          sparkTestInvalidTrainingSetDefs,
		"sparkTestLabelTableNotFound":              sparkTestLabelTableNotFound,
		"sparkTestFeatureTableNotFound":            sparkTestFeatureTableNotFound,
		"sparkTestCreatePrimaryFromSource":         sparkTestCreatePrimaryFromSource,
		"sparkTestCreateDuplicatePrimaryTable":     sparkTestCreateDuplicatePrimaryTable,

		// Databricks Test (use FileStore and spark executor)
		// "sparkTestTrainingSet":             sparkTestTrainingSet,
		// "sparkTestMaterializations":        sparkTestMaterializations,
		// "sparkTestTrainingSetDefShorthand": sparkTestTrainingSetDefShorthand,
		// "sparkTestTrainingSetUpdate":       sparkTestTrainingSetUpdate,
		// "sparkTestSQLTransformation": testSparkSQLTransformation,
		// "sparkTestGetDFArgs":                          testGetDFArgs,
		"sparkTestGetResourceInformationFromFilePath": testGetResourceInformationFromFilePath,
		// "sparkTestGetSourcePath":                      testGetSourcePath,
		// "sparkTestGetTransformation": testGetTransformation,
		// "sparkTestTransformation":                     testTransformation, //Passing except dataframes

		// NON-passing tests

		// "sparkTestOfflineTableNotFound":               sparkTestOfflineTableNotFound, //TODO error returns correct error struct
		// "sparkTestMaterializationNotFound":            sparkTestMaterializationNotFound, //TODO error returns correct error type
		// "sparkTestGetUnknownTrainingSet":              sparkTestGetUnknownTrainingSet, //TODO error returns correct error type

		// "sparkTestUpdateQuery":                        testUpdateQuery, //TODO, change so not hardcoded for s3
		// "sparkTestMaterializationUpdate":              sparkTestMaterializationUpdate, //TODO change upload timestamp formats

	}

	t.Run("SPARK_STORE_FUNCTIONS", func(t *testing.T) {
		for name, testFn := range testFns {
			nameConst := name
			testFnConst := testFn
			for name, store := range sparkStores {
				storeNameConst := name
				storeConst := store
				t.Run(fmt.Sprintf("%s_%s", nameConst, storeNameConst), func(t *testing.T) {
					t.Parallel()
					testFnConst(t, storeConst)
				})
			}
		}
	})

}

type TestRecordInt struct {
	Entity string
	Value  int
	TS     int64 `parquet:"," parquet-key:",timestamp"`
}

type TestRecordString struct {
	Entity string
	Value  string
	TS     int64 `parquet:"," parquet-key:",timestamp"`
}

type TestRecordBool struct {
	Entity string
	Value  bool
	TS     int64 `parquet:"," parquet-key:",timestamp"`
}

func sparkTestCreateDuplicatePrimaryTable(t *testing.T, store *SparkOfflineStore) {
	var err error
	randomSourceTablePath := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
	table := []TestRecordInt{{
		Entity: "a", Value: 1, TS: 0,
	}}
	if err := uploadCSVTable(store.Store, randomSourceTablePath, table); err != nil {
		t.Fatalf("could not upload source table")
	}
	primaryID := sparkSafeRandomID(Primary)
	_, err = store.RegisterPrimaryFromSourceTable(primaryID, randomSourceTablePath)
	if err != nil {
		t.Fatalf("Could not register from Source Table: %s", err)
	}
	_, err = store.GetPrimaryTable(primaryID)
	if err != nil {
		t.Fatalf("Could not get primary table: %v", err)
	}
	_, err = store.RegisterPrimaryFromSourceTable(primaryID, randomSourceTablePath)
	if err == nil {
		t.Fatalf("Successfully create duplicate tables")
	}
}

func sparkTestCreatePrimaryFromSource(t *testing.T, store *SparkOfflineStore) {
	//upload random source table
	var err error
	randomSourceTablePath := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
	table := []TestRecordInt{{
		Entity: "a", Value: 1, TS: int64(1),
	}}
	if err := uploadCSVTable(store.Store, randomSourceTablePath, table); err != nil {
		t.Fatalf("could not upload source table")
	}
	primaryID := sparkSafeRandomID(Primary)
	_, err = store.RegisterPrimaryFromSourceTable(primaryID, randomSourceTablePath)
	if err != nil {
		t.Fatalf("Could not register from Source Table: %s", err)
	}
	_, err = store.GetPrimaryTable(primaryID)
	if err != nil {
		t.Fatalf("Could not get primary table: %v", err)
	}
}

func sparkTestGetTrainingSetInvalidResourceID(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Feature)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting invalid training set ResourceID")
	}
}

func sparkTestGetUnknownTrainingSet(t *testing.T, store *SparkOfflineStore) {
	// This should default to TrainingSet
	id := sparkSafeRandomID(NoType)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting unknown training set ResourceID")
	} else if _, valid := err.(*TrainingSetNotFound); !valid {
		t.Fatalf("Wrong error for training set not found: %T", err)
	} else if err.Error() == "" {
		t.Fatalf("Training set not found error msg not set")
	}
}

func sparkTestInvalidTrainingSetDefs(t *testing.T, store *SparkOfflineStore) {
	invalidDefs := map[string]TrainingSetDef{
		"WrongTSType": TrainingSetDef{
			ID:    sparkSafeRandomID(Feature),
			Label: sparkSafeRandomID(Label),
			Features: []ResourceID{
				sparkSafeRandomID(Feature),
				sparkSafeRandomID(Feature),
				sparkSafeRandomID(Feature),
			},
		},
		"WrongLabelType": TrainingSetDef{
			ID:    sparkSafeRandomID(TrainingSet),
			Label: sparkSafeRandomID(Feature),
			Features: []ResourceID{
				sparkSafeRandomID(Feature),
				sparkSafeRandomID(Feature),
				sparkSafeRandomID(Feature),
			},
		},
		"WrongFeatureType": TrainingSetDef{
			ID:    sparkSafeRandomID(TrainingSet),
			Label: sparkSafeRandomID(Label),
			Features: []ResourceID{
				sparkSafeRandomID(Feature),
				sparkSafeRandomID(Label),
				sparkSafeRandomID(Feature),
			},
		},
		"NoFeatures": TrainingSetDef{
			ID:       sparkSafeRandomID(TrainingSet),
			Label:    sparkSafeRandomID(Label),
			Features: []ResourceID{},
		},
	}
	for name, def := range invalidDefs {
		nameConst := name
		defConst := def
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			if err := store.CreateTrainingSet(defConst); err == nil {
				t.Fatalf("Succeeded to create invalid def")
			}
		})
	}
}

func sparkTestLabelTableNotFound(t *testing.T, store *SparkOfflineStore) {
	featureID := sparkSafeRandomID(Feature)
	if err := registerRandomResource(featureID, store); err != nil {
		t.Fatalf("could not register random resource")
	}
	def := TrainingSetDef{
		ID:    sparkSafeRandomID(TrainingSet),
		Label: sparkSafeRandomID(Label),
		Features: []ResourceID{
			featureID,
		},
	}
	if err := store.CreateTrainingSet(def); err == nil {
		t.Fatalf("Succeeded in creating training set with unknown label")
	}
}

func sparkTestFeatureTableNotFound(t *testing.T, store *SparkOfflineStore) {
	labelID := sparkSafeRandomID(Label)
	if err := registerRandomResource(labelID, store); err != nil {
		t.Fatalf("could not register random resource")
	}
	def := TrainingSetDef{
		ID:    sparkSafeRandomID(TrainingSet),
		Label: labelID,
		Features: []ResourceID{
			sparkSafeRandomID(Feature),
		},
	}
	if err := store.CreateTrainingSet(def); err == nil {
		t.Fatalf("Succeeded in creating training set with unknown feature")
	}
}

func sparkTestTrainingSetDefShorthand(t *testing.T, store *SparkOfflineStore) {
	featureID := sparkSafeRandomID(Feature)
	if err := registerRandomResource(featureID, store); err != nil {
		t.Fatalf("could not register random resource")
	}
	labelID := sparkSafeRandomID(Label)
	if err := registerRandomResource(labelID, store); err != nil {
		t.Fatalf("could not register random resource")
	}
	// TrainingSetDef can be done in shorthand without types. Their types should
	// be set automatically by the check() function.
	labelID.Type = NoType
	featureID.Type = NoType
	def := TrainingSetDef{
		ID:       sparkSafeRandomID(NoType),
		Label:    labelID,
		Features: []ResourceID{featureID},
	}
	if err := store.CreateTrainingSet(def); err != nil {
		t.Fatalf("Failed to create training set: %s", err)
	}
}

func sparkTestOfflineTableNotFound(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Feature, Label)
	if _, err := store.GetResourceTable(id); err == nil {
		t.Fatalf("Succeeded in getting non-existant table")
	} else if casted, valid := err.(*TableNotFound); !valid {
		t.Fatalf("Wrong error for table not found: %v, %T", err, err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

type simpleTestStruct struct {
	Entity string
	Value  int
	Ts     int64
}

func registerRandomResourceGiveTablePath(id ResourceID, path string, store *SparkOfflineStore, table interface{}, timestamp bool) error {
	if err := uploadCSVTable(store.Store, path, table); err != nil {
		return err
	}
	var schema ResourceSchema
	if timestamp {
		schema = ResourceSchema{"entity", "value", "ts", path}
	} else {
		schema = ResourceSchema{Entity: "entity", Value: "value", SourceTable: path}
	}
	_, err := store.RegisterResourceFromSourceTable(id, schema)
	if err != nil {
		return err
	}
	return nil
}

func registerRandomResourceGiveTable(id ResourceID, store *SparkOfflineStore, table interface{}, timestamp bool) error {
	randomSourceTablePath := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
	if err := uploadCSVTable(store.Store, randomSourceTablePath, table); err != nil {
		return err
	}
	var schema ResourceSchema
	if timestamp {
		schema = ResourceSchema{"entity", "value", "ts", randomSourceTablePath}
	} else {
		schema = ResourceSchema{Entity: "entity", Value: "value", SourceTable: randomSourceTablePath}
	}
	_, err := store.RegisterResourceFromSourceTable(id, schema)
	if err != nil {
		return err
	}
	return nil
}

func registerRandomResource(id ResourceID, store *SparkOfflineStore) error {
	randomSourceTablePath := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
	randomSourceData := []simpleTestStruct{{
		"a", 1, int64(1),
	}}
	if err := uploadCSVTable(store.Store, randomSourceTablePath, randomSourceData); err != nil {
		return err
	}
	schema := ResourceSchema{"entity", "value", "ts", randomSourceTablePath}
	_, err := store.RegisterResourceFromSourceTable(id, schema)
	if err != nil {
		return err
	}
	return nil
}

func sparkTestCreateGetOfflineTable(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Feature, Label)
	if err := registerRandomResource(id, store); err != nil {
		t.Fatalf("could not register random resource: %v", err)
	}
	if tab, err := store.GetResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func sparkTestOfflineTableAlreadyExists(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Feature, Label)
	if err := registerRandomResource(id, store); err != nil {
		t.Fatalf("could not register random resource: %v", err)
	}
	if err := registerRandomResource(id, store); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*TableAlreadyExists); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func sparkTestInvalidResourceIDs(t *testing.T, store *SparkOfflineStore) {
	invalidIds := []ResourceID{
		{Type: Feature},
		{Name: uuid.NewString()},
	}
	for _, id := range invalidIds {
		if err := registerRandomResource(id, store); err == nil {
			t.Fatalf("Succeeded in creating invalid ResourceID: %v", id)
		}
	}
}

type TestRecordTS struct {
	Entity string
	Value  string
	TS     time.Time
}

func sparkTestMaterializations(t *testing.T, store *SparkOfflineStore) {
	type TestCase struct {
		WriteRecords             []any
		Timestamp                bool
		Schema                   TableSchema
		ExpectedRows             int64
		SegmentStart, SegmentEnd int64
		ExpectedSegment          []any
	}

	schemaInt := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	tests := map[string]TestCase{
		"NoOverlap": {
			WriteRecords: []any{
				TestRecordTS{Entity: "a", Value: "1"},
				TestRecordTS{Entity: "b", Value: "2"},
				TestRecordTS{Entity: "c", Value: "3"},
			},
			Timestamp:    false,
			Schema:       schemaInt,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			// Have to expect int64(0) as it is the default value
			// if a resource does not have a set timestamp
			ExpectedSegment: []any{
				ResourceRecord{Entity: "a", Value: "1"},
				ResourceRecord{Entity: "b", Value: "2"},
				ResourceRecord{Entity: "c", Value: "3"},
			},
		},
		// "SubSegmentNoOverlap": {
		// 	WriteRecords: []any{
		// 		TestRecordTS{Entity: "a", Value: "1"},
		// 		TestRecordTS{Entity: "b", Value: "2"},
		// 		TestRecordTS{Entity: "c", Value: "3"},
		// 	},
		// 	Timestamp:    false,
		// 	Schema:       schemaInt,
		// 	ExpectedRows: 3,
		// 	SegmentStart: 1,
		// 	SegmentEnd:   2,
		// 	ExpectedSegment: []any{
		// 		ResourceRecord{Entity: "b", Value: "2"},
		// 	},
		// },
		// Added .UTC() b/c DeepEqual checks the timezone field of time.Time which can vary, resulting in false failures
		// during tests even if time is correct
		"SimpleChanges": {
			WriteRecords: []any{
				TestRecordTS{Entity: "a", Value: "1", TS: time.UnixMilli(int64(0)).UTC()},
				TestRecordTS{Entity: "b", Value: "2", TS: time.UnixMilli(int64(0)).UTC()},
				TestRecordTS{Entity: "c", Value: "3", TS: time.UnixMilli(int64(0)).UTC()},
				TestRecordTS{Entity: "a", Value: "4", TS: time.UnixMilli(int64(1)).UTC()},
			},
			Schema:       schemaInt,
			Timestamp:    true,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []any{
				ResourceRecord{Entity: "a", Value: "4", TS: time.UnixMilli(int64(1)).UTC()},
				ResourceRecord{Entity: "b", Value: "2", TS: time.UnixMilli(int64(0)).UTC()},
				ResourceRecord{Entity: "c", Value: "3", TS: time.UnixMilli(int64(0)).UTC()},
			},
		},
		"OutOfOrderWrites": {
			WriteRecords: []any{
				TestRecordTS{Entity: "a", Value: "1", TS: time.UnixMilli(int64(10)).UTC()},
				TestRecordTS{Entity: "b", Value: "2", TS: time.UnixMilli(int64(3)).UTC()},
				TestRecordTS{Entity: "c", Value: "3", TS: time.UnixMilli(int64(7)).UTC()},
				TestRecordTS{Entity: "c", Value: "9", TS: time.UnixMilli(int64(5)).UTC()},
				TestRecordTS{Entity: "a", Value: "4", TS: time.UnixMilli(int64(1)).UTC()},
			},
			Schema:       schemaInt,
			Timestamp:    true,
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []any{
				ResourceRecord{Entity: "a", Value: "1", TS: time.UnixMilli(int64(10)).UTC()},
				ResourceRecord{Entity: "b", Value: "2", TS: time.UnixMilli(int64(3)).UTC()},
				ResourceRecord{Entity: "c", Value: "3", TS: time.UnixMilli(int64(7)).UTC()},
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
				t.Fatalf("Value %v %T not found in materialization %v %T", actual, actual, expectedRows, expectedRows)
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
		id := sparkSafeRandomID(Feature)
		if err := registerRandomResourceGiveTable(id, store, test.WriteRecords, test.Timestamp); err != nil {
			t.Fatalf("Failed to create table: %s", err)
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			runTestCase(t, testConst)
		})
	}

}
func sparkTestInvalidMaterialization(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Label)
	if err := registerRandomResource(id, store); err != nil {
		t.Fatalf("could not register random resource: %v", err)
	}
	if _, err := store.CreateMaterialization(id); err == nil {
		t.Fatalf("Succeeded in materializing label")
	}
}

func sparkTestMaterializeUnknown(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Feature)
	if _, err := store.CreateMaterialization(id); err == nil {
		t.Fatalf("Succeeded in materializing uninitialized resource")
	}
}

func sparkTestMaterializationNotFound(t *testing.T, store *SparkOfflineStore) {
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

func sparkSafeRandomID(types ...OfflineResourceType) ResourceID {
	var t OfflineResourceType
	if len(types) == 0 {
		t = NoType
	} else if len(types) == 1 {
		t = types[0]
	} else {
		t = types[rand.Intn(len(types))]
	}
	return ResourceID{
		Name:    strings.ReplaceAll(uuid.NewString(), "-", ""),
		Variant: strings.ReplaceAll(uuid.NewString(), "-", ""),
		Type:    t,
	}
}

func testSparkSQLTransformation(t *testing.T, store *SparkOfflineStore) {
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
						Source:   "featureform_primary__test_name__14e4cd5e183d44968a6cf22f2f61d945",
					},
				},
			},
			ResourceID{"test_name", "14e4cd5e183d44968a6cf22f2f61d945", Primary},
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
						Source:   "featureform_primary__test_fake_name__test_fake_variant",
					},
				},
			},
			ResourceID{"test_name", "test_variant", Primary},
			true,
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			//TODO need to create a source table for checking
			err := store.CreateTransformation(ttConst.config)
			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("could not create transformation '%v' because %s", ttConst.config, err)
			}

			sourceTable, err := store.GetPrimaryTable(ttConst.sourceID)
			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("failed to get source table, %v,: %s", ttConst.sourceID, err)
			}

			transformationTable, err := store.GetTransformationTable(ttConst.config.TargetTableID)
			if err != nil {
				if ttConst.expectedFailure {
					return
				}
				t.Fatalf("failed to get the transformation, %s", err)
			}

			sourceCount, err := sourceTable.NumRows()
			transformationCount, err := transformationTable.NumRows()
			if !ttConst.expectedFailure && sourceCount != transformationCount {
				t.Fatalf("the source table and expected did not match: %v:%v", sourceCount, transformationCount)
			}

			// test transformation result rows are correct

			sourcePath := fileStoreResourcePath(ttConst.config.TargetTableID)

			updateConfig := TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name:    ttConst.config.TargetTableID.Name,
					Type:    Transformation,
					Variant: ttConst.config.TargetTableID.Variant,
				},
				Query: ttConst.config.Query,
				SourceMapping: []SourceMapping{
					SourceMapping{
						Template: ttConst.config.SourceMapping[0].Template,
						Source:   sourcePath,
					},
				},
			}

			err = store.UpdateTransformation(updateConfig)
			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("could not update transformation '%v' because %s", updateConfig, err)
			}

			updateTable, err := store.GetTransformationTable(updateConfig.TargetTableID)
			if err != nil {
				if ttConst.expectedFailure {
					return
				}
				t.Fatalf("failed to get the updated transformation, %s", err)
			}

			updateCount, err := updateTable.NumRows()
			if !ttConst.expectedFailure && updateCount != transformationCount {
				t.Fatalf("the source table and expected did not match: %v:%v", updateCount, transformationCount)
			}
		})
	}
}

func testUpdateQuery(t *testing.T, store *SparkOfflineStore) {
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
					Source:   "featureform_primary__test_name__test_variant",
				},
				SourceMapping{
					Template: "{{name2.variant2}}",
					Source:   "featureform_transformation__028f6213-77a8-43bb-9d91-dd7e9ee96102__test_variant",
				},
			},
			"SELECT * FROM source_0 and more source_1",
			[]string{
				"s3://featureform-spark-testing/featureform/testprimary/testFile.csv",
				"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384/",
			},
			false,
		},
		{
			"OneReplacementPass",
			"SELECT * FROM {{028f6213-77a8-43bb-9d91-dd7e9ee96102.test_variant}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{028f6213-77a8-43bb-9d91-dd7e9ee96102.test_variant}}",
					Source:   "featureform_transformation__028f6213-77a8-43bb-9d91-dd7e9ee96102__test_variant",
				},
			},
			"SELECT * FROM source_0",
			[]string{
				"s3://featureform-spark-testing/featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384/",
			},
			false,
		},
		{
			"ReplacementExpectedFailure",
			"SELECT * FROM {{name1.variant1}} and more {{name2.variant2}}",
			[]SourceMapping{
				SourceMapping{
					Template: "{{name1.variant1}}",
					Source:   "featureform_transformation__name1__variant1/",
				},
			},
			"SELECT * FROM source_0",
			[]string{
				"s3://featureform-bucket/featureform/Transformation/name1/variant1/file",
			},
			true,
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			t.Parallel()
			retreivedQuery, sources, err := store.updateQuery(ttConst.query, ttConst.sourceMap)

			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("Could not replace the template query: %v", err)
			}
			if !ttConst.expectedFailure && !reflect.DeepEqual(retreivedQuery, ttConst.expectedQuery) {
				t.Fatalf("updateQuery did not replace the query correctly. Expected \" %v \", got \" %v \".", ttConst.expectedQuery, retreivedQuery)
			}
			if !ttConst.expectedFailure && !reflect.DeepEqual(sources, ttConst.expectedSources) {
				t.Fatalf("updateQuery did not get the correct sources. Expected \" %v \", got \" %v \".", ttConst.expectedSources, sources)
			}
		})
	}
}

func testGetTransformation(t *testing.T, store *SparkOfflineStore) {
	cases := []struct {
		name             string
		id               ResourceID
		expectedRowCount int64
	}{
		{
			"testTransformation",
			ResourceID{
				Name:    "028f6213-77a8-43bb-9d91-dd7e9ee96102",
				Type:    Transformation,
				Variant: "test_variant",
			},
			10000,
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			table, err := store.GetTransformationTable(ttConst.id)
			if err != nil {
				t.Fatalf("Failed to get Transformation Table: %v", err)
			}

			caseNumRow, err := table.NumRows()
			if err != nil {
				t.Fatalf("Failed to get Transformation Table Num Rows: %v", err)
			}

			if caseNumRow != ttConst.expectedRowCount {
				t.Fatalf("Row count do not match. Expected \" %v \", got \" %v \".", ttConst.expectedRowCount, caseNumRow)
			}
		})
	}
}

func testGetSourcePath(t *testing.T, store *SparkOfflineStore) {
	cases := []struct {
		name            string
		sourcePath      string
		expectedPath    string
		expectedFailure bool
	}{
		{
			"PrimaryPathSuccess",
			"featureform_primary__test_name__14e4cd5e183d44968a6cf22f2f61d945",
			store.Store.PathWithPrefix("featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384/part-00000-9d3cb5a3-4b9c-4109-afa3-a75759bfcf89-c000.snappy.parquet", true),
			false,
		},
		{
			"TransformationPathSuccess",
			"featureform_transformation__028f6213-77a8-43bb-9d91-dd7e9ee96102__test_variant",
			store.Store.PathWithPrefix("featureform/Transformation/028f6213-77a8-43bb-9d91-dd7e9ee96102/test_variant/2022-08-19 17:37:36.546384", true),
			false,
		},
		{
			"PrimaryPathFailure",
			"featureform_primary__fake_name__fake_variant",
			"",
			true,
		},
		{
			"TransformationPathFailure",
			"featureform_transformation__fake_name__fake_variant",
			"",
			true,
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			retreivedPath, err := store.getSourcePath(ttConst.sourcePath)
			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("getSourcePath could not get the path because %s.", err)
			}

			if !ttConst.expectedFailure && !reflect.DeepEqual(ttConst.expectedPath, retreivedPath) {
				t.Fatalf("getSourcePath could not find the expected path. Expected \"%s\", got \"%s\".", ttConst.expectedPath, retreivedPath)
			}
		})
	}
}

func testGetResourceInformationFromFilePath(t *testing.T, store *SparkOfflineStore) {
	cases := []struct {
		name         string
		sourcePath   string
		expectedInfo []string
	}{
		{
			"PrimaryPathSuccess",
			"featureform_primary__test_name__test_variant",
			[]string{"primary", "test_name", "test_variant"},
		},
		{
			"TransformationPathSuccess",
			"featureform_transformation__028f6213-77a8-43bb-9d91-dd7e9ee96102__test_variant",
			[]string{"transformation", "028f6213-77a8-43bb-9d91-dd7e9ee96102", "test_variant"},
		},
		{
			"IncorrectPrimaryPath",
			"featureform_primary",
			[]string{"", "", ""},
		},
		{
			"IncorrectTransformationPath",
			"featureform_transformation__fake_028f6213",
			[]string{"", "", ""},
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			resourceType, resourceName, resourceVariant := store.getResourceInformationFromFilePath(ttConst.sourcePath)
			resourceInfo := []string{resourceType, resourceName, resourceVariant}

			if !reflect.DeepEqual(ttConst.expectedInfo, resourceInfo) {
				t.Fatalf("getSourcePath could not find the expected path. Expected \"%s\", got \"%s\".", ttConst.expectedInfo, resourceInfo)
			}
		})
	}
}

func testGetDFArgs(t *testing.T, store *SparkOfflineStore) {
	azureStore, ok := store.Store.(*SparkAzureFileStore)
	if !ok {
		t.Fatalf("could not case azure store")
	}

	cases := []struct {
		name            string
		outputURI       string
		code            string
		store_type      string
		mapping         []string
		expectedArgs    []string
		expectedFailure bool
	}{
		{
			"PrimaryPathSuccess",
			"featureform-spark-testing/featureform/Primary/test_name/test_variant",
			"code",
			"AzureBlobStore",
			[]string{
				"featureform-spark-testing/featureform/testprimary/testFile.csv",
			},
			[]string{
				"df",
				"--output_uri",
				"featureform-spark-testing/featureform/Primary/test_name/test_variant",
				"--code",
				"code",
				"--store_type",
				"azure_blob_store",
				"--spark_config",
				azureStore.configString(),
				"--credential",
				fmt.Sprintf("azure_connection_string=%s", azureStore.connectionString()),
				"--credential",
				fmt.Sprintf("azure_container_name=%s", azureStore.containerName()),
				"--source",
				store.Store.PathWithPrefix("featureform-spark-testing/featureform/testprimary/testFile.csv", true),
			},
			false,
		},
		{
			"FakePrimaryPath",
			"s3://featureform-spark-testing/featureform/Primary/test_name/test_variant",
			"code",
			"AzureBlobStore",
			[]string{
				"featureform_primary",
			},
			nil,
			true,
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			args, err := store.Executor.GetDFArgs(ttConst.outputURI, ttConst.code, ttConst.mapping, store.Store)

			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("could not get df args %s", err)
			}

			if !ttConst.expectedFailure && !reflect.DeepEqual(ttConst.expectedArgs, args) {
				t.Fatalf("getDFArgs could not generate the expected args. Expected \"%s\", got \"%s\".", ttConst.expectedArgs, args)
			}
		})
	}
}

func testTransformation(t *testing.T, store *SparkOfflineStore) {
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
						Source:   "featureform_primary__test_name__14e4cd5e183d44968a6cf22f2f61d945",
					},
				},
			},
			ResourceID{"test_name", "14e4cd5e183d44968a6cf22f2f61d945", Primary},
			false,
		},
		// {
		// 	"DFTransformationType",
		// 	TransformationConfig{
		// 		Type: DFTransformation,
		// 		TargetTableID: ResourceID{
		// 			Name:    uuid.NewString(),
		// 			Type:    Transformation,
		// 			Variant: "test_variant",
		// 		},
		// 		Query: "s3://featureform-spark-testing/featureform/DFTransformations/test_name/test_variant/transformation.pkl",
		// 		SourceMapping: []SourceMapping{
		// 			SourceMapping{
		// 				Template: "transaction",
		// 				Source:   "featureform_primary__test_name__test_variant",
		// 			},
		// 		},
		// 	},
		// 	ResourceID{"test_name", "test_variant", Primary},
		// 	false,
		// },
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

	for _, tt := range cases {
		ttConst := tt
		t.Run(ttConst.name, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			err := store.transformation(ttConst.config, false)
			if err != nil {
				if ttConst.expectedFailure {
					return
				}
				t.Fatalf("could not run transformation %s", err)
			}

			sourceTable, err := store.GetPrimaryTable(ttConst.sourceID)
			if !ttConst.expectedFailure && err != nil {
				t.Fatalf("failed to get source table, %v,: %s", ttConst.sourceID, err)
			}

			transformationTable, err := store.GetTransformationTable(ttConst.config.TargetTableID)
			if err != nil {
				if ttConst.expectedFailure {
					return
				}
				t.Fatalf("failed to get the transformation, %s", err)
			}

			sourceCount, err := sourceTable.NumRows()
			transformationCount, err := transformationTable.NumRows()
			if !ttConst.expectedFailure && sourceCount != transformationCount {
				t.Fatalf("the source table and expected did not match: %v:%v", sourceCount, transformationCount)
			}
		})
	}
}

// func getSparkOfflineStore(t *testing.T) (*SparkOfflineStore, error) {
// 	err := godotenv.Load("../.env")
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	emrConf := EMRConfig{
// 		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
// 		ClusterRegion:  os.Getenv("AWS_EMR_CLUSTER_REGION"),
// 		ClusterName:    os.Getenv("AWS_EMR_CLUSTER_ID"),
// 	}
// 	s3Conf := S3Config{
// 		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
// 		BucketRegion:   os.Getenv("S3_BUCKET_REGION"),
// 		BucketPath:     os.Getenv("S3_BUCKET_PATH"),
// 	}
// 	SparkOfflineConfig := SparkConfig{
// 		ExecutorType:   EMR,
// 		ExecutorConfig: emrConf,
// 		StoreType:      S3,
// 		StoreConfig:    s3Conf,
// 	}
// 	sparkSerializedConfig := SparkOfflineConfig.Serialize()
// 	sparkProvider, err := Get("SPARK_OFFLINE", sparkSerializedConfig)
// 	if err != nil {
// 		t.Fatalf("Could not create spark provider: %s", err)
// 	}
// 	sparkStore, err := sparkProvider.AsOfflineStore()
// 	if err != nil {
// 		t.Fatalf("Could not convert spark provider to offline store: %s", err)
// 	}
// 	sparkOfflineStore := sparkStore.(*SparkOfflineStore)

// 	return sparkOfflineStore, nil
// }

func getDatabricksOfflineStore(t *testing.T) (*SparkOfflineStore, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}
	databricksConfig := pc.DatabricksConfig{
		Username: helpers.GetEnv("DATABRICKS_USERNAME", ""),
		Password: helpers.GetEnv("DATABRICKS_PASSWORD", ""),
		Host:     helpers.GetEnv("DATABRICKS_HOST", ""),
		Token:    helpers.GetEnv("DATABRICKS_TOKEN", ""),
		Cluster:  helpers.GetEnv("DATABRICKS_CLUSTER", ""),
	}
	azureConfig := pc.AzureFileStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
		Path:          helpers.GetEnv("AZURE_CONTAINER_PATH", ""),
	}
	SparkOfflineConfig := pc.SparkConfig{
		ExecutorType:   pc.Databricks,
		ExecutorConfig: &databricksConfig,
		StoreType:      pc.Azure,
		StoreConfig:    &azureConfig,
	}

	sparkSerializedConfig, err := SparkOfflineConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize the SparkOfflineConfig")
	}

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

// func TestSparkConfigDeserialize(t *testing.T) {
// 	err := godotenv.Load("../.env")
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	emrConf := EMRConfig{
// 		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
// 		ClusterRegion:  os.Getenv("AWS_EMR_CLUSTER_REGION"),
// 		ClusterName:    os.Getenv("AWS_EMR_CLUSTER_ID"),
// 	}
// 	s3Conf := S3Config{
// 		AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
// 		BucketRegion:   os.Getenv("S3_BUCKET_REGION"),
// 		BucketPath:     os.Getenv("S3_BUCKET_PATH"),
// 	}
// 	correctSparkConfig := SparkConfig{
// 		ExecutorType:   "EMR",
// 		ExecutorConfig: EMRConfig{},
// 		StoreType:      "S3",
// 		StoreConfig:    S3Config{},
// 	}
// 	serializedConfig := correctSparkConfig.Serialize()
// 	reserializedConfig := SparkConfig{}
// 	if err := reserializedConfig.Deserialize(SerializedConfig(serializedConfig)); err != nil {
// 		t.Fatalf("error deserializing spark config")
// 	}
// 	invalidConfig := SerializedConfig("invalidConfig")
// 	invalidDeserialized := SparkConfig{}
// 	if err := invalidDeserialized.Deserialize(invalidConfig); err == nil {
// 		t.Fatalf("did not return error on deserializing improper config")
// 	}
// }

// func TestEMRConfigDeserialize(t *testing.T) {
// 	correctEMRConfig := EMRConfig{
// 		AWSAccessKeyId: "",
// 		AWSSecretKey:   "",
// 		ClusterRegion:  "us-east-1",
// 		ClusterName:    "example",
// 	}
// 	serializedConfig := correctEMRConfig.Serialize()
// 	reserializedConfig := EMRConfig{}
// 	if err := reserializedConfig.Deserialize(SerializedConfig(serializedConfig)); err != nil {
// 		t.Fatalf("error deserializing emr config")
// 	}
// 	invalidConfig := SerializedConfig("invalidConfig")
// 	invalidDeserialized := EMRConfig{}
// 	if err := invalidDeserialized.Deserialize(invalidConfig); err == nil {
// 		t.Fatalf("did not return error on deserializing improper config")
// 	}
// }

// func TestS3ConfigDeserialize(t *testing.T) {
// 	correctSparkConfig := S3Config{
// 		AWSAccessKeyId: "",
// 		AWSSecretKey:   "",
// 		BucketRegion:   "us-east-1",
// 		BucketPath:     "example",
// 	}
// 	serializedConfig := correctSparkConfig.Serialize()
// 	reserializedConfig := S3Config{}
// 	if err := reserializedConfig.Deserialize(SerializedConfig(serializedConfig)); err != nil {
// 		t.Fatalf("error deserializing spark config")
// 	}
// 	invalidConfig := SerializedConfig("invalidConfig")
// 	invalidDeserialized := S3Config{}
// 	if err := invalidDeserialized.Deserialize(invalidConfig); err == nil {
// 		t.Fatalf("did not return error on deserializing improper config")
// 	}
// }

func TestMaterializationCreate(t *testing.T) {
	t.Parallel()
	exampleSchemaWithTS := ResourceSchema{
		Entity: "entity",
		Value:  "value",
		TS:     "timestamp",
	}
	queries := defaultPythonOfflineQueries{}
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
	correctTSQuery := "SELECT entity AS entity, value AS value, 0 as ts, ROW_NUMBER() over (ORDER BY (SELECT NULL)) AS row_number FROM source_0"
	if correctTSQuery != materializeTSQuery {
		t.Fatalf("Materialize create did not produce correct query substituting timestamp")
	}
}

func TestTrainingSetCreate(t *testing.T) {
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
	queries := defaultPythonOfflineQueries{}
	trainingSetQuery := queries.trainingSetCreate(testTrainingSetDef, testFeatureSchemas, testLabelSchema)

	correctQuery := "SELECT Feature__test_feature_1__default, Feature__test_feature_2__default, Label__test_label__default " +
		"FROM (SELECT * FROM (SELECT *, row_number FROM (SELECT Feature__test_feature_1__default, Feature__test_feature_2__default, " +
		"value AS Label__test_label__default, entity, label_ts, t1_ts, t2_ts, ROW_NUMBER() over (PARTITION BY entity, value, label_ts ORDER BY " +
		"label_ts DESC, t1_ts DESC,t2_ts DESC) as row_number FROM ((SELECT * FROM (SELECT entity, value, label_ts FROM (SELECT entity AS entity, label_value " +
		"AS value, ts AS label_ts FROM source_0) t ) t0) LEFT OUTER JOIN (SELECT * FROM (SELECT entity as t1_entity, feature_value_1 as " +
		"Feature__test_feature_1__default, ts as t1_ts FROM source_1) ORDER BY t1_ts ASC) t1 ON (t1_entity = entity AND t1_ts <= label_ts) " +
		"LEFT OUTER JOIN (SELECT * FROM (SELECT entity as t2_entity, feature_value_2 as Feature__test_feature_2__default, ts as t2_ts " +
		"FROM source_2) ORDER BY t2_ts ASC) t2 ON (t2_entity = entity AND t2_ts <= label_ts)) tt) WHERE row_number=1 ))  ORDER BY label_ts"

	if trainingSetQuery != correctQuery {
		t.Fatalf("training set query not correct")
	}
}

// func TestCompareStructsFail(t *testing.T) {
// 	t.Parallel()
// 	type testStruct struct {
// 		Field string
// 	}
// 	firstStruct := testStruct{"first"}
// 	secondStruct := testStruct{"second"}
// 	if err := compareStructs(firstStruct, secondStruct); err == nil {
// 		t.Fatalf("failed to trigger error with unequal structs")
// 	}
// 	type similarStruct struct {
// 		Field int
// 	}
// 	firstStructSimilar := testStruct{"1"}
// 	secondStructSimilar := similarStruct{1}
// 	if err := compareStructs(firstStructSimilar, secondStructSimilar); err == nil {
// 		t.Fatalf("failed to trigger error when structs contain different types")
// 	}
// 	type testStructFields struct {
// 		Field      string
// 		OtherField int
// 	}
// 	firstStructFields := testStructFields{"1", 2}
// 	secondStructFields := testStruct{"1"}
// 	if err := compareStructs(firstStructFields, secondStructFields); err == nil {
// 		t.Fatalf("failed to trigger error when structs contain different types")
// 	}
// }

// func TestSparkExecutorFail(t *testing.T) {
// 	invalidConfig := EMRConfig{}
// 	invalidExecType := SparkExecutorType("invalid")
// 	logger := zap.NewExample().Sugar()
// 	if executor, err := NewSparkExecutor(invalidExecType, invalidConfig, logger); !(executor == nil && err == nil) {
// 		t.Fatalf("did not return nil on invalid exec type")
// 	}
// }

// func TestSparkStoreFail(t *testing.T) {
// 	invalidConfig := S3Config{}
// 	invalidExecType := SparkStoreType("invalid")
// 	logger := zap.NewExample().Sugar()
// 	if executor, err := NewSparkStore(invalidExecType, invalidConfig, logger); !(executor == nil && err != nil) {
// 		t.Fatalf("did not return nil on invalid exec type")
// 	}
// }

// func TestUnimplimentedFailures(t *testing.T) {
// 	store := SparkOfflineStore{}
// 	if table, err := store.CreatePrimaryTable(ResourceID{}, TableSchema{}); !(table == nil && err == nil) {
// 		t.Fatalf("did not return nil on calling unimplimented function")
// 	}
// 	if table, err := store.CreateResourceTable(ResourceID{}, TableSchema{}); !(table == nil && err == nil) {
// 		t.Fatalf("did not return nil on calling unimplimented function")
// 	}
// }

func sparkTestTrainingSet(t *testing.T, store *SparkOfflineStore) {
	type expectedTrainingRow struct {
		Features []interface{}
		Label    interface{}
	}
	type TestCase struct {
		FeatureRecords [][]TestRecordString
		LabelRecords   []TestRecordString
		ExpectedRows   []expectedTrainingRow
		FeatureSchema  []TableSchema
		LabelSchema    TableSchema
		Timestamp      bool
	}

	tests := map[string]TestCase{
		"SimpleJoin": {
			FeatureRecords: [][]TestRecordString{
				{
					{Entity: "a", Value: "1"},
					{Entity: "b", Value: "2"},
					{Entity: "c", Value: "3"},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "c", Value: "blue"},
				},
			},
			LabelRecords: []TestRecordString{
				{Entity: "a", Value: "true"},
				{Entity: "b", Value: "false"},
				{Entity: "c", Value: "true"},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						"1",
						"red",
					},
					Label: "true",
				},
				{
					Features: []interface{}{
						"2",
						"green",
					},
					Label: "false",
				},
				{
					Features: []interface{}{
						"3",
						"blue",
					},
					Label: "true",
				},
			},
			Timestamp: false,
		},
		"ComplexJoin": {
			FeatureRecords: [][]TestRecordString{
				// Overwritten feature.
				{
					{Entity: "a", Value: "1", TS: int64(0)},
					{Entity: "b", Value: "2", TS: int64(0)},
					{Entity: "c", Value: "3", TS: int64(0)},
					{Entity: "a", Value: "4", TS: int64(0)},
				},
				// Feature didn't exist since created after label
				{
					{Entity: "a", Value: "doesnt exist", TS: int64(11)},
				},
				// Feature didn't change after label
				{
					{Entity: "c", Value: "real value first", TS: int64(3)},
					{Entity: "c", Value: "overwritten", TS: int64(4)},
					{Entity: "c", Value: "real value second", TS: int64(5)},
				},
				// Different feature values for different TS.
				{
					{Entity: "b", Value: "first", TS: int64(3)},
					{Entity: "b", Value: "second", TS: int64(4)},
					{Entity: "b", Value: "third", TS: int64(8)},
				},
				// Feature after time.
				{
					{Entity: "a", Value: "first", TS: int64(12)},
				},
			},
			LabelRecords: []TestRecordString{
				{Entity: "a", Value: "1", TS: int64(10)},
				{Entity: "b", Value: "5", TS: int64(5)},
				{Entity: "b", Value: "9", TS: int64(3)},
				{Entity: "c", Value: "3", TS: int64(7)},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						"4", nil, nil, nil, nil,
					},
					Label: "1",
				},
				{
					Features: []interface{}{
						"2", nil, nil, "second", nil,
					},
					Label: "5",
				},
				{
					Features: []interface{}{
						"2", nil, nil, "first", nil,
					},
					Label: "9",
				},
				{
					Features: []interface{}{
						"3", nil, "real value second", nil, nil,
					},
					Label: "3",
				},
			},
			Timestamp: true,
		},
	}
	runTestCase := func(t *testing.T, test TestCase) {
		featureIDs := make([]ResourceID, len(test.FeatureRecords))

		for i, recs := range test.FeatureRecords {
			id := sparkSafeRandomID(Feature)
			featureIDs[i] = id
			if err := registerRandomResourceGiveTable(id, store, recs, test.Timestamp); err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
		}
		labelID := sparkSafeRandomID(Label)
		if err := registerRandomResourceGiveTable(labelID, store, test.LabelRecords, test.Timestamp); err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}

		def := TrainingSetDef{
			ID:       sparkSafeRandomID(TrainingSet),
			Label:    labelID,
			Features: featureIDs,
		}
		fmt.Println(def)
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
				if unorderedEqual(realRow.Features, expRow.Features) && realRow.Label == expRow.Label {
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Second * 15)
			runTestCase(t, testConst)
		})

	}
}

func sparkTestMaterializationUpdate(t *testing.T, store *SparkOfflineStore) {
	type TestCase struct {
		WriteRecords                           []any
		UpdateRecords                          []any
		Schema                                 TableSchema
		ExpectedRows                           int64
		UpdatedRows                            int64
		SegmentStart, SegmentEnd               int64
		UpdatedSegmentStart, UpdatedSegmentEnd int64
		ExpectedSegment                        []any
		ExpectedUpdate                         []any
		Timestamp                              bool
	}

	schemaInt := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: String},
			{Name: "value", ValueType: Int},
			{Name: "ts", ValueType: Timestamp},
		},
	}
	tests := map[string]TestCase{
		"NoOverlap": {
			WriteRecords: []any{
				TestRecordInt{Entity: "a", Value: 1},
				TestRecordInt{Entity: "b", Value: 2},
				TestRecordInt{Entity: "c", Value: 3},
			},
			UpdateRecords: []any{
				TestRecordInt{Entity: "a", Value: 1},
				TestRecordInt{Entity: "b", Value: 2},
				TestRecordInt{Entity: "c", Value: 3},
				TestRecordInt{Entity: "d", Value: 4},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   4,
			UpdatedRows:         4,
			// Have to expect int64(0) as it is the default value
			// if a resource does not have a set timestamp
			ExpectedSegment: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(0)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
			},
			ExpectedUpdate: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(0)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
				TestRecordInt{Entity: "d", Value: 4, TS: int64(0)},
			},
			Timestamp: false,
		},
		"SimpleOverwrite": {
			WriteRecords: []any{
				TestRecordInt{Entity: "a", Value: 1},
				TestRecordInt{Entity: "b", Value: 2},
				TestRecordInt{Entity: "c", Value: 3},
			},
			UpdateRecords: []any{
				TestRecordInt{Entity: "a", Value: 3},
				TestRecordInt{Entity: "b", Value: 4},
				TestRecordInt{Entity: "c", Value: 3},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(0)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
			},
			ExpectedUpdate: []any{
				TestRecordInt{Entity: "a", Value: 3, TS: int64(0)},
				TestRecordInt{Entity: "b", Value: 4, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
			},
			Timestamp: false,
		},
		// Added .UTC() b/c DeepEqual checks the timezone field of time.Time which can vary, resulting in false failures
		// during tests even if time is correct
		"SimpleChanges": {
			WriteRecords: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(0)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
			},
			UpdateRecords: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(0)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(4)},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []any{
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
			},
			ExpectedUpdate: []any{
				TestRecordInt{Entity: "b", Value: 2, TS: int64(0)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(0)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(4)},
			},
			Timestamp: true,
		},
		"OutOfOrderWrites": {
			WriteRecords: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(10)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(3)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
				TestRecordInt{Entity: "c", Value: 9, TS: int64(5)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
			},
			UpdateRecords: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(10)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(3)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
				TestRecordInt{Entity: "c", Value: 9, TS: int64(5)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
				TestRecordInt{Entity: "a", Value: 6, TS: int64(12)},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(10)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(3)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
			},
			ExpectedUpdate: []any{
				TestRecordInt{Entity: "a", Value: 6, TS: int64(12)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(3)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
			},
			Timestamp: true,
		},
		"OutOfOrderOverwrites": {
			WriteRecords: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(10)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(3)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
				TestRecordInt{Entity: "c", Value: 9, TS: int64(5)},
				TestRecordInt{Entity: "b", Value: 12, TS: int64(2)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
				TestRecordInt{Entity: "b", Value: 9, TS: int64(4)},
			},
			UpdateRecords: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(10)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(3)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
				TestRecordInt{Entity: "c", Value: 9, TS: int64(5)},
				TestRecordInt{Entity: "b", Value: 12, TS: int64(2)},
				TestRecordInt{Entity: "a", Value: 4, TS: int64(1)},
				TestRecordInt{Entity: "b", Value: 9, TS: int64(3)},
				TestRecordInt{Entity: "a", Value: 5, TS: int64(20)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(5)},
			},
			Schema:              schemaInt,
			ExpectedRows:        3,
			SegmentStart:        0,
			SegmentEnd:          3,
			UpdatedSegmentStart: 0,
			UpdatedSegmentEnd:   3,
			UpdatedRows:         3,
			ExpectedSegment: []any{
				TestRecordInt{Entity: "a", Value: 1, TS: int64(10)},
				TestRecordInt{Entity: "b", Value: 9, TS: int64(4)},
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
			},
			ExpectedUpdate: []any{
				TestRecordInt{Entity: "c", Value: 3, TS: int64(7)},
				TestRecordInt{Entity: "a", Value: 5, TS: int64(20)},
				TestRecordInt{Entity: "b", Value: 2, TS: int64(5)},
			},
			Timestamp: true,
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
		id := sparkSafeRandomID(Feature)
		randomPath := fmt.Sprintf("featureform/tests/source_table/%s/table.csv", strings.ReplaceAll(uuid.NewString(), "-", ""))
		if err := registerRandomResourceGiveTablePath(id, randomPath, store, test.WriteRecords, test.Timestamp); err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		mat, err := store.CreateMaterialization(id)
		if err != nil {
			t.Fatalf("Failed to create materialization: %s", err)
		}
		testMaterialization(t, mat, test)
		if err := uploadCSVTable(store.Store, randomPath, test.UpdateRecords); err != nil {
			t.Fatalf("Failed to overwrite source table with new records")
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			runTestCase(t, testConst)
		})
	}

}

type TestRecord struct {
	Entity string
	Value  interface{}
	TS     int64
}

func sparkTestTrainingSetUpdate(t *testing.T, store *SparkOfflineStore) {
	type expectedTrainingRow struct {
		Features []interface{}
		Label    interface{}
	}
	type TestCase struct {
		FeatureRecords        [][]TestRecord
		UpdatedFeatureRecords [][]TestRecord
		LabelRecords          []TestRecord
		UpdatedLabelRecords   []TestRecord
		ExpectedRows          []expectedTrainingRow
		UpdatedExpectedRows   []expectedTrainingRow
		FeatureSchema         []TableSchema
		LabelSchema           TableSchema
		Timestamp             bool
	}

	tests := map[string]TestCase{
		"SimpleJoin": {
			FeatureRecords: [][]TestRecord{
				{
					{Entity: "a", Value: "1"},
					{Entity: "b", Value: "2"},
					{Entity: "c", Value: "3"},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "c", Value: "blue"},
				},
			},
			UpdatedFeatureRecords: [][]TestRecord{
				{
					{Entity: "a", Value: "1"},
					{Entity: "b", Value: "2"},
					{Entity: "c", Value: "3"},
					{Entity: "d", Value: "4"},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "c", Value: "blue"},
					{Entity: "d", Value: "purple"},
				},
			},
			LabelRecords: []TestRecord{
				{Entity: "a", Value: "true"},
				{Entity: "b", Value: "false"},
				{Entity: "c", Value: "true"},
			},
			UpdatedLabelRecords: []TestRecord{
				{Entity: "a", Value: "true"},
				{Entity: "b", Value: "false"},
				{Entity: "c", Value: "true"},
				{Entity: "d", Value: "false"},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						"1",
						"red",
					},
					Label: "true",
				},
				{
					Features: []interface{}{
						"2",
						"green",
					},
					Label: "false",
				},
				{
					Features: []interface{}{
						"3",
						"blue",
					},
					Label: "true",
				},
			},
			UpdatedExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						"1",
						"red",
					},
					Label: "true",
				},
				{
					Features: []interface{}{
						"2",
						"green",
					},
					Label: "false",
				},
				{
					Features: []interface{}{
						"3",
						"blue",
					},
					Label: "true",
				},
				{
					Features: []interface{}{
						"4",
						"purple",
					},
					Label: "false",
				},
			},
			Timestamp: false,
		},
		"ComplexJoin": {
			FeatureRecords: [][]TestRecord{
				// Overwritten feature.
				{
					{Entity: "a", Value: "1", TS: int64(0)},
					{Entity: "b", Value: "2", TS: int64(0)},
					{Entity: "c", Value: "3", TS: int64(0)},
					{Entity: "a", Value: "4", TS: int64(0)},
				},
				// Feature didn't exist before label
				{
					{Entity: "a", Value: "doesnt exist", TS: int64(11)},
				},
				// Feature didn't change after label
				{
					{Entity: "c", Value: "real value first", TS: int64(5)},
					{Entity: "c", Value: "overwritten", TS: int64(4)},
					{Entity: "c", Value: "real value second", TS: int64(5)},
				},
				// Different feature values for different TS.
				{
					{Entity: "b", Value: "first", TS: int64(3)},
					{Entity: "b", Value: "second", TS: int64(4)},
					{Entity: "b", Value: "third", TS: int64(8)},
				},
				// After feature
				{
					{Entity: "a", Value: "1", TS: int64(12)},
				},
			},
			UpdatedFeatureRecords: [][]TestRecord{
				{
					{Entity: "a", Value: "1", TS: int64(0)},
					{Entity: "b", Value: "2", TS: int64(0)},
					{Entity: "c", Value: "3", TS: int64(0)},
					{Entity: "a", Value: "4", TS: int64(0)},
					{Entity: "a", Value: "5", TS: int64(0)},
				},
				{
					{Entity: "a", Value: "doesnt exist", TS: int64(11)},
				},
				{
					{Entity: "c", Value: "real value first", TS: int64(3)},
					{Entity: "c", Value: "overwritten", TS: int64(4)},
					{Entity: "c", Value: "real value second", TS: int64(5)},
				},
				{
					{Entity: "b", Value: "first", TS: int64(1)},
					{Entity: "b", Value: "second", TS: int64(2)},
					{Entity: "b", Value: "third", TS: int64(8)},
					{Entity: "b", Value: "zeroth", TS: int64(5)},
				},
				{
					{Entity: "a", Value: "1", TS: int64(12)},
				},
			},
			LabelRecords: []TestRecord{
				{Entity: "a", Value: "1", TS: int64(10)},
				{Entity: "b", Value: "9", TS: int64(3)},
				{Entity: "b", Value: "5", TS: int64(5)},
				{Entity: "c", Value: "3", TS: int64(7)},
			},
			UpdatedLabelRecords: []TestRecord{
				{Entity: "a", Value: "1", TS: int64(10)},
				{Entity: "b", Value: "5", TS: int64(3)},
				{Entity: "b", Value: "9", TS: int64(5)},
				{Entity: "c", Value: "3", TS: int64(7)},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						"4", nil, nil, nil, nil,
					},
					Label: "1",
				},
				{
					Features: []interface{}{
						"2", nil, nil, "first", nil,
					},
					Label: "9",
				},
				{
					Features: []interface{}{
						"2", nil, nil, "second", nil,
					},
					Label: "5",
				},
				{
					Features: []interface{}{
						"3", nil, "real value second", nil, nil,
					},
					Label: "3",
				},
			},
			UpdatedExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						"5", nil, nil, nil, nil,
					},
					Label: "1",
				},
				{
					Features: []interface{}{
						"2", nil, nil, "zeroth", nil,
					},
					Label: "9",
				},
				{
					Features: []interface{}{
						"2", nil, nil, "second", nil,
					},
					Label: "5",
				},
				{
					Features: []interface{}{
						"3", nil, "real value second", nil, nil,
					},
					Label: "3",
				},
			},
			Timestamp: true,
		},
	}
	runTestCase := func(t *testing.T, test TestCase) {
		featureIDs := make([]ResourceID, len(test.FeatureRecords))
		featureSourceTables := make([]string, 0)
		for i, recs := range test.FeatureRecords {
			id := sparkSafeRandomID(Feature)
			randomSourceTablePath := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
			featureSourceTables = append(featureSourceTables, randomSourceTablePath)
			featureIDs[i] = id
			if err := registerRandomResourceGiveTablePath(id, randomSourceTablePath, store, recs, test.Timestamp); err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
		}
		labelID := sparkSafeRandomID(Label)
		labelSourceTable := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
		if err := registerRandomResourceGiveTablePath(labelID, labelSourceTable, store, test.LabelRecords, test.Timestamp); err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}

		def := TrainingSetDef{
			ID:       sparkSafeRandomID(TrainingSet),
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
				if unorderedEqual(realRow.Features, expRow.Features) && realRow.Label == expRow.Label {
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
		for i, table := range featureSourceTables {
			if err := uploadCSVTable(store.Store, table, test.UpdatedFeatureRecords[i]); err != nil {
				t.Errorf("Could not update table: %v", table)
			}
		}

		if err := uploadCSVTable(store.Store, labelSourceTable, test.UpdatedLabelRecords); err != nil {
			t.Errorf("Could not update table: %v", labelSourceTable)
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
				if unorderedEqual(realRow.Features, expRow.Features) && realRow.Label == expRow.Label {
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			runTestCase(t, testConst)
		})
	}
}

func TestExecutors(t *testing.T) {
	logger := zap.NewExample().Sugar()

	testCases := []struct {
		name             string
		executorType     pc.SparkExecutorType
		executorConfig   pc.SparkExecutorConfig
		expectedExecutor SparkExecutor
	}{
		{
			"SparkGenericExecutor",
			pc.SparkGeneric,
			&pc.SparkGenericConfig{
				Master:        "local",
				DeployMode:    "client",
				PythonVersion: "3.7.16",
			},
			&SparkGenericExecutor{
				master:        "local",
				deployMode:    "client",
				pythonVersion: "3.7.16",
				logger:        logger,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			executor, err := NewSparkExecutor(tt.executorType, tt.executorConfig, logger)
			if err != nil {
				t.Fatalf("Could not replace the template query: %v", err)
			}

			if !reflect.DeepEqual(executor, tt.expectedExecutor) {
				t.Fatalf("ReplaceSourceName did not replace the query correctly.")
			}
		})
	}
}

func TestInitSparkS3(t *testing.T) {
	config := pc.S3FileStoreConfig{
		Credentials: pc.AWSCredentials{
			AWSSecretKey:   "",
			AWSAccessKeyId: "",
		},
		BucketRegion: "abc",
		BucketPath:   "abc",
		Path:         "abc",
	}
	serializedConfig, err := config.Serialize()
	if err != nil {
		fmt.Errorf("Could not serialize config: %v", err)
	}
	_, err = NewSparkS3FileStore(serializedConfig)
	if err != nil {
		t.Errorf("Could not initialize store: %v", err)
	}
}

func TestInitSparkAzure(t *testing.T) {
	config := pc.AzureFileStoreConfig{
		AccountName:   "",
		AccountKey:    "asbc",
		ContainerName: "asdf",
		Path:          "/",
	}
	serializedConfig, err := config.Serialize()
	if err != nil {
		fmt.Errorf("Could not serialize config: %v", err)
	}
	_, err = NewAzureFileStore(serializedConfig)
	if err != nil {
		t.Errorf("Could not initialize store: %v", err)
	}
}

func TestCreateLogS3FileStore(t *testing.T) {
	type TestCase struct {
		Region      string
		LogLocation string
		AccessKey   string
		SecretKey   string
	}

	tests := map[string]TestCase{
		"USEast1S3": {
			Region:      "us-east-1",
			LogLocation: "s3://bucket/elasticmapreduce/",
			AccessKey:   "accessKey",
			SecretKey:   "secretKey",
		},
		"USEast1S3a": {
			Region:      "us-east-1",
			LogLocation: "s3a://bucket/elasticmapreduce/",
			AccessKey:   "accessKey",
			SecretKey:   "secretKey",
		},
		"USWest1S3": {
			Region:      "us-west-1",
			LogLocation: "s3://bucket/elasticmapreduce/",
			AccessKey:   "accessKey",
			SecretKey:   "secretKey",
		},
		"USWest1S3a": {
			Region:      "us-west-1",
			LogLocation: "s3a://bucket/elasticmapreduce/",
			AccessKey:   "accessKey",
			SecretKey:   "secretKey",
		},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		_, err := createLogS3FileStore(test.Region, test.LogLocation, test.AccessKey, test.SecretKey)
		if err != nil {
			t.Fatalf("could not create S3 Filestore: %v", err)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runTestCase(t, test)
		})
	}
}

func TestEMRErrorMessages(t *testing.T) {
	bucketName := helpers.GetEnv("S3_BUCKET_PATH", "")
	emr, s3, err := createEMRAndS3(bucketName)
	if err != nil {
		t.Fatalf("could not create emr and/or s3 connection: %v", err)
	}

	localScriptPath := "scripts/spark/tests/test_files/scripts/test_emr_error.py"
	remoteScriptPath := "unit_tests/scripts/tests/test_emr_error.py"
	err = readAndUploadFile(localScriptPath, remoteScriptPath, s3)
	if err != nil {
		t.Fatalf("could not upload '%s' to '%s': %v", localScriptPath, remoteScriptPath, err)
	}

	type TestCase struct {
		ErrorMessage         string
		ExpectedErrorMessage string
	}

	tests := map[string]TestCase{
		"ErrorBoring": {
			ErrorMessage:         "Boring",
			ExpectedErrorMessage: "ERROR: Boring",
		},
		"NoErrorMessage": {
			ErrorMessage:         "",
			ExpectedErrorMessage: "There is an Error",
		},
	}

	remoteScriptPathWithPrefix := fmt.Sprintf("s3://%s/%s", bucketName, remoteScriptPath)
	args := []string{
		"spark-submit",
		"--deploy-mode",
		"client",
		remoteScriptPathWithPrefix,
	}

	runTestCase := func(t *testing.T, test TestCase) {
		runArgs := append(args, test.ErrorMessage)
		err := emr.RunSparkJob(runArgs, s3)
		if err == nil {
			t.Fatal("job did not failed as expected")
		}

		errAsString := fmt.Sprintf("%s", err)
		scriptError := strings.Split(errAsString, "failed: Exception:")[1]
		errorMessage := strings.Trim(scriptError, " ")

		if errorMessage != test.ExpectedErrorMessage {
			t.Fatalf("did not get the expected error message: expected '%s' but got '%s'", test.ExpectedErrorMessage, errorMessage)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runTestCase(t, test)
		})
	}
}

func createEMRAndS3(bucketName string) (SparkExecutor, SparkFileStore, error) {
	awsAccessKeyId := helpers.GetEnv("AWS_ACCESS_KEY_ID", "")
	awsSecretKey := helpers.GetEnv("AWS_SECRET_KEY", "")

	bucketRegion := helpers.GetEnv("S3_BUCKET_REGION", "us-east-1")

	s3Config := pc.S3FileStoreConfig{
		Credentials:  pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awsSecretKey},
		BucketRegion: bucketRegion,
		BucketPath:   bucketName,
		Path:         "unit_tests",
	}

	config, err := s3Config.Serialize()
	if err != nil {
		return nil, nil, fmt.Errorf("could not serialize S3 config: %v", err)
	}
	s3, err := NewSparkS3FileStore(config)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create new S3 file store: %v", err)
	}

	emrRegion := helpers.GetEnv("AWS_EMR_CLUSTER_REGION", "us-east-1")
	emrClusterId := helpers.GetEnv("AWS_EMR_CLUSTER_ID", "")

	emrConfig := pc.EMRConfig{
		Credentials:   pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awsSecretKey},
		ClusterRegion: emrRegion,
		ClusterName:   emrClusterId,
	}

	logger := logging.NewLogger("spark-unit-tests")
	emr, err := NewEMRExecutor(emrConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create new EMR executor: %v", err)
	}
	return emr, s3, nil
}
