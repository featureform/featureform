// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/featureform/provider/provider_type"

	"github.com/stretchr/testify/assert"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	fs "github.com/featureform/filestore"
	"github.com/featureform/metadata"
	"github.com/featureform/provider/spark"

	"bytes"
	"encoding/csv"
	random "math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"

	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	"github.com/featureform/provider/types"
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
	destination, err := store.CreateFilePath(path, false)
	if err != nil {
		return fmt.Errorf("could not create file path: %v", err)
	}
	if err := store.Write(destination, buf.Bytes()); err != nil {
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
	fp, err := store.Store.CreateFilePath(path, false)
	if err != nil {
		return err
	}
	testResourceSchema := ResourceSchema{"name", "age", "registered", pl.NewFileLocation(fp)}
	testFeatureResource := sparkSafeRandomID(Feature)
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
	testLabelResourceSchema := ResourceSchema{"name", "winner", "registered", pl.NewFileLocation(fp)}
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
	fp, err := store.Store.CreateFilePath(path, false)
	if err != nil {
		return err
	}
	fpLocation := pl.NewFileLocation(fp)
	testResourceName := "test_name_materialize"
	testResourceVariant := uuid.New().String()
	testResource := ResourceID{testResourceName, testResourceVariant, Feature}
	testResourceSchema := ResourceSchema{"name", "age", "registered", fpLocation}
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
	testResourceMaterializationID := MaterializationID(
		fmt.Sprintf(
			"%s/%s/%s",
			FeatureMaterialization,
			testResourceName,
			testResourceVariant,
		),
	)
	materialization, err := store.CreateMaterialization(testResource, MaterializationOptions{Output: fs.Parquet})
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
	if fetchedMaterialization.ID() != MaterializationID(
		fmt.Sprintf(
			"Materialization/%s/%s",
			testResourceName,
			testResourceVariant,
		),
	) {
		return fmt.Errorf(
			"materialization id not correct, expected Materialization/test_name_materialize/test_variant, got %s",
			fetchedMaterialization.ID(),
		)
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
		return fmt.Errorf(
			"Feature iterator had wrong number of iterations. Expected %d, got %d",
			numRowsFirst,
			iterations,
		)
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
		return fmt.Errorf(
			"Feature iterator had wrong number of iterations. Expected %d, got %d",
			numRowsSecond,
			iterations,
		)
	}
	for _, rec := range comparisonList {
		if !reflect.DeepEqual(rec, correctMaterialization[rec.Entity]) {
			return fmt.Errorf(
				"Wrong materialization entry: %v does not equal %v",
				rec.Value,
				correctMaterialization[rec.Entity].Value,
			)
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
	fp, err := store.Store.CreateFilePath(path, false)
	if err != nil {
		return err
	}
	fpLocation := pl.NewFileLocation(fp)
	resourceVariantName := uuid.New().String()
	testResource := ResourceID{"test_name", resourceVariantName, Feature}
	testResourceSchema := ResourceSchema{"name", "age", "registered", fpLocation}
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
	fp, err := store.Store.CreateFilePath(path, false)
	if err != nil {
		return err
	}
	primaryVariantName := uuid.New().String()
	testResource := ResourceID{"test_name", primaryVariantName, Primary}
	table, err := store.RegisterPrimaryFromSourceTable(testResource, pl.NewFileLocation(fp))
	if err != nil {
		return err
	}
	fetchedTable, err := store.GetPrimaryTable(testResource, metadata.SourceVariant{})
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
		t.Skip("skipping integration tests")
	}
	// emrSparkOfflineStore, err := getSparkOfflineStore(t)
	// if err != nil {
	// 	t.Fatalf("could not get SparkOfflineStore: %s", err)
	// }
	databricksSparkOfflineStore := GetTestingS3Databricks(t)
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

	t.Run(
		"SPARK_STORE_FUNCTIONS", func(t *testing.T) {
			for name, testFn := range testFns {
				nameConst := name
				testFnConst := testFn
				for name, store := range sparkStores {
					storeNameConst := name
					storeConst := store
					t.Run(
						fmt.Sprintf("%s_%s", nameConst, storeNameConst), func(t *testing.T) {
							t.Parallel()
							testFnConst(t, storeConst)
						},
					)
				}
			}
		},
	)

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

	filePath, err := store.Store.CreateFilePath(randomSourceTablePath, false)
	if err != nil {
		t.Fatalf("could not create file path: %v", err)
	}
	_, err = store.RegisterPrimaryFromSourceTable(primaryID, pl.NewFileLocation(filePath))
	if err != nil {
		t.Fatalf("Could not register from Source Table: %s", err)
	}
	_, err = store.GetPrimaryTable(primaryID, metadata.SourceVariant{})
	if err != nil {
		t.Fatalf("Could not get primary table: %v", err)
	}
	_, err = store.RegisterPrimaryFromSourceTable(primaryID, pl.NewFileLocation(filePath))
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
	filePath, err := store.Store.CreateFilePath(randomSourceTablePath, false)
	if err != nil {
		t.Fatalf("could not create file path: %v", err)
	}
	_, err = store.RegisterPrimaryFromSourceTable(primaryID, pl.NewFileLocation(filePath))
	if err != nil {
		t.Fatalf("Could not register from Source Table: %s", err)
	}
	_, err = store.GetPrimaryTable(primaryID, metadata.SourceVariant{})
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
	} else if _, valid := err.(*fferr.TrainingSetNotFoundError); !valid {
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
		t.Run(
			nameConst, func(t *testing.T) {
				t.Parallel()
				time.Sleep(time.Second * 15)
				if err := store.CreateTrainingSet(defConst); err == nil {
					t.Fatalf("Succeeded to create invalid def")
				}
			},
		)
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
	} else if casted, valid := err.(*fferr.DatasetNotFoundError); !valid {
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

func registerRandomResourceGiveTablePath(
	id ResourceID,
	path string,
	store *SparkOfflineStore,
	table interface{},
	timestamp bool,
) error {
	if err := uploadCSVTable(store.Store, path, table); err != nil {
		return err
	}
	fp, err := store.Store.CreateFilePath(path, false)
	if err != nil {
		return err
	}
	fpLocation := pl.NewFileLocation(fp)
	var schema ResourceSchema
	if timestamp {
		schema = ResourceSchema{"entity", "value", "ts", fpLocation}
	} else {
		schema = ResourceSchema{Entity: "entity", Value: "value", SourceTable: fpLocation}
	}
	_, regErr := store.RegisterResourceFromSourceTable(id, schema)
	if regErr != nil {
		return regErr
	}
	return nil
}

func registerRandomResourceGiveTable(id ResourceID, store *SparkOfflineStore, table interface{}, timestamp bool) error {
	randomSourceTablePath := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
	if err := uploadCSVTable(store.Store, randomSourceTablePath, table); err != nil {
		return err
	}
	fp, err := store.Store.CreateFilePath(randomSourceTablePath, false)
	if err != nil {
		return err
	}
	fpLocation := pl.NewFileLocation(fp)

	var schema ResourceSchema
	if timestamp {
		schema = ResourceSchema{"entity", "value", "ts", fpLocation}
	} else {
		schema = ResourceSchema{Entity: "entity", Value: "value", SourceTable: fpLocation}
	}
	_, regErr := store.RegisterResourceFromSourceTable(id, schema)
	if regErr != nil {
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
	fp, err := store.Store.CreateFilePath(randomSourceTablePath, false)
	if err != nil {
		return err
	}
	fpLocation := pl.NewFileLocation(fp)
	schema := ResourceSchema{"entity", "value", "ts", fpLocation}
	_, regErr := store.RegisterResourceFromSourceTable(id, schema)
	if regErr != nil {
		return regErr
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
	} else if casted, valid := err.(*fferr.DatasetAlreadyExistsError); !valid {
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
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
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
		mat, err := store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet})
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
		t.Run(
			nameConst, func(t *testing.T) {
				t.Parallel()
				time.Sleep(time.Second * 15)
				runTestCase(t, testConst)
			},
		)
	}

}
func sparkTestInvalidMaterialization(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Label)
	if err := registerRandomResource(id, store); err != nil {
		t.Fatalf("could not register random resource: %v", err)
	}
	if _, err := store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet}); err == nil {
		t.Fatalf("Succeeded in materializing label")
	}
}

func sparkTestMaterializeUnknown(t *testing.T, store *SparkOfflineStore) {
	id := sparkSafeRandomID(Feature)
	if _, err := store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet}); err == nil {
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
	var notFoundErr *fferr.DatasetNotFoundError
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
		t = types[random.Intn(len(types))]
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
		t.Run(
			ttConst.name, func(t *testing.T) {
				t.Parallel()
				time.Sleep(time.Second * 15)
				//TODO need to create a source table for checking
				err := store.CreateTransformation(ttConst.config)
				if !ttConst.expectedFailure && err != nil {
					t.Fatalf("could not create transformation '%v' because %s", ttConst.config, err)
				}

				sourceTable, err := store.GetPrimaryTable(ttConst.sourceID, metadata.SourceVariant{})
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
				sourcePath := ps.ResourceToDirectoryPath(
					ttConst.config.TargetTableID.Type.String(),
					ttConst.config.TargetTableID.Name,
					ttConst.config.TargetTableID.Variant,
				)

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
			},
		)
	}
}

func testPrepareQueryForSpark(t *testing.T, store *SparkOfflineStore) {
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
		t.Run(
			ttConst.name, func(t *testing.T) {
				t.Parallel()
				retreivedQuery, sources, err := store.prepareQueryForSpark(ttConst.query, ttConst.sourceMap)

				if !ttConst.expectedFailure && err != nil {
					t.Fatalf("Could not replace the template query: %v", err)
				}
				if !ttConst.expectedFailure && !reflect.DeepEqual(retreivedQuery, ttConst.expectedQuery) {
					t.Fatalf(
						"updateQuery did not replace the query correctly. Expected \" %v \", got \" %v \".",
						ttConst.expectedQuery,
						retreivedQuery,
					)
				}
				if !ttConst.expectedFailure && !reflect.DeepEqual(sources, ttConst.expectedSources) {
					t.Fatalf(
						"updateQuery did not get the correct sources. Expected \" %v \", got \" %v \".",
						ttConst.expectedSources,
						sources,
					)
				}
			},
		)
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
		t.Run(
			ttConst.name, func(t *testing.T) {
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
					t.Fatalf(
						"Row count do not match. Expected \" %v \", got \" %v \".",
						ttConst.expectedRowCount,
						caseNumRow,
					)
				}
			},
		)
	}
}

func testGetResourceInformationFromFilePath(t *testing.T, store *SparkOfflineStore) {
	cases := []struct {
		name         string
		sourcePath   string
		expectedInfo []string
		expectedErr  bool
	}{
		{
			"PrimaryPathSuccess",
			"featureform_primary__test_name__test_variant",
			[]string{"Primary", "test_name", "test_variant"},
			false,
		},
		{
			"TransformationPathSuccess",
			"featureform_transformation__028f6213-77a8-43bb-9d91-dd7e9ee96102__test_variant",
			[]string{"Transformation", "028f6213-77a8-43bb-9d91-dd7e9ee96102", "test_variant"},
			false,
		},
		{
			"IncorrectPrimaryPath",
			"featureform_primary",
			[]string{"", "", ""},
			true,
		},
		{
			"IncorrectTransformationPath",
			"featureform_transformation__fake_028f6213",
			[]string{"", "", ""},
			true,
		},
	}

	for _, tt := range cases {
		ttConst := tt
		t.Run(
			ttConst.name, func(t *testing.T) {
				t.Parallel()
				time.Sleep(time.Second * 15)
				resourceType, resourceName, resourceVariant, err := ps.TableNameToResource(ttConst.sourcePath)
				if err != nil && !ttConst.expectedErr {
					t.Fatalf("getSourcePath could not get the path because %s.", err)
				}
				if err == nil && ttConst.expectedErr {
					t.Fatalf("getSourcePath should have failed but did not.")
				}
				resourceInfo := []string{resourceType, resourceName, resourceVariant}

				if !reflect.DeepEqual(ttConst.expectedInfo, resourceInfo) {
					t.Fatalf(
						"getSourcePath could not find the expected path. Expected \"%s\", got \"%s\".",
						ttConst.expectedInfo,
						resourceInfo,
					)
				}
			},
		)
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
				"--sources",
				"featureform-spark-testing/featureform/testprimary/testFile.csv",
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
		t.Run(
			ttConst.name, func(t *testing.T) {
				output, err := store.Store.CreateFilePath(ttConst.outputURI, false)
				if err != nil {
					t.Fatalf("could not create output path %s", err)
				}
				args, err := sparkScriptCommandDef{
					DeployMode:     types.SparkClientDeployMode,
					TFType:         DFTransformation,
					OutputLocation: pl.NewFileLocation(output),
					Code:           ttConst.code,
					SourceList:     spark.WrapLegacySourceInfos(ttConst.mapping),
					JobType:        types.Transform,
					Store:          store.Store,
					Mappings:       make([]SourceMapping, 0),
				}.PrepareCommand(logging.NewTestLogger(t))

				if !ttConst.expectedFailure && err != nil {
					t.Fatalf("could not get df args %s", err)
				}

				if !ttConst.expectedFailure && !reflect.DeepEqual(ttConst.expectedArgs, args) {
					t.Fatalf(
						"getDFArgs could not generate the expected args. Expected \"%s\", got \"%s\".",
						ttConst.expectedArgs,
						args,
					)
				}
			},
		)
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
		t.Run(
			ttConst.name, func(t *testing.T) {
				t.Parallel()
				time.Sleep(time.Second * 15)
				err := store.transformation(ttConst.config, false, nil)
				if err != nil {
					if ttConst.expectedFailure {
						return
					}
					t.Fatalf("could not run transformation %s", err)
				}

				sourceTable, err := store.GetPrimaryTable(ttConst.sourceID, metadata.SourceVariant{})
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
			},
		)
	}
}

// func getSparkOfflineStore(t *testing.T) (*SparkOfflineStore, error) {
// 	err := godotenv.Load("../.env")
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	emrConf := EMRConfig{
// 		AccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		SecretKey:   os.Getenv("AWS_SECRET_KEY"),
// 		ClusterRegion:  os.Getenv("AWS_EMR_CLUSTER_REGION"),
// 		ClusterName:    os.Getenv("AWS_EMR_CLUSTER_ID"),
// 	}
// 	s3Conf := S3Config{
// 		AccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		SecretKey:   os.Getenv("AWS_SECRET_KEY"),
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
// 	sparkProvider, err := Get(pt.SparkOffline, sparkSerializedConfig)
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

// Unit tests

// func TestSparkConfigDeserialize(t *testing.T) {
// 	err := godotenv.Load("../.env")
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	emrConf := EMRConfig{
// 		AccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		SecretKey:   os.Getenv("AWS_SECRET_KEY"),
// 		ClusterRegion:  os.Getenv("AWS_EMR_CLUSTER_REGION"),
// 		ClusterName:    os.Getenv("AWS_EMR_CLUSTER_ID"),
// 	}
// 	s3Conf := S3Config{
// 		AccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
// 		SecretKey:   os.Getenv("AWS_SECRET_KEY"),
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
// 		AccessKeyId: "",
// 		SecretKey:   "",
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
// 		AccessKeyId: "",
// 		SecretKey:   "",
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

	correctQuery := "SELECT `Feature__test_feature_1__default`, `Feature__test_feature_2__default`, `Label__test_label__default` " +
		"FROM (SELECT * FROM (SELECT *, row_number FROM (SELECT `Feature__test_feature_1__default`, `Feature__test_feature_2__default`, " +
		"value AS `Label__test_label__default`, entity, label_ts, t1_ts, t2_ts, ROW_NUMBER() over (PARTITION BY entity, value, label_ts ORDER BY " +
		"label_ts DESC, t1_ts DESC,t2_ts DESC) as row_number FROM ((SELECT * FROM (SELECT entity, value, label_ts FROM (SELECT entity AS entity, label_value " +
		"AS value, ts AS label_ts FROM source_0) t ) t0) LEFT OUTER JOIN (SELECT * FROM (SELECT entity as t1_entity, feature_value_1 as " +
		"`Feature__test_feature_1__default`, ts as t1_ts FROM source_1) ORDER BY t1_ts ASC) t1 ON (t1_entity = entity AND t1_ts <= label_ts) " +
		"LEFT OUTER JOIN (SELECT * FROM (SELECT entity as t2_entity, feature_value_2 as `Feature__test_feature_2__default`, ts as t2_ts " +
		"FROM source_2) ORDER BY t2_ts ASC) t2 ON (t2_entity = entity AND t2_ts <= label_ts)) tt) WHERE row_number=1 ))  ORDER BY label_ts"

	if trainingSetQuery != correctQuery {
		t.Fatalf("training set query not correct, got %s, expected %s", trainingSetQuery, correctQuery)
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
		t.Run(
			nameConst, func(t *testing.T) {
				t.Parallel()
				time.Sleep(time.Second * 15)
				runTestCase(t, testConst)
			},
		)

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
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
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
		randomPath := fmt.Sprintf(
			"featureform/tests/source_table/%s/table.csv",
			strings.ReplaceAll(uuid.NewString(), "-", ""),
		)
		if err := registerRandomResourceGiveTablePath(
			id,
			randomPath,
			store,
			test.WriteRecords,
			test.Timestamp,
		); err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		opts := MaterializationOptions{Output: fs.Parquet}
		mat, err := store.CreateMaterialization(id, opts)
		if err != nil {
			t.Fatalf("Failed to create materialization: %s", err)
		}
		testMaterialization(t, mat, test)
		if err := uploadCSVTable(store.Store, randomPath, test.UpdateRecords); err != nil {
			t.Fatalf("Failed to overwrite source table with new records")
		}
		mat, err = store.UpdateMaterialization(id, opts)
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
		t.Run(
			nameConst, func(t *testing.T) {
				t.Parallel()
				runTestCase(t, testConst)
			},
		)
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
			if err := registerRandomResourceGiveTablePath(
				id,
				randomSourceTablePath,
				store,
				recs,
				test.Timestamp,
			); err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
		}
		labelID := sparkSafeRandomID(Label)
		labelSourceTable := fmt.Sprintf("featureform/tests/source_tables/%s/table.csv", uuid.NewString())
		if err := registerRandomResourceGiveTablePath(
			labelID,
			labelSourceTable,
			store,
			test.LabelRecords,
			test.Timestamp,
		); err != nil {
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
		t.Run(
			nameConst, func(t *testing.T) {
				t.Parallel()
				runTestCase(t, testConst)
			},
		)
	}
}

func TestExecutors(t *testing.T) {
	logger := logging.NewTestLogger(t)
	base, err := newBaseExecutor()
	if err != nil {
		t.Fatalf("Failed to create base executor: %s", err)
	}

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
				baseExecutor:  base,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(
			tt.name, func(t *testing.T) {
				executor, err := NewSparkExecutor(tt.executorType, tt.executorConfig, logger)
				if err != nil {
					t.Fatalf("Could not replace the template query: %v", err)
				}

				if !reflect.DeepEqual(executor, tt.expectedExecutor) {
					t.Fatalf("ReplaceSourceName did not replace the query correctly.")
				}
			},
		)
	}
}

func TestInitSparkS3(t *testing.T) {
	config := pc.S3FileStoreConfig{
		Credentials: pc.AWSStaticCredentials{
			SecretKey:   "",
			AccessKeyId: "",
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
	config := &pc.AzureFileStoreConfig{
		AccountName:   "xyz",
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
		Region            string
		LogLocation       string
		AccessKey         string
		SecretKey         string
		UseServiceAccount bool
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
		_, err := createLogS3FileStore(
			test.Region,
			test.LogLocation,
			test.AccessKey,
			test.SecretKey,
			test.UseServiceAccount,
		)
		if err != nil {
			t.Fatalf("could not create S3 Filestore: %v", err)
		}
	}

	for name, test := range tests {
		t.Run(
			name, func(t *testing.T) {
				t.Parallel()
				runTestCase(t, test)
			},
		)
	}
}

func TestEMRErrorMessages(t *testing.T) {

	t.Skip("extremely long running needs to be resolved")
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}

	bucketName := helpers.GetEnv("S3_BUCKET_PATH", "")
	emr, s3, err := createEMRAndS3(bucketName)
	if err != nil {
		t.Fatalf("could not create emr and/or s3 connection: %v", err)
	}

	localScriptPath := &fs.LocalFilepath{}
	if err := localScriptPath.SetKey("scripts/spark/integration_test_scripts/test_emr_error.py"); err != nil {
		t.Fatalf("could not set local script path: %v", err)
	}
	remoteScriptPath, err := s3.CreateFilePath("unit_tests/scripts/tests/test_emr_error.py", false)
	if err != nil {
		t.Fatalf("could not create remote script path: %v", err)
	}
	err = readAndUploadFile(localScriptPath, remoteScriptPath, s3)
	if err != nil {
		t.Fatalf("could not upload '%s' to '%s': %v", localScriptPath.ToURI(), remoteScriptPath.ToURI(), err)
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

	runTestCase := func(t *testing.T, test TestCase) {
		cmd := &spark.Command{
			Script: remoteScriptPath,
			ScriptArgs: []string{
				test.ErrorMessage,
			},
			Configs: spark.Configs{
				spark.DeployFlag{
					Mode: types.SparkClientDeployMode,
				},
			},
		}
		err := emr.RunSparkJob(cmd, s3, SparkJobOptions{}, nil)
		if err == nil {
			t.Fatal("job did not failed as expected")
		}

		errAsString := fmt.Sprintf("%s", err)
		scriptError := strings.Split(errAsString, "failed: Exception:")

		// Throw an error if we don't find the script error
		if len(scriptError) != 2 {
			t.Fatalf("could not find script error: %v", errAsString)
		}
		errorMessage := strings.Trim(scriptError[1], " ")

		if !strings.Contains(errorMessage, test.ExpectedErrorMessage) {
			t.Fatalf(
				"did not get the expected error message: expected '%s' but got '%s'",
				test.ExpectedErrorMessage,
				errorMessage,
			)
		}
	}

	for name, test := range tests {
		t.Run(
			name, func(t *testing.T) {
				t.Parallel()
				runTestCase(t, test)
			},
		)
	}
}

func createEMRAndS3(bucketName string) (SparkExecutor, SparkFileStore, error) {
	AccessKeyId := helpers.GetEnv("AWS_ACCESS_KEY_ID", "")
	SecretKey := helpers.GetEnv("AWS_SECRET_KEY", "")

	bucketRegion := helpers.GetEnv("S3_BUCKET_REGION", "us-east-1")

	s3Config := pc.S3FileStoreConfig{
		Credentials:  pc.AWSStaticCredentials{AccessKeyId: AccessKeyId, SecretKey: SecretKey},
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
		Credentials:   pc.AWSStaticCredentials{AccessKeyId: AccessKeyId, SecretKey: SecretKey},
		ClusterRegion: emrRegion,
		ClusterName:   emrClusterId,
	}
	logger := logging.NewLoggerWithLevel("spark-unit-tests", logging.DebugLevel)
	emr, err := NewEMRExecutor(emrConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create new EMR executor: %v", err)
	}
	return emr, s3, nil
}

func TestCreateSparkFileStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	type args struct {
		name   string
		config Config
	}
	tests := []struct {
		name    string
		args    args
		want    SparkFileStore
		wantErr bool
	}{
		{"Invalid Storage Provider", args{"invalid", Config{}}, nil, true},
		{"Invalid Config", args{"S3", Config{}}, nil, true},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := CreateSparkFileStore(fs.FileStoreType(tt.args.name), nil, tt.args.config)
				if (err != nil) != tt.wantErr {
					t.Errorf("CreateSparkFileStore() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("CreateSparkFileStore() got = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestLocalSparkS3GlueStore(t *testing.T) {
	t.Skip("Need to get all env vars setup")
	err := godotenv.Load("../.env")

	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	credentials := pc.AWSStaticCredentials{
		AccessKeyId: helpers.GetEnv("AWS_ACCESS_KEY_ID", ""),
		SecretKey:   helpers.GetEnv("AWS_SECRET_KEY", ""),
	}

	sparkConfig := &pc.SparkConfig{
		ExecutorType: pc.EMR,
		ExecutorConfig: &pc.EMRConfig{
			Credentials:   credentials,
			ClusterRegion: helpers.GetEnv("AWS_EMR_ICEBERG_CLUSTER_REGION", "us-east-1"),
			ClusterName:   helpers.GetEnv("AWS_EMR_ICEBERG_CLUSTER_ID", ""),
		},
		StoreType: fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials:  credentials,
			BucketRegion: helpers.GetEnv("AWS_EMR_ICEBERG_CLUSTER_ID", "us-east-1"),
			BucketPath:   "ali-aws-lake-house-iceberg-blog-demo",
			Path:         "",
		},
		GlueConfig: &pc.GlueConfig{
			Warehouse: helpers.GetEnv("AWS_GLUE_WAREHOUSE", ""),
			Region:    helpers.GetEnv("AWS_GLUE_REGION", "us-east-1"),
		},
	}

	sparkSerializedConfig, _ := sparkConfig.Serialize()
	sparkProvider, err := Get("SPARK_OFFLINE", sparkSerializedConfig)
	if err != nil {
		t.Fatalf("Could not create spark provider: %s", err)
	}
	sparkStore, err := sparkProvider.AsOfflineStore()
	if err != nil {
		t.Fatalf("Could not convert spark provider to offline store: %s", err)
	}

	randomId := uuidWithoutDashes()
	variant := fmt.Sprintf("version-%s", randomId)
	resourceId := ResourceID{Name: "TEST_ICEBERG", Variant: variant, Type: Primary}
	tbl, err := sparkStore.RegisterPrimaryFromSourceTable(
		resourceId,
		pl.NewCatalogLocation("ff", "transactions2", "iceberg"),
	)
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}
	t.Logf("Table created: %v", tbl)

	err = sparkStore.CreateTransformation(
		TransformationConfig{
			Type:          SQLTransformation,
			TargetTableID: ResourceID{Name: "code-sql-tf", Variant: variant},
			Query:         "SELECT * FROM {{test_name.test_variant}}",
			SourceMapping: []SourceMapping{
				{
					Template: "{{test_name.test_variant}}",
					Source:   fmt.Sprintf("featureform_primary__%s__%s", resourceId.Name, resourceId.Variant),
				},
			},
		},
	)

	if err != nil {
		t.Fatalf("Could not create transformation: %s", err)

	}
}

func TestSparkFileStoreV2Config(t *testing.T) {
	script, err := filestore.NewEmptyFilepath(filestore.S3)
	if err != nil {
		t.Fatalf("Failed to create empty file path: %s", err)
	}
	script.SetScheme(filestore.S3Prefix)
	script.SetBucket("test")
	script.SetKey("script.py")
	testCases := []struct {
		name          string
		store         SparkFileStoreV2
		ExpectedFlags []string
	}{
		{
			name:          "Local No Args",
			store:         SparkLocalFileStore{LocalFileStore: &LocalFileStore{}},
			ExpectedFlags: []string{"spark-submit", "s3://test/script.py"},
		},
		{
			name: "S3 static creds",
			store: SparkS3FileStore{
				S3FileStore: &S3FileStore{
					Credentials: pc.AWSStaticCredentials{
						AccessKeyId: "TEST-ACCESS",
						SecretKey:   "TEST-SECRET",
					},
					Bucket:       "test-bucket",
					BucketRegion: "us-east-1",
					Path:         "path",
				},
			},
			ExpectedFlags: []string{
				"spark-submit", "s3://test/script.py",
				"--spark_config", "\"spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\"",
				"--credential", "\"aws_bucket_name=test-bucket\"",
				"--credential", "\"aws_region=us-east-1\"",
				"--store_type", "s3",
				"--spark_config", "\"fs.s3a.endpoint=s3.us-east-1.amazonaws.com\"",
				"--spark_config", "\"fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\"",
				"--spark_config", "\"fs.s3a.access.key=TEST-ACCESS\"",
				"--spark_config", "\"fs.s3a.secret.key=TEST-SECRET\"",
				"--credential", "\"aws_access_key_id=TEST-ACCESS\"",
				"--credential", "\"aws_secret_access_key=TEST-SECRET\"",
			},
		},
		{
			name: "S3 assume creds",
			store: SparkS3FileStore{
				S3FileStore: &S3FileStore{
					Credentials:  pc.AWSAssumeRoleCredentials{},
					Bucket:       "test-bucket",
					BucketRegion: "us-east-1",
					Path:         "path",
				},
			},
			ExpectedFlags: []string{
				"spark-submit", "s3://test/script.py",
				"--spark_config", "\"spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\"",
				"--credential", "\"aws_bucket_name=test-bucket\"",
				"--credential", "\"aws_region=us-east-1\"",
				"--store_type", "s3",
				"--spark_config", "\"fs.s3a.endpoint=s3.us-east-1.amazonaws.com\"",
				"--credential", "\"use_service_account=true\"",
			},
		},
		{
			name: "S3 with Glue",
			store: SparkGlueS3FileStore{
				GlueConfig: &pc.GlueConfig{
					Warehouse:   "s3://bucket/warehouse",
					Region:      "us-east-3",
					TableFormat: pc.Iceberg,
				},
				SparkS3FileStore: SparkS3FileStore{
					S3FileStore: &S3FileStore{
						Credentials: pc.AWSStaticCredentials{
							AccessKeyId: "TEST-ACCESS",
							SecretKey:   "TEST-SECRET",
						},
						Bucket:       "test-bucket",
						BucketRegion: "us-east-1",
						Path:         "path",
					},
				},
			},
			ExpectedFlags: []string{
				"spark-submit",
				"--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
				"s3://test/script.py",
				"--spark_config", "\"spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\"",
				"--credential", "\"aws_bucket_name=test-bucket\"",
				"--credential", "\"aws_region=us-east-1\"",
				"--store_type", "s3",
				"--spark_config", "\"fs.s3a.endpoint=s3.us-east-1.amazonaws.com\"",
				"--spark_config", "\"fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\"",
				"--spark_config", "\"fs.s3a.access.key=TEST-ACCESS\"",
				"--spark_config", "\"fs.s3a.secret.key=TEST-SECRET\"",
				"--credential", "\"aws_access_key_id=TEST-ACCESS\"",
				"--credential", "\"aws_secret_access_key=TEST-SECRET\"",
				"--spark_config", "\"spark.sql.catalog.ff_catalog.region=us-east-3\"",
				"--spark_config", "\"spark.sql.catalog.ff_catalog=org.apache.iceberg.spark.SparkCatalog\"",
				"--spark_config", "\"spark.sql.catalog.ff_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog\"",
				"--spark_config", "\"spark.sql.catalog.ff_catalog.warehouse=s3://bucket/warehouse\"",
				"--spark_config", "\"spark.sql.catalog.ff_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO\"",
				"--spark_config", "\"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\"",
			},
		},
	}
	for _, tt := range testCases {
		t.Run(
			tt.name, func(t *testing.T) {
				cfgs := tt.store.SparkConfigs()
				flags := cfgs.CompileCommand(script)
				assert.ElementsMatchf(
					t,
					flags,
					tt.ExpectedFlags,
					"SparkFileStore.SparkConfig() = %#v, want %#v",
					flags,
					tt.ExpectedFlags,
				)
			},
		)
	}
}

func TestSparkGenericExecutor_getYarnCommand(t *testing.T) {
	type fields struct {
		master        string
		deployMode    string
		pythonVersion string
		coreSite      string
		yarnSite      string
		logger        logging.Logger
	}
	type args struct {
		args string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"Basic",
			fields{
				master:        "yarn",
				deployMode:    "cluster",
				pythonVersion: "3.7.16",
				coreSite:      "core-site.xml",
				yarnSite:      "yarn-site.xml",
				logger:        logging.NewTestLogger(t),
			},
			args{"--arg1 --arg2"},
			`^pyenv global 3\\.7\\.16 && export HADOOP_CONF_DIR=(.+) &&  pyenv exec --arg1 --arg2; rm -r (.+)$`,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {

				base, err := newBaseExecutor()
				if err != nil {
					t.Fatalf("failed to create base executor: %v", err)
				}
				s := &SparkGenericExecutor{
					master:        tt.fields.master,
					deployMode:    tt.fields.deployMode,
					pythonVersion: tt.fields.pythonVersion,
					coreSite:      tt.fields.coreSite,
					yarnSite:      tt.fields.yarnSite,
					logger:        tt.fields.logger,
					baseExecutor:  base,
				}
				got, err := s.getYarnCommand(tt.args.args)
				if (err != nil) != tt.wantErr {
					t.Errorf("getYarnCommand() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				// Uses regex to see if everything matches except the filepath since its generated
				re := regexp.MustCompile(tt.want)
				matches := re.FindStringSubmatch(got)
				if len(matches) == 3 {
					t.Errorf("getYarnCommand() got = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestSparkGenericExecutorArgs(t *testing.T) {
	// type SubmitArgs struct {
	// 	DestPath   string
	// 	Query      string
	// 	SourceList []string
	// 	JobType    JobType
	// }
	// type DFArgs struct {
	// 	DestPath string
	// 	Code     string
	// 	Sources  []string
	// }

	// logger := logging.NewTestLogger(t)
	// wd, err := os.Getwd()
	// if err != nil {
	// 	t.Fatalf("error getting working directory: %v", err)
	// }
	// dirPath := fmt.Sprintf("{\"DirPath\": \"file:///%s/\"}", wd)
	// fileStore, err := NewLocalFileStore([]byte(dirPath))
	// if err != nil {
	// 	t.Fatalf("error creating local file store: %v", err)
	// }
	// localFileStore := fileStore.(*LocalFileStore)

	// store := SparkLocalFileStore{
	// 	LocalFileStore: localFileStore,
	// }

	// base, err := newBaseExecutor()
	// if err != nil {
	// 	t.Fatalf("failed to create base executor: %v", err)
	// }

	// testCases := []struct {
	// 	name                  string
	// 	executor              SparkExecutor
	// 	SubmitArgs            SubmitArgs
	// 	DFArgs                DFArgs
	// 	ExpectedPythonFileURI filestore.Filepath
	// 	ExpectedSubmitArgs    []string
	// 	ExpectedDFArgs        []string
	// }{
	// {
	// 	name: "Generic",
	// 	executor: &SparkGenericExecutor{
	// 		master:       "yarn",
	// 		deployMode:   "cluster",
	// 		logger:       logging.NewTestLogger(t),
	// 		baseExecutor: base,
	// 	},
	// 	SubmitArgs: SubmitArgs{
	// 		DestPath:   "path/to/dest",
	// 		Query:      "SELECT * FROM table",
	// 		SourceList: []string{"source1", "source2"},
	// 		JobType:    Materialize,
	// 	},
	// 	DFArgs: DFArgs{
	// 		DestPath: "path/to/dest",
	// 		Code:     "code",
	// 		Sources:  []string{"source1", "source2"},
	// 	},
	// 	ExpectedPythonFileURI: nil,
	// 	ExpectedSubmitArgs:    []string{"spark-submit", "--deploy-mode", "cluster", "--master", "yarn", base.files.LocalScriptPath, "sql", "--output", "{\"outputLocation\":\"file:///path/to/dest\",\"locationType\":\"filestore\"}", "--sql_query", "'SELECT * FROM table'", "--job_type", "'Materialization'", "--store_type", "local", "--sources", "source1", "source2"},
	// 	ExpectedDFArgs:        []string{"spark-submit", "--deploy-mode", "cluster", "--master", "yarn", base.files.LocalScriptPath, "df", "--output", "{\"outputLocation\":\"file:///path/to/dest\",\"locationType\":\"filestore\"}", "--code", "code", "--store_type", "local", "--sources", "source1", "source2"},
	// },
	// {
	// 	name:     "Databricks",
	// 	executor: &DatabricksExecutor{},
	// 	SubmitArgs: SubmitArgs{
	// 		DestPath:   "path/to/dest",
	// 		Query:      "SELECT * FROM table",
	// 		SourceList: []string{"source1", "source2"},
	// 		JobType:    Materialize,
	// 	},
	// 	DFArgs: DFArgs{
	// 		OutputURI: "path/to/output",
	// 		Code:      "code",
	// 		Sources:   []string{"source1", "source2"},
	// 	},
	// 	ExpectedPythonFileURI: "/featureform/scripts/spark/offline_store_spark_runner.py",
	// 	ExpectedSubmitArgs:    []string{"sql", "--output_uri", "path/to/dest", "--sql_query", "SELECT * FROM table", "--job_type", "Materialization", "--store_type", "local", "--sources", "source1", "source2"},
	// 	ExpectedDFArgs:        []string{"df", "--output_uri", "path/to/output", "--code", "code", "--store_type", "local", "--source", "source1", "source2"},
	// },
	// {
	// 	name: "EMR",
	// 	executor: &EMRExecutor{
	// 		logger: zaptest.NewLogger(t).Sugar(),
	// 	},
	// 	SubmitArgs: SubmitArgs{
	// 		DestPath:   "path/to/dest",
	// 		Query:      "SELECT * FROM table",
	// 		SourceList: []string{"source1", "source2"},
	// 		JobType:    Materialize,
	// 	},
	// 	DFArgs: DFArgs{
	// 		OutputURI: "path/to/output",
	// 		Code:      "code",
	// 		Sources:   []string{"source1", "source2"},
	// 	},
	// 	ExpectedPythonFileURI: "",
	// 	ExpectedSubmitArgs:    []string{"spark-submit", "--deploy-mode", "client", "/featureform/scripts/spark/offline_store_spark_runner.py", "sql", "--output_uri", "/path/to/dest", "--sql_query", "SELECT * FROM table", "--job_type", "Materialization", "--store_type", "local", "--sources", "source1", "source2"},
	// 	ExpectedDFArgs:        []string{"spark-submit", "--deploy-mode", "client", "/featureform/scripts/spark/offline_store_spark_runner.py", "df", "--output_uri", "/path/to/output", "--code", "code", "--store_type", "local", "--source", "source1", "source2"},
	// },
	// }
	// for _, tt := range testCases {
	// 	t.Run(
	// 		tt.name, func(t *testing.T) {
	// 			pythonURI, err := sparkPythonFileURI(store, logger)
	// 			if err != nil {
	// 				t.Errorf("SparkExecutor.PythonFileURI() = %#v, want %#v", err, nil)
	// 			}
	// 			if !reflect.DeepEqual(pythonURI, tt.ExpectedPythonFileURI) {
	// 				t.Errorf("SparkExecutor.PythonFileURI() = %#v, want %#v", pythonURI, tt.ExpectedPythonFileURI)
	// 			}
	// 			destination, err := store.CreateFilePath(tt.SubmitArgs.DestPath, true)
	// 			if err != nil {
	// 				t.Errorf("SparkExecutor.CreateFilePath() = %#v, want %#v", err, nil)
	// 			}
	// 			submitArgs, err := tt.executor.SparkSubmitArgs(
	// 				types.SparkClientDeployMode,
	// 				SQLTransformation,
	// 				pl.NewFileLocation(destination),
	// 				tt.SubmitArgs.Query,
	// 				spark.WrapLegacySourceInfos(tt.SubmitArgs.SourceList),
	// 				tt.SubmitArgs.JobType,
	// 				store,
	// 				make([]SourceMapping, 0),
	// 			)
	// 			if err != nil {
	// 				t.Errorf("SparkExecutor.SparkSubmitArgs() = %#v, want %#v", err, nil)
	// 			}
	// 			if !reflect.DeepEqual(submitArgs, tt.ExpectedSubmitArgs) {
	// 				t.Errorf("SparkExecutor.SubmitArgs() = %#v, want %#v", submitArgs, tt.ExpectedSubmitArgs)
	// 			}
	// 			output, err := store.CreateFilePath(tt.DFArgs.DestPath, true)
	// 			if err != nil {
	// 				t.Errorf("SparkExecutor.CreateFilePath() = %#v, want %#v", err, nil)
	// 			}
	// 			dfArgs, err := tt.executor.SparkSubmitArgs(
	// 				types.SparkClientDeployMode,
	// 				DFTransformation,
	// 				pl.NewFileLocation(output),
	// 				tt.DFArgs.Code,
	// 				spark.WrapLegacySourceInfos(tt.DFArgs.Sources),
	// 				Transform,
	// 				store,
	// 				make([]SourceMapping, 0),
	// 			)
	// 			if err != nil {
	// 				t.Errorf("SparkExecutor.GetDFArgs() = %#v, want %#v", err, nil)
	// 			}
	// 			if !reflect.DeepEqual(dfArgs, tt.ExpectedDFArgs) {
	// 				t.Errorf("SparkExecutor.GetDFArgs() = %#v, want %#v", dfArgs, tt.ExpectedDFArgs)
	// 			}
	// 		},
	// 	)
	// }
}

func TestExceedsSubmitParamsTotalByteLimit(t *testing.T) {
	script, err := filestore.NewEmptyFilepath(filestore.S3)
	if err != nil {
		t.Fatalf("Failed to create empty file path: %s", err)
	}
	script.SetScheme(filestore.S3Prefix)
	script.SetBucket("bucket")
	script.SetKey("featureform/Feature/t_name/t_variant")
	testCases := []struct {
		name                 string
		cmd                  *spark.Command
		shouldExceedAPILimit bool
	}{
		{
			name: "SubmitParamsWithinLimit",
			cmd: &spark.Command{
				Script:     script,
				ScriptArgs: []string{"sql"},
				Configs:    nil,
			},
			shouldExceedAPILimit: false,
		},
		{
			name: "SubmitParamsExceedsLimit",
			cmd: &spark.Command{
				Script:     script,
				ScriptArgs: []string{"sql"},
				Configs: spark.Configs{
					spark.SqlQueryFlag{
						CleanQuery: randomStringNBytes(5_000, t),
						Sources: spark.WrapLegacySourceInfos([]string{
							randomStringNBytes(1_000, t),
							randomStringNBytes(1_000, t),
							randomStringNBytes(1_000, t),
							randomStringNBytes(1_000, t),
							randomStringNBytes(1_000, t),
						}),
					},
				},
			},
			shouldExceedAPILimit: true,
		},
	}

	for _, tt := range testCases {
		t.Run(
			tt.name, func(t *testing.T) {
				actual := exceedsSubmitParamsTotalByteLimit(tt.cmd)
				if actual != tt.shouldExceedAPILimit {
					t.Fatalf("Expected %v, got %v", tt.shouldExceedAPILimit, actual)
				}
			},
		)
	}
}

func TestNewSparkFileStores(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NewSparkFileStores tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}

	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}

	directoryPath := fmt.Sprintf("%s/scripts/k8s/tests/test_files/output/go_tests", mydir)
	_ = os.MkdirAll(directoryPath, os.ModePerm)

	localStoreConfig := pc.LocalFileStoreConfig{DirPath: fmt.Sprintf(`file:///%s`, directoryPath)}
	localStoreConfigSerialized, err := localStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("Could not serialize local store config: %v", err)
	}
	_, err = NewSparkLocalFileStore(localStoreConfigSerialized)
	if err != nil {
		t.Fatalf("Could not create local store: %v", err)
	}

	s3StoreConfig := pc.S3FileStoreConfig{
		Credentials: pc.AWSStaticCredentials{
			SecretKey:   "",
			AccessKeyId: "",
		},
		BucketRegion: "abc",
		BucketPath:   "abc",
		Path:         "abc",
	}
	s3StoreConfigSerialized, err := s3StoreConfig.Serialize()
	if err != nil {
		t.Fatalf("Could not serialize s3 store config: %v", err)
	}
	_, err = NewSparkS3FileStore(s3StoreConfigSerialized)
	if err != nil {
		t.Fatalf("Could not create s3 store: %v", err)
	}

	credsFile := os.Getenv("GCP_CREDENTIALS_FILE")
	content, err := os.ReadFile(credsFile)
	if err != nil {
		t.Errorf("Error when opening file: %v", err)
	}
	var creds map[string]interface{}
	err = json.Unmarshal(content, &creds)
	if err != nil {
		t.Errorf("Error during Unmarshal() creds: %v", err)
	}

	gcsStoreConfig := pc.GCSFileStoreConfig{
		BucketName: os.Getenv("GCS_BUCKET_NAME"),
		BucketPath: "",
		Credentials: pc.GCPCredentials{
			ProjectId: os.Getenv("GCP_PROJECT_ID"),
			JSON:      creds,
		},
	}
	gcsStoreConfigSerialized, err := gcsStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("Could not serialize gcs store config: %v", err)
	}
	_, err = NewSparkGCSFileStore(gcsStoreConfigSerialized)
	if err != nil {
		t.Fatalf("Could not create gcs store: %v", err)
	}

	hdfsStoreConfig := pc.HDFSFileStoreConfig{
		Host:           "localhost",
		Port:           "9000",
		CredentialType: pc.BasicCredential,
		CredentialConfig: &pc.BasicCredentialConfig{
			Username: "hduser",
		},
	}
	hdfsStoreConfigSerialized, err := hdfsStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("Could not serialize hdfs store config: %v", err)
	}
	_, err = NewSparkHDFSFileStore(hdfsStoreConfigSerialized)
	if err != nil {
		t.Fatalf("Could not create hdfs store: %v", err)
	}
}

func TestEmrResumeID(t *testing.T) {
	t.Run("Invalid IDs", func(t *testing.T) {
		tests := []emrResumeID{
			{},
			{ClusterID: "abc"},
			{StepID: "abc"},
		}
		for _, test := range tests {
			id, err := test.Marshal()
			if err == nil {
				t.Fatalf("Succeeded to marshal %v into %s", test, id)
			}
		}
	})

	validID := emrResumeID{ClusterID: "a", StepID: "b"}
	resumeID, err := validID.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal %v: %s", validID, err)
	}
	parsedID, err := deserializeEMRResumeID(resumeID)
	if err != nil {
		t.Fatalf("Failed to deserialize %v: %s", validID, err)
	}
	if validID != *parsedID {
		t.Fatalf("IDs don't match %v %v", validID, *parsedID)
	}
}

func randomStringNBytes(size int, t *testing.T) string {
	randomBytes := make([]byte, size)
	_, err := rand.Read(randomBytes)
	if err != nil {
		t.Fatalf("Error generating random bytes: %v", err)
	}
	return base64.StdEncoding.EncodeToString(randomBytes)
}

func TestSuiteSparkExecutorTransforms(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping spark executor integration tests")
	}
	t.Parallel()
	bucketName := helpers.MustGetTestingEnv(t, "S3_BUCKET_PATH")
	emr, s3, err := createEMRAndS3(bucketName)
	if err != nil {
		t.Fatalf("Failed to create EMR and S3: %s", err)
	}
	testInfra := []struct {
		Executor  SparkExecutor
		FileStore SparkFileStoreV2
	}{
		{emr, s3},
	}
	testSuite := map[string]struct {
		fn func(*testing.T, SparkExecutor, SparkFileStoreV2)
	}{
		"TestResume":           {fn: testRunAndResume},
		"TestReadWriteIceberg": {fn: createIcebergIntegrationTest().Run},
		// // TODO fix this
		// // "TestReadWriteDelta": {fn: testReadWriteDelta},
		"TestReadWriteDynamo": {fn: createDynamoIntegrationTest().Run},
		"TestFeatureQuery":    {fn: createFeatureQueryTest().Run},
		// // TODO handle non-TS duplicates
		"TestMaterialize": {fn: createMaterializeTest().Run},
		// "TestKafka":       {fn: createKafkaTest().Run},
	}

	for _, infra := range testInfra {
		// If we don't do this, we run into bugs due to how t.Parallel works
		constInfra := infra
		for name, test := range testSuite {
			// If we don't do this, we run into bugs due to how t.Parallel works
			constTest := test
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				constTest.fn(t, constInfra.Executor, constInfra.FileStore)
			})
		}
	}
}

func testRunAndResume(t *testing.T, executor SparkExecutor, sfs SparkFileStoreV2) {
	localScriptPath := &fs.LocalFilepath{}
	if err := localScriptPath.SetKey("scripts/spark/integration_test_scripts/test_noop_5sec.py"); err != nil {
		t.Fatalf("could not set local script path: %v", err)
	}
	remoteScriptPath, err := sfs.CreateFilePath("unit_tests/scripts/tests/test_noop_5sec.py", false)
	if err != nil {
		t.Fatalf("could not create remote script path: %v", err)
	}
	if err := readAndUploadFile(localScriptPath, remoteScriptPath, sfs); err != nil {
		t.Fatalf("could not upload '%s' to '%s': %v", localScriptPath.ToURI(), remoteScriptPath.ToURI(), err)
	}

	cmd := &spark.Command{
		Script: remoteScriptPath,
		Configs: spark.Configs{
			spark.DeployFlag{
				Mode: types.SparkClientDeployMode,
			},
		},
	}
	maxWait := time.Hour * 2
	jobOpts := SparkJobOptions{MaxJobDuration: maxWait, JobName: "testRunAndResume"}

	// First test them async
	t.Run("Resume running task", func(t *testing.T) {
		t.Parallel()
		asyncOpt := RunAsyncWithResume(maxWait)
		if err := executor.RunSparkJob(cmd, sfs, jobOpts, []TransformationOption{asyncOpt}); err != nil {
			t.Fatalf("Failed to run no-op with resume: %s", err)
		}
		resumeOpt, err := ResumeOptionWithID(asyncOpt.ResumeID(), maxWait)
		if err != nil {
			t.Fatalf("Failed to create resume option: %s", err)
		}
		if err := executor.RunSparkJob(cmd, sfs, jobOpts, []TransformationOption{resumeOpt}); err != nil {
			t.Fatalf("Failed to run no-op with resume: %s", err)
		}
		fmt.Println("Waiting for resume opt")
		if err := resumeOpt.Wait(); err != nil {
			t.Fatalf("ResumeOpt failed with resume: %s", err)
		}
		fmt.Println("Waiting for async opt")
		if err := asyncOpt.Wait(); err != nil {
			t.Fatalf("AsyncOpt failed with resume: %s", err)
		}
	})

	t.Run("Resume finished task", func(t *testing.T) {
		t.Parallel()
		asyncOpt := RunAsyncWithResume(maxWait)
		if err := executor.RunSparkJob(cmd, sfs, jobOpts, []TransformationOption{asyncOpt}); err != nil {
			t.Fatalf("Failed to run no-op with resume: %s", err)
		}
		// Finish the task first, resume after
		fmt.Println("waiting for async opt")
		if err := asyncOpt.Wait(); err != nil {
			t.Fatalf("AsyncOpt failed with resume: %s", err)
		}
		resumeOpt, err := ResumeOptionWithID(asyncOpt.ResumeID(), maxWait)
		if err != nil {
			t.Fatalf("Failed to create resume option: %s", err)
		}
		if err := executor.RunSparkJob(cmd, sfs, jobOpts, []TransformationOption{resumeOpt}); err != nil {
			t.Fatalf("Failed to run no-op with resume: %s", err)
		}
		fmt.Println("waiting for resume opt")
		if err := resumeOpt.Wait(); err != nil {
			t.Fatalf("ResumeOpt failed with resume: %s", err)
		}
	})
}

// A spark integration test runs a pyspark scripts and allows you to import our main
// runner script to run against. It expects the files to be under:
// provider/scripts/spark/integration_test_scripts
type sparkIntegrationTest struct {
	// JobName will be the jobs name in Spark
	JobName string
	// File will be the file name under provider/scripts/spar/integration_test_scripts
	// that will be run. Look under that directory to see some examples.
	File string
	// TestConfigs should include all SparkConfigs except for the one that includes
	// the actual pyspark script and the deploy mode.
	TestConfigs spark.Configs
	// DeployMode is how the spark test should be run.
	DeployMode types.SparkDeployMode
}

func (test sparkIntegrationTest) Run(t *testing.T, executor SparkExecutor, sfs SparkFileStoreV2) {
	// This allows us to write all scripts to different areas. The format should be safe for SparkFileStore.
	safeTimeString := time.Now().Format("2006-01-02_15-04-05")
	// UUID is added to deal with race conditions where two tests run in parallel.
	remoteScriptDir := fmt.Sprintf("unit_tests/scripts/tests/%s/%s/", safeTimeString, uuid.NewString())

	deleteRemotePath := func(path filestore.Filepath) {
		if err := sfs.Delete(path); err != nil {
			// Since this function is defered, we shouldn't use t.
			// The test may complete before this runs.
			fmt.Printf("Failed to delete remote path %s\n%s", path.ToURI(), err)
		}
	}

	localTestPathStr := fmt.Sprintf("scripts/spark/integration_test_scripts/%s", test.File)
	localTestPath := &fs.LocalFilepath{}
	if err := localTestPath.SetKey(localTestPathStr); err != nil {
		t.Fatalf("could not set local test path: %v", err)
	}
	remoteTestPathStr := fmt.Sprintf("%s%s", remoteScriptDir, test.File)
	remoteTestPath, err := sfs.CreateFilePath(remoteTestPathStr, false)
	if err != nil {
		t.Errorf("could not create remote test path: %s", err)
	}
	if err := readAndUploadFile(localTestPath, remoteTestPath, sfs); err != nil {
		t.Fatalf("could not upload '%s' to '%s': %v", localTestPath.ToURI(), remoteTestPath.ToURI(), err)
	}
	t.Logf("Wrote from %s to %s", localTestPath.ToURI(), remoteTestPath.ToURI())
	defer deleteRemotePath(remoteTestPath)

	localRunnerPathStr := "scripts/spark/offline_store_spark_runner.py"
	localRunnerPath := &fs.LocalFilepath{}
	if err := localRunnerPath.SetKey(localRunnerPathStr); err != nil {
		t.Fatalf("could not set local path: %v", err)
	}
	remoteRunnerPathStr := fmt.Sprintf("%soffline_store_spark_runner.py", remoteScriptDir)
	remoteRunnerPath, err := sfs.CreateFilePath(remoteRunnerPathStr, false)
	if err != nil {
		t.Errorf("could not create remote runner path: %s", err)
	}
	if err := readAndUploadFile(localRunnerPath, remoteRunnerPath, sfs); err != nil {
		t.Fatalf("could not upload '%s' to '%s': %v", localRunnerPath.ToURI(), remoteRunnerPath.ToURI(), err)
	}
	t.Logf("Wrote from %s to %s", localRunnerPath.ToURI(), remoteRunnerPath.ToURI())
	defer deleteRemotePath(remoteRunnerPath)

	baseConfigs := spark.Configs{
		spark.DeployFlag{
			Mode: test.DeployMode,
		},
		spark.IncludePyScript{
			Path: remoteRunnerPath,
		},
		spark.OutputFlag{},
	}
	configs := append(baseConfigs, test.TestConfigs...)
	cmd := &spark.Command{
		Script:     remoteTestPath,
		ScriptArgs: []string{"sql"},
		Configs:    configs,
	}

	maxWait := time.Hour * 2
	jobOpts := SparkJobOptions{MaxJobDuration: maxWait, JobName: test.JobName}
	asyncOpt := RunAsyncWithResume(maxWait)
	if err := executor.RunSparkJob(cmd, sfs, jobOpts, []TransformationOption{asyncOpt}); err != nil {
		t.Fatalf("Failed to run no-op with resume: %s", err)
	}
	t.Logf("Waiting for async opt")
	if err := asyncOpt.Wait(); err != nil {
		t.Fatalf("AsyncOpt failed with resume: %s", err)
	}
}

func createIcebergIntegrationTest() sparkIntegrationTest {
	bucketRegion := helpers.GetEnv("S3_BUCKET_REGION", "us-east-1")
	accessKey := helpers.GetEnv("AWS_ACCESS_KEY", "")
	secretKey := helpers.GetEnv("AWS_SECRET_KEY", "")
	bucket := helpers.GetEnv("S3_BUCKET_NAME", "")
	configs := spark.Configs{
		spark.GlueFlags{
			FileStoreType:   types.S3Type,
			TableFormatType: types.IcebergType,
			// This test assumes that the glue catalog and S3 bucket are in the same region, but that
			// doesn't have to be true.
			Region: bucketRegion,
			// TODO make this a secret so we can get from env.
			Warehouse: "s3://ali-aws-lake-house-iceberg-blog-demo/demo2/",
		},
		spark.S3Flags{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Region:    bucketRegion,
			Bucket:    bucket,
		},
		spark.IcebergFlags{},
	}
	return sparkIntegrationTest{
		JobName:     "IcebergIntegrationTest",
		File:        "test_iceberg.py",
		TestConfigs: configs,
		DeployMode:  types.SparkClientDeployMode,
	}
}

// TODO figure this out
// func testReadWriteDelta(t *testing.T, executor SparkExecutor, sfs SparkFileStore) {
// 	localScriptPath := &fs.LocalFilepath{}
// 	// This allows us to write all scripts to different areas. The format is safe for S3.
// 	safeTimeString := time.Now().Format("2006-01-02_15-04-05")
// 	remoteScriptDir := fmt.Sprintf("unit_tests/scripts/tests/%s/", safeTimeString)
// 	if err := localScriptPath.SetKey("scripts/spark/integration_test_scripts/test_delta.py"); err != nil {
// 		t.Fatalf("could not set local script path: %v", err)
// 	}
// 	remoteScriptPath, err := sfs.CreateFilePath(fmt.Sprintf("%stest_delta.py", remoteScriptDir), false)
// 	if err != nil {
// 		t.Fatalf("could not create remote script path: %v", err)
// 	}
// 	if err := readAndUploadFile(localScriptPath, remoteScriptPath, sfs); err != nil {
// 		t.Fatalf("could not upload '%s' to '%s': %v", localScriptPath.ToURI(), remoteScriptPath.ToURI(), err)
// 	}
//
// 	localRunnerPath := &fs.LocalFilepath{}
// 	if err := localRunnerPath.SetKey("scripts/spark/offline_store_spark_runner.py"); err != nil {
// 		t.Fatalf("could not set local script path: %v", err)
// 	}
// 	remoteRunnerPath, err := sfs.CreateFilePath(fmt.Sprintf("%soffline_store_spark_runner.py", remoteScriptDir), false)
// 	if err != nil {
// 		t.Fatalf("could not create remote script path: %v", err)
// 	}
// 	if err := readAndUploadFile(localRunnerPath, remoteRunnerPath, sfs); err != nil {
// 		t.Fatalf("could not upload '%s' to '%s': %v", localRunnerPath.ToURI(), remoteRunnerPath.ToURI(), err)
// 	}
//
// 	bucketRegion := helpers.GetEnv("S3_BUCKET_REGION", "us-east-1")
// 	accessKey := helpers.GetEnv("AWS_ACCESS_KEY", "")
// 	secretKey := helpers.GetEnv("AWS_SECRET_KEY", "")
// 	bucket := helpers.GetEnv("S3_BUCKET_NAME", "")
// 	configs := SparkConfigs{
// 		sparkGlueFlags{
// 			fileStoreType:   types.S3Type,
// 			tableFormatType: types.DeltaType,
// 			Region:    "us-east-1",
// 			Warehouse: "s3://emr-delta-poc/",
// 		},
// 		sparkDeployFlag{
// 			Mode: types.SparkClientDeployMode,
// 		},
// 		sparkS3Flags{
// 			AccessKey: accessKey,
// 			SecretKey: secretKey,
// 			Region:    bucketRegion,
// 			Bucket:    bucket,
// 		},
// 		sparkDeltaFlags{},
// 		sparkIncludePyScript{
// 			Path: remoteRunnerPath,
// 		},
// 	}
//
// 	cmd := configs.CompileCommand(remoteScriptPath, "sql", "--output", "{}")
//
// 	maxWait := time.Minute * 10
// 	jobOpts := SparkJobOptions{MaxJobDuration: maxWait, JobName: "deltaTest"}
// 	asyncOpt := RunAsyncWithResume(maxWait)
// 	if err := executor.RunSparkJob(cmd, sfs, jobOpts, []TransformationOption{asyncOpt}); err != nil {
// 		t.Fatalf("Failed to run no-op with resume: %s", err)
// 	}
// 	fmt.Println("Waiting for async opt")
// 	if err := asyncOpt.Wait(); err != nil {
// 		t.Fatalf("AsyncOpt failed with resume: %s", err)
// 	}
// }

func createDynamoIntegrationTest() sparkIntegrationTest {
	accessKey := helpers.GetEnv("AWS_ACCESS_KEY_ID", "")
	secretKey := helpers.GetEnv("AWS_SECRET_KEY", "")
	dynamoRegion := "us-east-1"
	configs := spark.Configs{
		spark.DynamoFlags{
			Region:    dynamoRegion,
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
		spark.HighMemoryFlags{},
	}
	return sparkIntegrationTest{
		JobName:     "DynamodbIntegrationTest",
		File:        "test_dynamodb.py",
		TestConfigs: configs,
		DeployMode:  types.SparkClientDeployMode,
	}
}

func createFeatureQueryTest() sparkIntegrationTest {
	return sparkIntegrationTest{
		JobName:     "FeatureQueryTest",
		File:        "test_feature_query.py",
		TestConfigs: spark.Configs{},
		DeployMode:  types.SparkClientDeployMode,
	}
}

func createMaterializeTest() sparkIntegrationTest {
	bucketRegion := helpers.GetEnv("S3_BUCKET_REGION", "us-east-1")
	// TODO add this to secrets
	dynamoRegion := "us-east-1"
	accessKey := helpers.GetEnv("AWS_ACCESS_KEY_ID", "")
	secretKey := helpers.GetEnv("AWS_SECRET_KEY", "")
	bucket := helpers.GetEnv("S3_BUCKET_NAME", "")
	configs := spark.Configs{
		spark.DynamoFlags{
			Region:    dynamoRegion,
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
		spark.GlueFlags{
			FileStoreType:   types.S3Type,
			TableFormatType: types.IcebergType,
			// This test assumes that the glue catalog and S3 bucket are in the same region, but that
			// doesn't have to be true.
			Region: bucketRegion,
			// TODO add this to secrets
			Warehouse: "s3://ali-aws-lake-house-iceberg-blog-demo/demo2/",
		},
		spark.S3Flags{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Region:    bucketRegion,
			Bucket:    bucket,
		},
		spark.IcebergFlags{},
		spark.HighMemoryFlags{},
	}
	return sparkIntegrationTest{
		JobName:     "MaterializeTest",
		File:        "test_materialize.py",
		TestConfigs: configs,
		DeployMode:  types.SparkClientDeployMode,
	}
}

func createKafkaTest() sparkIntegrationTest {
	configs := spark.Configs{
		spark.KafkaFlags{},
	}
	return sparkIntegrationTest{
		JobName:     "KafkaTest",
		File:        "test_kafka.py",
		TestConfigs: configs,
		DeployMode:  types.SparkClientDeployMode,
	}
}

func TestCreateSourceInfo(t *testing.T) {
	logger := logging.NewTestLogger(t)

	// Set up the local file path
	fp := filestore.LocalFilepath{}
	err := fp.SetKey("/path/to/data")
	if err != nil {
		t.Fatalf("could not set local file path: %v", err)
	}

	// Serialize the SparkConfig
	sc := pc.SparkConfig{
		ExecutorType: pc.EMR,
		ExecutorConfig: &pc.EMRConfig{
			Credentials:   pc.AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
			ClusterRegion: "us-east-1",
			ClusterName:   "featureform-clst",
		},
		StoreType: filestore.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials:  pc.AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
			BucketRegion: "us-east-1",
			BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
		},
		GlueConfig: &pc.GlueConfig{
			Warehouse:   "s3://featureform/warehouse",
			Database:    "featureform",
			Region:      "us-east-1",
			TableFormat: pc.Iceberg,
		},
	}
	scSerialized, err := sc.Serialize()
	if err != nil {
		t.Fatalf("could not serialize spark config: %v", err)
	}

	// Define test cases
	testCases := []struct {
		name        string
		mappings    []SourceMapping
		expected    []spark.SourceInfo
		expectError bool
	}{
		{
			name: "Valid SparkOffline SourceMapping",
			mappings: []SourceMapping{
				{
					ProviderType:        provider_type.SparkOffline,
					ProviderConfig:      scSerialized,
					Location:            pl.NewFileLocation(&fp),
					TimestampColumnName: "event_time",
				},
			},
			expected: []spark.SourceInfo{
				{
					Location:            "//path/to/data",
					LocationType:        "filestore",
					Provider:            provider_type.SparkOffline,
					TableFormat:         "", // Adjust if necessary
					FileType:            "", // Adjust if necessary
					IsDir:               false,
					Database:            "",
					Schema:              "",
					AwsAssumeRoleArn:    "", // Adjust if necessary
					TimestampColumnName: "event_time",
				},
			},
			expectError: false,
		},
		{
			name: "Unsupported ProviderType",
			mappings: []SourceMapping{
				{
					ProviderType: "UnsupportedProvider",
					Location:     pl.NewFileLocation(&fp),
				},
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "Different SparkOffline SourceMappings",
			mappings: []SourceMapping{
				{
					ProviderType:   provider_type.SparkOffline,
					ProviderConfig: scSerialized,
					Location:       pl.NewCatalogLocation("database", "table", "schema"),
				},
				{
					ProviderType:   provider_type.SparkOffline,
					ProviderConfig: scSerialized,
					Location:       pl.NewFileLocation(&fp),
				},
			},
			expected: []spark.SourceInfo{
				{
					Location:     "database.table",
					LocationType: "catalog",
					Provider:     provider_type.SparkOffline,
					TableFormat:  "schema",
					// Other fields set to zero values or adjusted as needed
				},
				{
					Location:     "//path/to/data",
					LocationType: "filestore",
					Provider:     provider_type.SparkOffline,
					// Other fields set to zero values or adjusted as needed
				},
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sources, err := createSourceInfo(tc.mappings, logger)

			if tc.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, len(tc.expected), len(sources), "Expected %d sources, got %d", len(tc.expected), len(sources))

			for i, expected := range tc.expected {
				actual := sources[i]
				assert.Equal(t, expected, actual, "Source %d does not match expected value", i)
			}
		})
	}
}
