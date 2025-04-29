// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"

	"github.com/featureform/fferr"
	fftypes "github.com/featureform/fftypes"
	"github.com/featureform/filestore"
	fs "github.com/featureform/filestore"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

const outputDir = "test_files/output"

type OfflineStoreTest struct {
	t     *testing.T
	store OfflineStore
}

func (test *OfflineStoreTest) Run() {
	t := test.t
	store := test.store

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
		"BatchFeatures":           testBatchFeature,
		// "TrainingSetLag":          testLagFeaturesTrainingSet,
		"TrainingSetInvalidID":   testGetTrainingSetInvalidResourceID,
		"GetUnknownTrainingSet":  testGetUnknownTrainingSet,
		"InvalidTrainingSetDefs": testInvalidTrainingSetDefs,
		"LabelTableNotFound":     testLabelTableNotFound,
		"FeatureTableNotFound":   testFeatureTableNotFound,
		"TrainingDefShorthand":   testTrainingSetDefShorthand,
		// TODO: Re-enable when refactoring providers
		//"ResourceLocation":       testResourceLocation,
	}

	for name, fn := range testFns {
		nameConst := name
		fnConst := fn
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			fnConst(t, store)
		})
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("%v - %v\n", store.Type(), err)
		}
	})
}

func (test *OfflineStoreTest) RunSQL() {
	t := test.t
	store := test.store
	testFns := map[string]func(*testing.T, OfflineStore){
		"PrimaryTableCreate":                 testPrimaryCreateTable,
		"PrimaryTableWrite":                  testPrimaryTableWrite,
		"Transformation":                     testTransform,
		"TransformationUpdate":               testTransformUpdate,
		"TransformationUpdateWithFeature":    testTransformUpdateWithFeatures,
		"CreateDuplicatePrimaryTable":        testCreateDuplicatePrimaryTable,
		"ChainTransformations":               testChainTransform,
		"CreateResourceFromSource":           testCreateResourceFromSource,
		"CreateResourceFromSourceNoTS":       testCreateResourceFromSourceNoTS,
		"CreatePrimaryFromSource":            testCreatePrimaryFromSource,
		"CreatePrimaryFromNonExistentSource": testCreatePrimaryFromNonExistentSource,
		"TrainTestSplit":                     testTrainTestSplit,
	}

	for name, fn := range testFns {
		nameConst := name
		fnConst := fn
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			fnConst(t, store)
		})
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("%v - %v\n", store.Type(), err)
		}
	})
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
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}
	if tab, err := store.CreateResourceTable(id, schema); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if tab, err := store.GetResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}
}

func testResourceLocation(t *testing.T, store OfflineStore) {
	if store.Type() == pt.MemoryOffline {
		t.Skip("Skipping test for memory store")
	}

	id := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Primary,
	}

	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "int", ValueType: types.Int},
			{Name: "bool", ValueType: types.Bool},
			{Name: "string", ValueType: types.String},
			{Name: "float", ValueType: types.Float32},
		},
	}

	table, err := store.CreatePrimaryTable(id, schema)
	if err != nil {
		t.Fatalf("could not create primary table: %v", err)
	}

	if tab, err := store.GetResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	name := table.Location().Location()
	svProto := pb.SourceVariant{
		Table: name,
		Definition: &pb.SourceVariant_PrimaryData{
			PrimaryData: &pb.PrimaryData{
				Location: &pb.PrimaryData_Table{
					Table: &pb.SQLTable{
						Name: name,
					},
				},
			},
		},
	}
	sv := metadata.WrapProtoSourceVariant(&svProto)

	location, err := store.ResourceLocation(id, *sv)
	if err != nil {
		t.Fatalf("Failed to get location: %v", err)
	}
	if location.Location() == "" || err != nil {
		t.Fatalf("Failed to get location: %v", err)
	}

	if store.Type() == pt.SparkOffline || store.Type() == pt.K8sOffline {
		expectedLocation := fmt.Sprintf("featureform/transformation/%s/%s", id.Name, id.Variant)
		if !strings.Contains(location.Location(), expectedLocation) {
			t.Fatalf("Location is incorrect: %s needs to have %s", location, expectedLocation)
		}
	} else {
		expectedLocation := fmt.Sprintf("featureform_primary__%s__%s", id.Name, id.Variant)
		if location.Location() != expectedLocation {
			t.Fatalf("Location is incorrect: %s != expected location (%s)", location, expectedLocation)
		}
	}
}

func testOfflineTableAlreadyExists(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	schema := TableSchema{
		// TODO: Verify whether these should be empty strings or not
		// Columns: []TableColumn{
		// 	{Name: "", ValueType: types.String},
		// 	{Name: "", ValueType: types.Int},
		// 	{Name: "", ValueType: types.Timestamp},
		// },
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}
	if _, err := store.CreateResourceTable(id, schema); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateResourceTable(id, schema); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*fferr.DatasetAlreadyExistsError); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func testOfflineTableNotFound(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	if _, err := store.GetResourceTable(id); err == nil {
		t.Fatalf("Succeeded in getting non-existant table")
	} else if casted, valid := err.(*fferr.DatasetNotFoundError); !valid {
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
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
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
	testMaterialization := func(t *testing.T, matDataset dataset.Materialization, test TestCase) {
		ctx := logging.NewTestContext(t)

		// Check the number of rows
		if numRows, err := matDataset.Len(); err != nil {
			t.Fatalf("Failed to get num rows: %s", err)
		} else if numRows != test.ExpectedRows {
			t.Fatalf("Num rows not equal %d %d", numRows, test.ExpectedRows)
		}

		// Get iterator for the segment
		iter, err := matDataset.IterateSegment(ctx, test.SegmentStart, test.SegmentEnd)
		if err != nil {
			t.Fatalf("Failed to create segment: %s", err)
		}
		defer iter.Close()

		i := 0
		expectedRows := test.ExpectedSegment

		// Iterate through the rows
		for iter.Next() {
			row := iter.Values()
			actual, err := RowToResourceRecord(row)
			require.NoError(t, err, "Failed to convert row to resource record")

			// Check if this row matches any expected row
			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(actual, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1

					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation except that it re-orders the slice.
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

		// Check for errors
		if err := iter.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}

		// Make sure we've seen enough rows
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d. Expected: %d", i, len(test.ExpectedSegment))
		}
	}
	runTestCase := func(t *testing.T, test TestCase) {
		id := randomID(Feature)
		table, err := store.CreateResourceTable(id, test.Schema)

		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}

		if err := table.WriteBatch(test.WriteRecords); err != nil {
			t.Fatalf("Failed to write batch: %s", err)
		}

		mat, err := store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet})
		if err != nil {
			t.Fatalf("Failed to create materialization: %s", err)
		}

		testMaterialization(t, mat, test)
		getMat, err := store.GetMaterialization(MaterializationID(mat.ID()))

		if err != nil {
			t.Fatalf("Failed to get materialization: %s", err)
		}
		testMaterialization(t, getMat, test)
		if err := store.DeleteMaterialization(MaterializationID(mat.ID())); err != nil {
			t.Fatalf("Failed to delete materialization: %s", err)
		}
	}
	for name, test := range tests {
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			runTestCase(t, testConst)
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

	schemaWithTimestamp := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}
	schemaWithoutTimestamp := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
		},
	}
	tests := map[string]TestCase{
		"Empty": {
			WriteRecords:    []ResourceRecord{},
			UpdateRecords:   []ResourceRecord{},
			Schema:          schemaWithoutTimestamp,
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
			Schema:              schemaWithoutTimestamp,
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
			Schema:              schemaWithoutTimestamp,
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
			Schema:              schemaWithTimestamp,
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
			Schema:              schemaWithTimestamp,
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
			Schema:              schemaWithTimestamp,
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
	testMaterialization := func(t *testing.T, matDataset dataset.Materialization, test TestCase) {
		ctx := logging.NewTestContext(t)

		// Check the number of rows
		if numRows, err := matDataset.Len(); err != nil {
			t.Fatalf("Failed to get num rows: %s", err)
		} else if numRows != test.ExpectedRows {
			t.Fatalf("Num rows not equal %d %d", numRows, test.ExpectedRows)
		}

		// Get iterator for the segment
		iter, err := matDataset.IterateSegment(ctx, test.SegmentStart, test.SegmentEnd)
		if err != nil {
			t.Fatalf("Failed to create segment: %s", err)
		}
		defer iter.Close()

		i := 0
		expectedRows := test.ExpectedSegment

		// Iterate through the rows
		for iter.Next() {
			row := iter.Values()

			// Convert the row to a ResourceRecord for comparison
			actual := ResourceRecord{}

			// Entity should be the first column and a string
			if len(row) > 0 && row[0].Value != nil {
				if entityVal, ok := row[0].Value.(string); ok {
					actual.Entity = entityVal
				} else {
					t.Fatalf("entity value not a string: %v", row[0].Value)
				}
			}

			// Value should be the second column
			if len(row) > 1 {
				actual.Value = row[1].Value
			}

			// Timestamp may be the third column if present
			if len(row) > 2 && row[2].Value != nil {
				if tsVal, ok := row[2].Value.(time.Time); ok {
					actual.TS = tsVal
				}
			}

			// Check if this row matches any expected row
			found := false
			for i, expRow := range expectedRows {
				if reflect.DeepEqual(actual, expRow) {
					found = true
					lastIdx := len(expectedRows) - 1

					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation except that it re-orders the slice.
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

		// Check for errors
		if err := iter.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}

		// Make sure we've seen enough rows
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d. Expected: %d", i, len(test.ExpectedSegment))
		}
	}
	testUpdate := func(t *testing.T, matDataset dataset.Materialization, test TestCase) {
		ctx := logging.NewTestContext(t)

		// Check the number of rows
		if numRows, err := matDataset.Len(); err != nil {
			t.Fatalf("Failed to get num rows: %s", err)
		} else if numRows != test.UpdatedRows {
			t.Fatalf("Num rows not equal %d %d", numRows, test.UpdatedRows)
		}

		// Get iterator for the updated segment
		iter, err := matDataset.IterateSegment(ctx, test.UpdatedSegmentStart, test.UpdatedSegmentEnd)
		if err != nil {
			t.Fatalf("Failed to create segment: %s", err)
		}
		defer iter.Close()

		i := 0
		expectedUpdates := test.ExpectedUpdate

		// Iterate through the rows
		for iter.Next() {
			row := iter.Values()

			// Convert the row to a ResourceRecord for comparison
			actual := ResourceRecord{}

			// Entity should be the first column and a string
			if len(row) > 0 && row[0].Value != nil {
				if entityVal, ok := row[0].Value.(string); ok {
					actual.Entity = entityVal
				} else {
					t.Fatalf("entity value not a string: %v", row[0].Value)
				}
			}

			// Value should be the second column
			if len(row) > 1 {
				actual.Value = row[1].Value
			}

			// Timestamp may be the third column if present
			if len(row) > 2 && row[2].Value != nil {
				if tsVal, ok := row[2].Value.(time.Time); ok {
					actual.TS = tsVal
				}
			}

			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is inefficient, but these test
			// cases should all be small enough not to matter.
			found := false
			for i, expRow := range expectedUpdates {
				if reflect.DeepEqual(actual, expRow) {
					found = true
					lastIdx := len(expectedUpdates) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation except that it re-orders the slice.
					expectedUpdates[i], expectedUpdates[lastIdx] = expectedUpdates[lastIdx], expectedUpdates[i]
					expectedUpdates = expectedUpdates[:lastIdx]
					break
				}
			}

			if !found {
				t.Fatalf("Unexpected materialization row: %v, expected %v", actual, expectedUpdates)
			}
			i++
		}

		// Check for errors
		if err := iter.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}

		// Make sure we've seen enough rows
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d", i)
		}
	}
	runTestCase := func(t *testing.T, test TestCase) {
		id := randomID(Feature)
		table, err := store.CreateResourceTable(id, test.Schema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}

		if err := table.WriteBatch(test.WriteRecords); err != nil {
			t.Fatalf("Failed to write batch: %s", err)
		}
		opts := MaterializationOptions{Output: fs.Parquet}
		mat, err := store.CreateMaterialization(id, opts)
		if err != nil {
			t.Fatalf("Failed to create materialization: %s", err)
		}
		testMaterialization(t, mat, test)

		if err != nil {
			t.Fatalf("Failed to get materialization: %s", err)
		}

		if err := table.WriteBatch(test.UpdateRecords); err != nil {
			t.Fatalf("Failed to write batch: %s", err)
		}

		mat, err = store.UpdateMaterialization(id, opts)
		if err != nil {
			t.Fatalf("Failed to update materialization: %s", err)
		}
		testUpdate(t, mat, test)
		if err := store.DeleteMaterialization(MaterializationID(mat.ID())); err != nil {
			t.Fatalf("Failed to delete materialization: %s", err)
		}
	}
	for name, test := range tests {
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			runTestCase(t, testConst)
		})
	}

}

func testWriteInvalidResourceRecord(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}
	table, err := store.CreateResourceTable(id, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := table.WriteBatch([]ResourceRecord{{}}); err == nil {
		t.Fatalf("Succeeded in writing invalid resource record")
	}
}

func testInvalidMaterialization(t *testing.T, store OfflineStore) {
	id := randomID(Label)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}
	if _, err := store.CreateResourceTable(id, schema); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet}); err == nil {
		t.Fatalf("Succeeded in materializing label")
	}
}

func testMaterializeUnknown(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	if _, err := store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet}); err == nil {
		t.Fatalf("Succeeded in materializing uninitialized resource")
	}
}

func testMaterializationNotFound(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	matIDStr, err := ps.ResourceToMaterializationID(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		t.Fatalf("Failed to get materialization ID: %s", err)
	}
	matID := MaterializationID(matIDStr)
	if _, err := store.GetMaterialization(matID); err == nil {
		t.Fatalf("Succeeded in getting uninitialized materialization")
	}
	err = store.DeleteMaterialization(matID)
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

func testInvalidResourceIDs(t *testing.T, store OfflineStore) {
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
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
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
			},
			LabelRecords: []ResourceRecord{},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
				},
			},
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
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Bool},
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
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
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
			if err := table.WriteBatch(recs); err != nil {
				t.Fatalf("Failed to write batch: %v", err)
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, test.LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		if err := labelTable.WriteBatch(test.LabelRecords); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
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
			// then we delete that row. This is inefficient, but these test
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			runTestCase(t, testConst)
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
			FeatureSchema: []TableSchema{{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
				},
			}},
			LabelRecords:        []ResourceRecord{},
			UpdatedLabelRecords: []ResourceRecord{},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
				},
			},
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
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Bool},
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
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
						{Name: "ts", ValueType: types.Timestamp},
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
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
			if err := table.WriteBatch(recs); err != nil {
				t.Fatalf("Failed to write records %v: %v", recs, err)
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, test.LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		if err := labelTable.WriteBatch(test.LabelRecords); err != nil {
			t.Fatalf("Failed to write records %v: %v", test.LabelRecords, err)
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
			if err := table.WriteBatch(test.UpdatedFeatureRecords[i]); err != nil {
				t.Fatalf("Failed to write records %v: %v", test.UpdatedFeatureRecords[i], err)
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			runTestCase(t, testConst)
		})
	}
}

func testGetTrainingSetInvalidResourceID(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting invalid training set ResourceID")
	}
}

func testGetUnknownTrainingSet(t *testing.T, store OfflineStore) {
	// This should default to TrainingSet
	id := randomID(NoType)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting unknown training set ResourceID")
	} else if _, valid := err.(*fferr.DatasetNotFoundError); !valid {
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
		nameConst := name
		defConst := def
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			if err := store.CreateTrainingSet(defConst); err == nil {
				t.Fatalf("Succeeded to create invalid def")
			}
		})
	}
}

func testLabelTableNotFound(t *testing.T, store OfflineStore) {
	featureID := randomID(Feature)
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
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
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Int},
			{Name: "ts", ValueType: types.Timestamp},
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
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.String},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}
	fId := randomID(Feature)
	fTable, err := store.CreateResourceTable(fId, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := fTable.WriteBatch([]ResourceRecord{{Entity: "a", Value: "feature"}}); err != nil {
		t.Fatalf("Failed to write record: %s", err)
	}
	lId := randomID(Label)
	lTable, err := store.CreateResourceTable(lId, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := lTable.WriteBatch([]ResourceRecord{{Entity: "a", Value: "label"}}); err != nil {
		t.Fatalf("Failed to write record: %s", err)
	}
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
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "bool", ValueType: types.Bool},
					{Name: "string", ValueType: types.String},
					{Name: "float", ValueType: types.Float32},
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			testPrimary(t, testConst, store)
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "timestamp", ValueType: types.Timestamp},
				},
			},
			Records:     []GenericRecord{},
			ExpectError: false,
			Expected:    []GenericRecord{},
		},
		// Unclear on how this test differs from the previous one.
		"SimpleColumnEmpty": {
			Rec: ResourceID{
				Name: uuid.NewString(),
				Type: Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "timestamp", ValueType: types.Timestamp},
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "timestamp", ValueType: types.Timestamp},
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

		name := table.Location().Location()
		svProto := pb.SourceVariant{
			Table: name,
			Definition: &pb.SourceVariant_PrimaryData{
				PrimaryData: &pb.PrimaryData{
					Location: &pb.PrimaryData_Table{
						Table: &pb.SQLTable{
							Name: name,
						},
					},
				},
			},
		}
		sv := metadata.WrapProtoSourceVariant(&svProto)

		_, err = store.GetPrimaryTable(test.Rec, *sv) // Need To Fix Schema Here
		if err != nil {
			t.Fatalf("Could not get Primary table: %v", err)
		}

		// cast to writableDataset
		writableDataset, ok := table.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Could not cast to WritableDataset")
		}

		ctx := logging.NewTestContext(t)
		if err := writableDataset.WriteBatch(ctx, GenericRecordsToRows(test.Records)); err != nil {
			t.Fatalf("Could not write: %v", err)
		}
	}

	for name, test := range tests {
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			testTableWrite(t, testConst)
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
				Name:    uuid.NewString(),
				Variant: uuid.NewString(),
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "flt", ValueType: types.Float64},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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
					Name:    uuid.NewString(),
					Variant: uuid.NewString(),
					Type:    Transformation,
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
				Name:    uuid.NewString(),
				Variant: uuid.NewString(),
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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
					Name:    uuid.NewString(),
					Variant: uuid.NewString(),
					Type:    Transformation,
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

		writableTable, ok := table.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Table does not implement WritableDataset interface")
		}

		if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(test.Records)); err != nil {
			t.Fatalf("Could not write: %v", err)
		}

		name := table.Location().Location()
		modifyTransformationConfig(t, t.Name(), name, store.Type(), &test.Config)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}

		sized, ok := table.(dataset.SizedDataset)
		if !ok {
			t.Fatalf("Table does not implement SizedDataset interface")
		}
		rows, err := sized.Len()
		if err != nil {
			t.Fatalf("could not get NumRows of table: %v", err)
		}
		if int(rows) != len(test.Records) {
			t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(test.Records), rows)
		}

		tfDs, err := store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get transformation table: %v", err)
		}

		iterator, err := tfDs.Iterator(ctx, 0)
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

			schema := iterator.Schema()
			tableColumns := schema.ColumnNames()
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			testTransform(t, testConst)
		})
	}
}

// Tests the update of a transformation that has a feature registered on it. The main idea being that the atomic update
// works as expected.
func testTransformUpdateWithFeatures(t *testing.T, store OfflineStore) {
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
				Name:    uuid.NewString(),
				Variant: uuid.NewString(),
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "flt", ValueType: types.Float64},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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
					Name:    uuid.NewString(),
					Variant: uuid.NewString(),
					Type:    Transformation,
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
	}

	featureID := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Feature,
	}

	testTransform := func(t *testing.T, test TransformTest) {
		ctx := logging.NewTestContext(t)

		primaryTable, err := store.CreatePrimaryTable(test.PrimaryTable, test.Schema)
		if err != nil {
			t.Fatalf("Could not initialize table: %v", err)
		}

		writableTable, ok := primaryTable.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Table does not implement WritableDataset interface")
		}

		if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(test.Records)); err != nil {
			t.Fatalf("Could not write value: %v", err)
		}

		name := primaryTable.Location().Location()
		modifyTransformationConfig(t, t.Name(), name, store.Type(), &test.Config)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}

		sized, ok := primaryTable.(dataset.SizedDataset)
		if !ok {
			t.Fatalf("Table does not implement SizedDataset interface")
		}
		rows, err := sized.Len()
		if err != nil {
			t.Fatalf("could not get NumRows of table: %v", err)
		}

		if int(rows) != len(test.Records) {
			t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(test.Records), rows)
		}
		tfDs, err := store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get transformation table: %v", err)
		}

		var location pl.Location
		if store.Type() == pt.SparkOffline {
			sparkStore := store.(*SparkOfflineStore)
			fp, err := sparkStore.Store.CreateFilePath(tfDs.Location().Location(), false)
			if err != nil {
				t.Fatalf("Could not create file path: %v", err)
			}
			location = pl.NewFileLocation(fp)
		} else {
			location = pl.NewSQLLocation(tfDs.Location().Location())
		}

		// create feature on transformation
		recSchema := ResourceSchema{
			Entity:      "entity",
			Value:       "int",
			TS:          "ts",
			SourceTable: location,
		}
		_, err = store.RegisterResourceFromSourceTable(featureID, recSchema)
		if err != nil {
			t.Fatalf("Could not register from tf: %s", err)
		}
		_, err = store.GetResourceTable(featureID)
		if err != nil {
			t.Fatalf("Could not get resource table: %v", err)
		}
		_, err = store.CreateMaterialization(featureID, MaterializationOptions{Output: fs.Parquet})
		if err != nil {
			t.Fatalf("Could not create materialization: %v", err)
		}

		name = primaryTable.Location().Location()
		svProto := pb.SourceVariant{
			Table: name,
			Definition: &pb.SourceVariant_PrimaryData{
				PrimaryData: &pb.PrimaryData{
					Location: &pb.PrimaryData_Table{
						Table: &pb.SQLTable{
							Name: name,
						},
					},
				},
			},
		}
		sv := metadata.WrapProtoSourceVariant(&svProto)

		ds, err := store.GetPrimaryTable(test.PrimaryTable, *sv)
		if err != nil {
			t.Fatalf("Could not get primary table: %v", err)
		}
		writableDataset, ok := ds.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Dataset is not writeable: %v", err)
		}

		if err := writableDataset.WriteBatch(ctx, GenericRecordsToRows(test.UpdatedRecords)); err != nil {
			t.Fatalf("Could not write value: %v", err)
		}

		if err := store.UpdateTransformation(test.Config); err != nil {
			t.Errorf("could not update transformation: %v", err)
		}
		tfDs, err = store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get updated transformation table: %v", err)
		}

		iterator, err := tfDs.Iterator(ctx, 0)
		if err != nil {
			t.Fatalf("Could not get generic iterator: %v", err)
		}

		i := 0
		for iterator.Next() {
			found := false
			for i, expRow := range test.UpdatedExpected {
				if reflect.DeepEqual(iterator.Values(), expRow) {
					found = true
					lastIdx := len(test.UpdatedExpected) - 1
					// Swap the record that we've found to the end, then shrink the slice to not include it.
					// This is essentially a delete operation except that it re-orders the slice.
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			testTransform(t, testConst)
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
				Name:    uuid.NewString(),
				Variant: uuid.NewString(),
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "flt", ValueType: types.Float64},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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
					Name:    uuid.NewString(),
					Variant: uuid.NewString(),
					Type:    Transformation,
				},
				Query: "SELECT * FROM tb",
				SourceMapping: []SourceMapping{
					{
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
				Name:    uuid.NewString(),
				Variant: uuid.NewString(),
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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
					Name:    uuid.NewString(),
					Variant: uuid.NewString(),
					Type:    Transformation,
				},
				Query: "SELECT COUNT(*) as total_count FROM tb",
				SourceMapping: []SourceMapping{
					{
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
		primaryTable, err := store.CreatePrimaryTable(test.PrimaryTable, test.Schema)
		if err != nil {
			t.Fatalf("Could not initialize table: %v", err)
		}

		writableTable, ok := primaryTable.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Table does not implement WritableDataset interface")
		}

		if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(test.Records)); err != nil {
			t.Fatalf("Could not write records: %v", err)
		}

		name := primaryTable.Location().Location()
		modifyTransformationConfig(t, t.Name(), name, store.Type(), &test.Config)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}

		sized, ok := primaryTable.(dataset.SizedDataset)
		if !ok {
			t.Fatalf("Table does not implement SizedDataset interface")
		}
		rows, err := sized.Len()
		if err != nil {
			t.Fatalf("could not get NumRows of table: %v", err)
		}
		if int(rows) != len(test.Records) {
			t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(test.Records), rows)
		}
		table, err := store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get transformation table: %v", err)
		}
		iterator, err := table.Iterator(ctx, 0)
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

		name = primaryTable.Location().Location()
		svProto := pb.SourceVariant{
			Table: name,
			Definition: &pb.SourceVariant_PrimaryData{
				PrimaryData: &pb.PrimaryData{
					Location: &pb.PrimaryData_Table{
						Table: &pb.SQLTable{
							Name: name,
						},
					},
				},
			},
		}
		sv := metadata.WrapProtoSourceVariant(&svProto)

		table, err = store.GetPrimaryTable(test.PrimaryTable, *sv)
		if err != nil {
			t.Fatalf("Could not get primary table: %v", err)
		}
		ds, ok := table.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Dataset is not writeable: %v", err)
		}

		if err := ds.WriteBatch(ctx, GenericRecordsToRows(test.UpdatedRecords)); err != nil {
			t.Fatalf("Could not write updated records: %v", err)
		}

		if err := store.UpdateTransformation(test.Config); err != nil {
			t.Errorf("could not update transformation: %v", err)
		}
		tfDs, err := store.GetTransformationTable(test.Config.TargetTableID)
		if err != nil {
			t.Errorf("Could not get updated transformation table: %v", err)
		}

		iterator, err = tfDs.Iterator(ctx, 0)
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
				t.Fatalf("Unexpected training row: %v, expected %v in updated transformation", iterator.Values(), test.UpdatedExpected)
			}
			i++
		}
		if err := iterator.Close(); err != nil {
			t.Fatalf("Could not close iterator: %v", err)
		}
	}

	for name, test := range tests {
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			testTransform(t, testConst)
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
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "flt", ValueType: types.Float64},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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

		writableTable, ok := table.(dataset.WriteableDataset)
		if !ok {
			t.Fatalf("Table does not implement WritableDataset interface")
		}

		if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(test.Records)); err != nil {
			t.Fatalf("Could not write records: %v", err)
		}

		name := table.Location().Location()
		test.Config.Query = strings.Replace(test.Config.Query, "tb", name, 1)
		if err := store.CreateTransformation(test.Config); err != nil {
			t.Fatalf("Could not create transformation: %v", err)
		}
		sized, ok := table.(dataset.SizedDataset)
		if !ok {
			t.Fatalf("Table does not implement SizedDataset interface")
		}
		rows, err := sized.Len()
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
		Name:    table,
		Variant: uuid.NewString(),
		Type:    Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{
				Name:      "entity",
				ValueType: types.Int,
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
	firstTransformVariant := uuid.NewString()
	tests := map[string]TransformTest{
		"First": {
			PrimaryTable: ResourceID{
				Name:    uuid.NewString(),
				Variant: uuid.NewString(),
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int_col", ValueType: types.Int},
					{Name: "flt_col", ValueType: types.Float64},
					{Name: "str_col", ValueType: types.String},
					{Name: "bool_col", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
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
					Name:    firstTransformName,
					Variant: firstTransformVariant,
					Type:    Transformation,
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
				Name:    firstTransformName,
				Variant: firstTransformVariant,
				Type:    Primary,
			},
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int_col", ValueType: types.Int},
					{Name: "str_col", ValueType: types.String},
				},
			},
			Config: TransformationConfig{
				Type: SQLTransformation,
				TargetTableID: ResourceID{
					Name:    uuid.NewString(),
					Variant: uuid.NewString(),
					Type:    Transformation,
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
	writableTable, ok := table.(dataset.WriteableDataset)
	if !ok {
		t.Fatalf("Table does not implement WritableDataset interface")
	}
	if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(tests["First"].Records)); err != nil {
		t.Fatalf("Could not write batch: %v", err)
	}

	config := TransformationConfig{
		Type: SQLTransformation,
		TargetTableID: ResourceID{
			Name:    firstTransformName,
			Variant: firstTransformVariant,
			Type:    Transformation,
		},
		Query: "SELECT entity, int_col, flt_col, str_col FROM tb",
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: "tb",
				Source:   "TBD",
			},
		},
	}
	name := table.Location().Location()
	modifyTransformationConfig(t, t.Name(), name, store.Type(), &config)

	if err := store.CreateTransformation(config); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}
	sized, ok := table.(dataset.SizedDataset)
	if !ok {
		t.Fatalf("Table does not implement SizedDataset interface")
	}
	rows, err := sized.Len()
	if err != nil {
		t.Fatalf("could not get NumRows of table: %v", err)
	}
	if int(rows) != len(tests["First"].Records) {
		t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(tests["First"].Records), rows)
	}
	ds, err := store.GetTransformationTable(tests["First"].Config.TargetTableID)
	if err != nil {
		t.Errorf("Could not get transformation table: %v", err)
	}
	iterator, err := ds.Iterator(ctx, 0)
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

	secondTransformName := uuid.NewString()
	secondTransformVariant := uuid.NewString()
	config = TransformationConfig{
		Type: SQLTransformation,
		TargetTableID: ResourceID{
			Name:    secondTransformName,
			Variant: secondTransformVariant,
			Type:    Transformation,
		},
		Query: "SELECT Count(*) as total_count FROM tb",
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: "tb",
				Source:   "TBD",
			},
		},
	}

	name = table.Location().Location()
	modifyTransformationConfig(t, t.Name(), name, store.Type(), &config)
	if err := store.CreateTransformation(config); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}

	tfDs, err := store.GetTransformationTable(config.TargetTableID)
	if err != nil {
		t.Errorf("Could not get transformation table: %v", err)
	}
	iterator, err = tfDs.Iterator(ctx, 0)
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
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
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
	writableTable, ok := table.(dataset.WriteableDataset)
	if !ok {
		t.Fatalf("Table does not implement WritableDataset interface")
	}
	if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(tests["First"].Records)); err != nil {
		t.Fatalf("Could not write batch: %v", err)
	}

	name := table.Location().Location()
	config := TransformationConfig{
		Type: SQLTransformation,
		TargetTableID: ResourceID{
			Name: firstTransformName,
			Type: Transformation,
		},
		Query: fmt.Sprintf("SELECT entity, int, flt, str FROM %s", name),
		SourceMapping: []SourceMapping{
			SourceMapping{
				Template: name,
				Source:   name,
			},
		},
	}
	if err := store.CreateTransformation(config); err != nil {
		t.Fatalf("Could not create transformation: %v", err)
	}

	sized, ok := table.(dataset.SizedDataset)
	if !ok {
		t.Fatalf("Table does not implement SizedDataset interface")
	}
	rows, err := sized.Len()
	if err != nil {
		t.Fatalf("could not get NumRows of table: %v", err)
	}
	if int(rows) != len(tests["First"].Records) {
		t.Fatalf("NumRows do not match. Expected: %d, Got: %d", len(tests["First"].Records), rows)
	}
	mat, err := store.CreateMaterialization(tests["First"].Config.TargetTableID, MaterializationOptions{Output: fs.Parquet})
	if err != nil {
		t.Fatalf("Could not create materialization: %v", err)
	}
	ctx := logging.NewTestContext(t)
	iterator, err := mat.IterateSegment(ctx, 0, 10)
	if err != nil {
		t.Fatalf("Could not get iterator: %v", err)
	}
	i := 0
	for iterator.Next() {
		if !reflect.DeepEqual(iterator.Values(), tests["First"].Expected[i]) {
			t.Fatalf("Expected: %#v, Got: %#v", tests["First"].Expected[i], iterator.Values())
		}
		i++
	}
	if err := iterator.Close(); err != nil {
		t.Fatalf("Could not close iterator: %v", err)
	}
}

func testCreateResourceFromSource(t *testing.T, store OfflineStore) {
	primaryID := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: types.String},
			{Name: "col2", ValueType: types.Int},
			{Name: "col3", ValueType: types.String},
			{Name: "col4", ValueType: types.Timestamp},
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
	writableTable, ok := table.(dataset.WriteableDataset)
	if !ok {
		t.Fatalf("Table does not implement WritableDataset interface")
	}
	if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(records)); err != nil {
		t.Fatalf("Could not write batch: %v", err)
	}

	var location pl.Location
	name := table.Location().Location()
	if store.Type() == pt.SparkOffline {
		sparkStore := store.(*SparkOfflineStore)
		fp, err := sparkStore.Store.CreateFilePath(name, false)
		if err != nil {
			t.Fatalf("Could not create file path: %v", err)
		}
		location = pl.NewFileLocation(fp)
	} else {
		location = pl.NewSQLLocation(name)
	}

	featureID := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Feature,
	}
	recSchema := ResourceSchema{
		Entity:      "col1",
		Value:       "col2",
		TS:          "col4",
		SourceTable: location,
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
	mat, err := store.CreateMaterialization(featureID, MaterializationOptions{Output: fs.Parquet})
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

	if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(updatedRecords)); err != nil {
		t.Fatalf("Could not write batch: %v", err)
	}
	err = store.DeleteMaterialization(MaterializationID(mat.ID()))
	if err != nil {
		t.Fatalf("Could not delete materialization: %v", err)
	}
	mat, err = store.CreateMaterialization(featureID, MaterializationOptions{Output: fs.Parquet})
	if err != nil {
		t.Fatalf("Could not recreate materialization: %v", err)
	}
	sized, ok := table.(dataset.SizedDataset)
	if !ok {
		t.Fatalf("Table does not implement SizedDataset interface")
	}
	expected, err := sized.Len()
	if err != nil {
		t.Fatalf("Could not get resource table rows: %v", err)
	}
	actual, err := mat.Len()
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
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: types.String},
			{Name: "col2", ValueType: types.Int},
			{Name: "col3", ValueType: types.String},
			{Name: "col4", ValueType: types.Bool},
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

	writableTable, ok := table.(dataset.WriteableDataset)
	if !ok {
		t.Fatalf("Table does not implement WritableDataset interface")
	}
	if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(records)); err != nil {
		t.Fatalf("Could not write batch: %v", err)
	}

	var location pl.Location
	name := table.Location().Location()
	if store.Type() == pt.SparkOffline {
		sparkStore := store.(*SparkOfflineStore)
		fp, err := sparkStore.Store.CreateFilePath(name, false)
		if err != nil {
			t.Fatalf("Could not create file path: %v", err)
		}
		location = pl.NewFileLocation(fp)
	} else {
		location = pl.NewSQLLocation(name)
	}

	featureID := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Feature,
	}
	recSchema := ResourceSchema{
		Entity:      "col1",
		Value:       "col2",
		SourceTable: location,
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
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Label,
	}
	labelSchema := ResourceSchema{
		Entity:      "col1",
		Value:       "col4",
		SourceTable: location,
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
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    TrainingSet,
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

func testCreatePrimaryFromNonExistentSource(t *testing.T, store OfflineStore) {
	primaryID := ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: types.String},
			{Name: "col2", ValueType: types.Int},
			{Name: "col3", ValueType: types.String},
			{Name: "col4", ValueType: types.Timestamp},
		},
	}

	table, err := store.CreatePrimaryTable(primaryID, schema)
	if err != nil {
		t.Fatalf("Could not create primary table: %v", err)
	}
	tableName := fmt.Sprintf("%s_%s", "nonexistent", table.Location().Location())
	var primaryErr error
	if store.Type() == pt.SparkOffline {
		// cast to correct location
		location, ok := table.Location().(*pl.FileStoreLocation)
		if !ok {
			t.Fatalf("Table location is not a FileStoreLocation")
		}
		sourceTablePath := location.Filepath()
		if err != nil {
			t.Fatalf("Could not get source table path: %v", err)
		}

		_, primaryErr = store.RegisterPrimaryFromSourceTable(primaryID, pl.NewFileLocation(sourceTablePath))
	} else {
		_, primaryErr = store.RegisterPrimaryFromSourceTable(primaryID, pl.NewSQLLocation(tableName))
	}

	if primaryErr == nil {
		t.Fatalf("Successfully created primary table from non-existent source")
	}
	if strings.Contains(primaryErr.Error(), "source does not exist") {
		t.Fatalf("error message doesn't match got: %s", err.Error())
	}
}

func createUUID() string {
	return strings.Replace(uuid.NewString(), "-", "_", -1)
}

func testCreatePrimaryFromSource(t *testing.T, store OfflineStore) {
	primaryID := ResourceID{
		Name:    createUUID(),
		Variant: createUUID(),
		Type:    Primary,
	}
	schema := TableSchema{
		Columns: []TableColumn{
			{Name: "col1", ValueType: types.String},
			{Name: "col2", ValueType: types.Int},
			{Name: "col3", ValueType: types.String},
			{Name: "col4", ValueType: types.Timestamp},
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
	writableTable, ok := table.(dataset.WriteableDataset)
	if !ok {
		t.Fatalf("Table does not implement WritableDataset interface")
	}
	if err := writableTable.WriteBatch(ctx, GenericRecordsToRows(records)); err != nil {
		t.Fatalf("Could not write batch: %v", err)
	}
	primaryCopyID := ResourceID{
		Name:    createUUID(),
		Variant: createUUID(),
		Type:    Primary,
	}

	t.Log("Primary Name: ", primaryCopyID.Name)
	// Need to sanitize name here b/c the the xxx-xxx format of the uuid. Cannot do it within
	// register function because precreated tables do not necessarily use double quotes
	tableName := table.Location().Location()
	t.Log("Table Name: ", tableName)
	// Currently, the assumption is that a primary table will always have an absolute path
	// to the source data file in its schema; to keep with this assumption until we determine
	// a better approach (e.g. handling directories of primary sources), we will use the
	// GetSource method on the FileStorePrimaryTable to get the absolute path to the source.
	if store.Type() == pt.SparkOffline {

		location, ok := table.Location().(*pl.FileStoreLocation)
		if !ok {
			t.Fatalf("Table location is not a FileStoreLocation")
		}
		sourceTablePath := location.Filepath()

		_, err = store.RegisterPrimaryFromSourceTable(primaryCopyID, pl.NewFileLocation(sourceTablePath))
		if err != nil {
			t.Fatalf("Could not register from Source Table: %s", err)
		}
	} else {
		_, err = store.RegisterPrimaryFromSourceTable(primaryCopyID, pl.NewSQLLocation(tableName))
		if err != nil {
			t.Fatalf("Could not register from Source Table: %s", err)
		}
	}

	svProto := pb.SourceVariant{
		Table: tableName,
		Definition: &pb.SourceVariant_PrimaryData{
			PrimaryData: &pb.PrimaryData{
				Location: &pb.PrimaryData_Table{
					Table: &pb.SQLTable{
						Name: tableName,
					},
				},
			},
		},
	}
	sv := metadata.WrapProtoSourceVariant(&svProto)
	_, err = store.GetPrimaryTable(primaryCopyID, *sv)
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
	} else if strings.Contains(testName, "CLICKHOUSE") {
		tableName = SanitizeClickHouseIdentifier(tableName)
	} else {
		tableName = sanitize(tableName)
	}
	return tableName
}

func sanitizeTableName(testName string, tableName string) string {
	if !strings.Contains(testName, "BIGQUERY") {
		tableName = sanitize(tableName)
	} else if strings.Contains(testName, "CLICKHOUSE") {
		tableName = SanitizeClickHouseIdentifier(tableName)
	}
	return tableName
}

func modifyTransformationConfig(t *testing.T, testName, tableName string, providerType pt.Type, config *TransformationConfig) {
	switch providerType {
	case pt.SparkOffline:
		// In contrast to the SQL provider, that only needed change is the table name to perform the required transformation configuration,
		// The Spark implementation needs to update the source mappings to ensure the source file is used in the transformation query.
		config.SourceMapping[0].Source = tableName
	case pt.MemoryOffline, pt.BigQueryOffline, pt.PostgresOffline, pt.MySqlOffline, pt.SnowflakeOffline, pt.ClickHouseOffline, pt.RedshiftOffline:
		tableName := getTableName(string(providerType), tableName)
		config.Query = strings.Replace(config.Query, "tb", tableName, 1)
	default:
		t.Fatalf("Unrecognized provider type %s", providerType)
	}
}

func TestBigQueryConfig_Deserialize(t *testing.T) {
	content, err := os.ReadFile("connection/connection_configs.json")
	if err != nil {
		t.Fatalf(err.Error())
	}
	var payload map[string]interface{}
	err = json.Unmarshal(content, &payload)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testConfig := payload["BigQueryConfig"].(map[string]interface{})

	bgconfig := pc.BigQueryConfig{
		ProjectId:   testConfig["ProjectID"].(string),
		DatasetId:   testConfig["DatasetID"].(string),
		Credentials: testConfig["Credentials"].(map[string]interface{}),
	}

	serialized := bgconfig.Serialize()

	type fields struct {
		ProjectId   string
		DatasetId   string
		Credentials map[string]interface{}
	}
	type args struct {
		config pc.SerializedConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "TestCredentials",
			fields: fields{
				ProjectId:   testConfig["ProjectID"].(string),
				DatasetId:   testConfig["DatasetID"].(string),
				Credentials: testConfig["Credentials"].(map[string]interface{}),
			},
			args: args{
				config: serialized,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := &pc.BigQueryConfig{
				ProjectId:   tt.fields.ProjectId,
				DatasetId:   tt.fields.DatasetId,
				Credentials: tt.fields.Credentials,
			}
			if err := bq.Deserialize(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func testLagFeaturesTrainingSet(t *testing.T, store OfflineStore) {
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
		LagFeatures    []func(ResourceID) LagFeatureDef
	}

	tests := map[string]TestCase{
		"NoLag": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1, TS: time.UnixMilli(1)},
					{Entity: "b", Value: 2, TS: time.UnixMilli(1)},
					{Entity: "c", Value: 3, TS: time.UnixMilli(1)},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
			},
			LagFeatures: []func(ResourceID) LagFeatureDef{
				func(id ResourceID) LagFeatureDef {
					return LagFeatureDef{
						FeatureName:    id.Name,
						FeatureVariant: id.Variant,
						LagName:        "",
						LagDelta:       time.Millisecond * 0,
					}
				},
			},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: true, TS: time.UnixMilli(1)},
				{Entity: "b", Value: false, TS: time.UnixMilli(1)},
				{Entity: "c", Value: true, TS: time.UnixMilli(1)},
			},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
				},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						1,
						1,
					},
					Label: true,
				},
				{
					Features: []interface{}{
						2,
						2,
					},
					Label: false,
				},
				{
					Features: []interface{}{
						3,
						3,
					},
					Label: true,
				},
			},
		},
		"SimpleLags": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1, TS: time.UnixMilli(1)},
					{Entity: "b", Value: 2, TS: time.UnixMilli(1)},
					{Entity: "c", Value: 3, TS: time.UnixMilli(1)},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
			},
			LagFeatures: []func(ResourceID) LagFeatureDef{
				func(id ResourceID) LagFeatureDef {
					return LagFeatureDef{
						FeatureName:    id.Name,
						FeatureVariant: id.Variant,
						LagName:        "",
						LagDelta:       time.Millisecond,
					}
				},
				func(id ResourceID) LagFeatureDef {
					return LagFeatureDef{
						FeatureName:    id.Name,
						FeatureVariant: id.Variant,
						LagName:        "",
						LagDelta:       time.Millisecond * 2,
					}
				},
			},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: 10, TS: time.UnixMilli(1)},
				{Entity: "b", Value: 20, TS: time.UnixMilli(2)},
				{Entity: "b", Value: 30, TS: time.UnixMilli(3)},
			},
			LabelSchema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "value", ValueType: types.Int},
					{Name: "ts", ValueType: types.Timestamp},
				},
			},
			ExpectedRows: []expectedTrainingRow{
				{
					Features: []interface{}{
						1, nil, nil,
					},
					Label: 10,
				},
				{
					Features: []interface{}{
						2, 1, nil,
					},
					Label: 20,
				},
				{
					Features: []interface{}{
						3, 2, 1,
					},
					Label: 30,
				},
				{
					Features: []interface{}{
						4, 3, 2,
					},
					Label: 40,
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
			if err := table.WriteBatch(recs); err != nil {
				t.Fatalf("Failed to write record %v: %v", recs, err)
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, test.LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		if err := labelTable.WriteBatch(test.LabelRecords); err != nil {
			t.Fatalf("Failed to write record %v", test.LabelRecords)
		}
		lagFeatureList := make([]LagFeatureDef, 0)
		for _, lagFeatureDef := range test.LagFeatures {
			// tests implicitly create lag feature from first listed feature
			lagFeatureList = append(lagFeatureList, lagFeatureDef(featureIDs[0]))
		}
		def := TrainingSetDef{
			ID:          randomID(TrainingSet),
			Label:       labelID,
			Features:    featureIDs,
			LagFeatures: lagFeatureList,
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
			// then we delete that row. This is inefficient, but these test
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			runTestCase(t, testConst)
		})
	}
}

func TestTableSchemaValue(t *testing.T) {
	tableSchema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "int", ValueType: types.Int},
			{Name: "flt", ValueType: types.Float64},
			{Name: "str", ValueType: types.String},
			{Name: "bool", ValueType: types.Bool},
			{Name: "ts", ValueType: types.Timestamp},
		},
	}

	value := tableSchema.AsReflectedStruct().Elem()
	typ := value.Type()

	if typ.Kind() != reflect.Struct {
		t.Fatalf("Expected type to be struct, got %v", typ.Kind())
	}

	type expectedField struct {
		Name string
		Type reflect.Type
		Tag  reflect.StructTag
	}

	expectedFields := []expectedField{
		{Name: "Entity", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: reflect.StructTag(`parquet:"entity,optional"`)},
		{Name: "Int", Type: reflect.PointerTo(reflect.TypeOf(int(0))), Tag: reflect.StructTag(`parquet:"int,optional"`)},
		{Name: "Flt", Type: reflect.PointerTo(reflect.TypeOf(float64(0))), Tag: reflect.StructTag(`parquet:"flt,optional"`)},
		{Name: "Str", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: reflect.StructTag(`parquet:"str,optional"`)},
		{Name: "Bool", Type: reflect.PointerTo(reflect.TypeOf(false)), Tag: reflect.StructTag(`parquet:"bool,optional"`)},
		{Name: "Ts", Type: reflect.TypeOf(time.UnixMilli(0)), Tag: reflect.StructTag(`parquet:"ts,optional,timestamp"`)},
	}

	for _, fieldName := range expectedFields {
		field, ok := typ.FieldByName(fieldName.Name)
		if !ok {
			t.Fatalf("Expected field %s is missing", fieldName)
		}
		if field.Type != fieldName.Type {
			t.Fatalf("Expected field %s to be type %v, got %v", fieldName.Name, fieldName.Type, field.Type)
		}
		if field.Tag != fieldName.Tag {
			t.Fatalf("Expected field %s to have tag %v, got %v", fieldName.Name, fieldName.Tag, field.Tag)
		}
	}

	if typ.NumField() != len(expectedFields) {
		t.Fatalf("Expected %v fields, got %v", len(expectedFields), typ.NumField())
	}
}

func testBatchFeature(t *testing.T, store OfflineStore) {
	if store.Type() != pt.SnowflakeOffline && store.Type() != pt.SparkOffline && store.Type() != pt.ClickHouseOffline {
		t.Skip("Skipping test for non-SnowflakeOffline, SparkOffline or ClickHouseOffline providers")
	}
	type expectedBatchRow struct {
		Entity   interface{}
		Features []interface{}
	}
	type TestCase struct {
		FeatureRecords [][]ResourceRecord
		ExpectedRows   []expectedBatchRow
		FeatureSchema  []TableSchema
	}

	tests := map[string]TestCase{
		// 1. An empty feature -> just returns an empty iterator
		"Empty": {
			FeatureRecords: [][]ResourceRecord{
				{},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
			},
			// No rows expected
			ExpectedRows: []expectedBatchRow{},
		},
		// 2. A single feature -> you write a list of features, we just return that same list
		"SingleFeature": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1, TS: time.UnixMilli(1)},
					{Entity: "b", Value: 2, TS: time.UnixMilli(1)},
					{Entity: "c", Value: 3, TS: time.UnixMilli(1)},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
						{Name: "ts", ValueType: types.Timestamp},
					},
				},
			},
			ExpectedRows: []expectedBatchRow{
				{
					Entity: "a",
					Features: []interface{}{
						1,
					},
				},
				{
					Entity: "b",
					Features: []interface{}{
						2,
					},
				},
				{
					Entity: "c",
					Features: []interface{}{
						3,
					},
				},
			},
		},

		// 3. Two features
		"SimpleJoin": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
				},
				{
					{Entity: "a", Value: false},
					{Entity: "b", Value: true},
					{Entity: "c", Value: true},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Bool},
					},
				},
			},
			ExpectedRows: []expectedBatchRow{
				{
					Entity: "a",
					Features: []interface{}{
						1,
						false,
					},
				},
				{
					Entity: "b",
					Features: []interface{}{
						2,
						true,
					},
				},
				{
					Entity: "c",
					Features: []interface{}{
						3,
						true,
					},
				},
			},
		},

		// 4. Three features with a missing entity
		"TripleJoin": {
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
					{Entity: "d", Value: "yellow"},
				},
				{
					{Entity: "a", Value: false},
					{Entity: "b", Value: true},
					{Entity: "c", Value: true},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Bool},
					},
				},
			},
			ExpectedRows: []expectedBatchRow{
				{
					Entity: "a",
					Features: []interface{}{
						1,
						"red",
						false,
					},
				},
				{
					Entity: "b",
					Features: []interface{}{
						2,
						"green",
						true,
					},
				},
				{
					Entity: "c",
					Features: []interface{}{
						3,
						"blue",
						true,
					},
				},
				{
					Entity: "d",
					Features: []interface{}{
						nil,
						"yellow",
						nil,
					},
				},
			},
		},
		// 4. Multiple features with a multiple missing entities
		"MultipleJoin": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
					{Entity: "e", Value: 5},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "d", Value: "yellow"},
					{Entity: "e", Value: "black"},
				},
				{
					{Entity: "b", Value: true},
					{Entity: "c", Value: true},
					{Entity: "d", Value: false},
					{Entity: "e", Value: true},
				},
				{
					{Entity: "a", Value: 343},
					{Entity: "b", Value: 546},
					{Entity: "c", Value: 7667},
					{Entity: "d", Value: 32},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Bool},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
			},
			ExpectedRows: []expectedBatchRow{
				{
					Entity: "a",
					Features: []interface{}{
						1,
						"red",
						nil,
						343,
					},
				},
				{
					Entity: "b",
					Features: []interface{}{
						2,
						"green",
						true,
						546,
					},
				},
				{
					Entity: "c",
					Features: []interface{}{
						3,
						nil,
						true,
						7667,
					},
				},
				{
					Entity: "e",
					Features: []interface{}{
						5,
						"black",
						true,
						nil,
					},
				},
				{
					Entity: "d",
					Features: []interface{}{
						nil,
						"yellow",
						false,
						32,
					},
				},
			},
		},
		// 5. Multiple tables of different sizes
		"VariableJoin": {
			FeatureRecords: [][]ResourceRecord{
				{
					{Entity: "a", Value: 1},
					{Entity: "b", Value: 2},
					{Entity: "c", Value: 3},
					{Entity: "e", Value: 5},
					{Entity: "f", Value: 6},
				},
				{
					{Entity: "a", Value: "red"},
					{Entity: "b", Value: "green"},
					{Entity: "d", Value: "yellow"},
					{Entity: "e", Value: "black"},
				},
				{
					{Entity: "b", Value: true},
					{Entity: "c", Value: true},
					{Entity: "d", Value: false},
					{Entity: "e", Value: true},
				},
				{
					{Entity: "a", Value: 343},
					{Entity: "b", Value: 546},
					{Entity: "c", Value: 7667},
					{Entity: "d", Value: 32},
					{Entity: "e", Value: 53},
					{Entity: "f", Value: 64556},
				},
			},
			FeatureSchema: []TableSchema{
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.String},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Bool},
					},
				},
				{
					Columns: []TableColumn{
						{Name: "entity", ValueType: types.String},
						{Name: "value", ValueType: types.Int},
					},
				},
			},
			ExpectedRows: []expectedBatchRow{
				{
					Entity: "a",
					Features: []interface{}{
						1,
						"red",
						nil,
						343,
					},
				},
				{
					Entity: "b",
					Features: []interface{}{
						2,
						"green",
						true,
						546,
					},
				},
				{
					Entity: "c",
					Features: []interface{}{
						3,
						nil,
						true,
						7667,
					},
				},
				{
					Entity: "e",
					Features: []interface{}{
						5,
						"black",
						true,
						53,
					},
				},
				{
					Entity: "d",
					Features: []interface{}{
						nil,
						"yellow",
						false,
						32,
					},
				},
				{
					Entity: "f",
					Features: []interface{}{
						6,
						nil,
						nil,
						64556,
					},
				},
			},
		},
	}
	runTestCase := func(t *testing.T, test TestCase) {
		// We have a resource ID list where each resource ID corresponds to a feature
		featureIDs := make([]ResourceID, len(test.FeatureRecords))

		for i, recs := range test.FeatureRecords {
			id := randomID(Feature)
			featureIDs[i] = id
			// Making a table storing the corresponding Resource IDs and the feature (schema)
			// Create a Resource Table
			table, err := store.CreateResourceTable(id, test.FeatureSchema[i])
			if err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
			if err := table.WriteBatch(recs); err != nil {
				t.Fatalf("Failed to write batch: %v", err)
			}
			_, err = store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet})
			if err != nil {
				t.Fatalf("Failed to create materialization: %s", err)
			}
		}
		iter, err := store.GetBatchFeatures(featureIDs)
		if err != nil {
			t.Fatalf("Failed to get batch of features: %s", err)
		}

		i := 0
		expectedRows := test.ExpectedRows
		for iter.Next() {
			realRow := expectedBatchRow{
				Entity:   iter.Entity(),
				Features: iter.Features(),
			}
			// Row order isn't guaranteed, we make sure one row is equivalent
			// then we delete that row. This is inefficient, but these test
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
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.MemoryOffline {
				t.Parallel()
			}
			runTestCase(t, testConst)
		})
	}
}

func TestTableSchemaToParquetRecords(t *testing.T) {
	type TableSchemaTest struct {
		Schema               TableSchema
		Records              []GenericRecord
		ExpectParquetRecords []GenericRecord
	}

	tests := map[string]TableSchemaTest{
		"WithoutNilValues": {
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "flt", ValueType: types.Float64},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, 1.3, "third string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0).UTC()},
			},
			ExpectParquetRecords: []GenericRecord{
				[]interface{}{"a", 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", 2, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, 1.3, "third string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, "fourth string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", true, time.UnixMilli(0).UTC()},
			},
		},
		"WithNilValues": {
			Schema: TableSchema{
				Columns: []TableColumn{
					{Name: "entity", ValueType: types.String},
					{Name: "int", ValueType: types.Int},
					{Name: "flt", ValueType: types.Float64},
					{Name: "str", ValueType: types.String},
					{Name: "bool", ValueType: types.Bool},
					{Name: "ts", ValueType: types.Timestamp},
				},
			},
			Records: []GenericRecord{
				[]interface{}{nil, 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", nil, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, nil, "third string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, nil, false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", nil, time.UnixMilli(0).UTC()},
				[]interface{}{"f", 6, 1.6, "sixth string", false, nil},
			},
			ExpectParquetRecords: []GenericRecord{
				[]interface{}{nil, 1, 1.1, "test string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"b", nil, 1.2, "second string", false, time.UnixMilli(0).UTC()},
				[]interface{}{"c", 3, nil, "third string", true, time.UnixMilli(0).UTC()},
				[]interface{}{"d", 4, 1.4, nil, false, time.UnixMilli(0).UTC()},
				[]interface{}{"e", 5, 1.5, "fifth string", nil, time.UnixMilli(0).UTC()},
				[]interface{}{"f", 6, 1.6, "sixth string", false, nil},
			},
		},
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Could not create output directory: %v", err)
	}

	testSchema := func(t *testing.T, test TableSchemaTest) {
		testFilename := fmt.Sprintf("%s/generic_records_%s.parquet", outputDir, uuid.NewString())
		schema := test.Schema.AsParquetSchema()
		parquetRecords, err := test.Schema.ToParquetRecords(test.Records)
		if err != nil {
			t.Fatalf("Could not write parquet records: %v", err)
		}
		buf := new(bytes.Buffer)
		writeErr := parquet.Write(buf, parquetRecords, schema)
		if writeErr != nil {
			t.Fatalf("Could not write parquet records: %v", writeErr)
		}
		wFileErr := os.WriteFile(testFilename, buf.Bytes(), 0644)
		if wFileErr != nil {
			t.Fatalf("Could not write parquet file: %v", wFileErr)
		}
		data, err := os.Open(testFilename)
		if err != nil {
			t.Fatalf("Could not read parquet file: %v", err)
		}
		iter, err := newParquetIterator(data, -1)
		if err != nil {
			t.Fatalf("Could not create iterator: %v", err)
		}
		actualRecords := make([]GenericRecord, 0)
		for {
			if hasNext := iter.Next(); !hasNext {
				if err := iter.Err(); err != nil {
					t.Fatalf("Could not iterate: %v", err)
				}
				break
			}
			actualRecords = append(actualRecords, iter.Values())
		}
		if !reflect.DeepEqual(test.ExpectParquetRecords, actualRecords) {
			t.Fatalf("Expected: %v\nGot: %v", test.ExpectParquetRecords, actualRecords)
		}
	}

	for name, test := range tests {
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			t.Parallel()
			testSchema(t, testConst)
		})
	}
}

func testTrainTestSplit(t *testing.T, store OfflineStore) {
	FeatureRecords := [][]ResourceRecord{
		{
			{Entity: "a", Value: 1},
			{Entity: "b", Value: 2},
			{Entity: "c", Value: 3},
			{Entity: "d", Value: 4},
			{Entity: "e", Value: 5},
			{Entity: "f", Value: 6},
			{Entity: "g", Value: 7},
			{Entity: "h", Value: 8},
			{Entity: "i", Value: 9},
			{Entity: "j", Value: 10},
		},
		{
			{Entity: "a", Value: "red"},
			{Entity: "b", Value: "green"},
			{Entity: "c", Value: "blue"},
			{Entity: "d", Value: "purple"},
			{Entity: "e", Value: "blue"},
			{Entity: "f", Value: "green"},
			{Entity: "g", Value: "red"},
			{Entity: "h", Value: "purple"},
			{Entity: "i", Value: "yellow"},
			{Entity: "j", Value: "pink"},
		},
	}
	FeatureSchema := []TableSchema{
		{
			Columns: []TableColumn{
				{Name: "entity", ValueType: types.String},
				{Name: "value", ValueType: types.Int},
			},
		},
		{
			Columns: []TableColumn{
				{Name: "entity", ValueType: types.String},
				{Name: "value", ValueType: types.String},
			},
		},
	}
	LabelRecords := []ResourceRecord{
		{Entity: "a", Value: true},
		{Entity: "b", Value: false},
		{Entity: "c", Value: true},
		{Entity: "d", Value: false},
		{Entity: "e", Value: true},
		{Entity: "f", Value: true},
		{Entity: "g", Value: true},
		{Entity: "h", Value: false},
		{Entity: "i", Value: true},
		{Entity: "j", Value: true},
	}
	LabelSchema := TableSchema{
		Columns: []TableColumn{
			{Name: "entity", ValueType: types.String},
			{Name: "value", ValueType: types.Bool},
		},
	}

	type TestParameters struct {
		TestSize          float32
		Shuffle           bool
		RandomState       int
		RandomState2      int
		ExpectedTestRows  int
		IterShouldBeEqual bool
	}

	type TestCase struct {
		TestParameters
		TestFunction func(t *testing.T, store OfflineStore, params TestParameters)
	}

	setupTable := func(t *testing.T, store OfflineStore) ResourceID {
		featureIDs := make([]ResourceID, len(FeatureRecords))

		for i, recs := range FeatureRecords {
			id := randomID(Feature)
			featureIDs[i] = id
			table, err := store.CreateResourceTable(id, FeatureSchema[i])
			if err != nil {
				t.Fatalf("Failed to create table: %s", err)
			}
			if err := table.WriteBatch(recs); err != nil {
				t.Fatalf("Failed to write batch: %v", err)
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		if err := labelTable.WriteBatch(LabelRecords); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		def := TrainingSetDef{
			ID:       randomID(TrainingSet),
			Label:    labelID,
			Features: featureIDs,
		}
		if err := store.CreateTrainingSet(def); err != nil {
			t.Fatalf("Failed to create training set: %s", err)
		}
		_, err = store.GetTrainingSet(def.ID)
		if err != nil {
			t.Fatalf("Failed to get training set: %s", err)
		}
		return def.ID
	}

	testSplit := func(t *testing.T, store OfflineStore, params TestParameters) {
		id := setupTable(t, store)
		trainTestSplitDef := TrainTestSplitDef{TrainingSetName: id.Name, TrainingSetVariant: id.Variant, TestSize: params.TestSize, Shuffle: params.Shuffle, RandomState: params.RandomState}
		cleanupFunc, err := store.CreateTrainTestSplit(trainTestSplitDef)
		defer cleanupFunc()
		if err != nil {
			t.Fatalf("failed to fetch train test split iterators: %v", err)
		}
		trainIter, testIter, err := store.GetTrainTestSplit(trainTestSplitDef)
		trainRows := 0
		for trainIter.Next() {
			trainRows += 1
		}
		testRows := 0
		for testIter.Next() {
			testRows += 1
		}
		if params.ExpectedTestRows != testRows {
			t.Fatalf("Expected %d test rows, got: %d", params.ExpectedTestRows, testRows)
		}
		if (10 - params.ExpectedTestRows) != trainRows {
			t.Fatalf("Expected %d train rows, got: %d", 10-params.ExpectedTestRows, testRows)
		}
	}

	testShuffle := func(t *testing.T, store OfflineStore, params TestParameters) {

		// helper function to extract the data from the TS iterator
		extractData := func(iter TrainingSetIterator) ([][]interface{}, []interface{}) {
			featureRows := make([][]interface{}, 0)
			labelRows := make([]interface{}, 0)
			for iter.Next() {
				featureRows = append(featureRows, iter.Features())
				labelRows = append(labelRows, iter.Label())
			}
			return featureRows, labelRows
		}

		id := setupTable(t, store)
		trainTestSplitDef := TrainTestSplitDef{TrainingSetName: id.Name, TrainingSetVariant: id.Variant, TestSize: params.TestSize, Shuffle: params.Shuffle, RandomState: params.RandomState}
		cleanupFunc, err := store.CreateTrainTestSplit(trainTestSplitDef)
		if err != nil {
			t.Fatalf("failed to fetch train test split iterators: %v", err)
		}
		trainIter, testIter, err := store.GetTrainTestSplit(trainTestSplitDef)
		if err != nil {
			t.Fatalf("failed to fetch train test split iterators: %v", err)
		}
		trainIter1FeatureRows, trainIter1LabelRows := extractData(trainIter)
		testIter1FeatureRows, testIter1LabelRows := extractData(testIter)
		if err := cleanupFunc(); err != nil {
			t.Fatalf("failed to drop views: %v", err)
		}

		trainTestSplitDef = TrainTestSplitDef{TrainingSetName: id.Name, TrainingSetVariant: id.Variant, TestSize: params.TestSize, Shuffle: params.Shuffle, RandomState: params.RandomState2}
		cleanupFunc, err = store.CreateTrainTestSplit(trainTestSplitDef)
		if err != nil {
			t.Fatalf("failed to fetch train test split iterators: %v", err)
		}
		trainIter2, testIter2, err := store.GetTrainTestSplit(trainTestSplitDef)
		if err != nil {
			t.Fatalf("failed to fetch second train test split iterators: %v", err)
		}
		trainIter2FeatureRows, trainIter2LabelRows := extractData(trainIter2)
		testIter2FeatureRows, testIter2LabelRows := extractData(testIter2)
		if err := cleanupFunc(); err != nil {
			t.Fatalf("failed to drop views: %v", err)
		}

		// compare training
		equalTrainRows := true
		for i, row := range trainIter1FeatureRows {
			if !reflect.DeepEqual(row, trainIter2FeatureRows[i]) {
				equalTrainRows = false
			}
			if !reflect.DeepEqual(trainIter1LabelRows[i], trainIter2LabelRows[i]) {
				equalTrainRows = false
			}
		}

		// compare test
		equalTestRows := true
		for i, row := range testIter1FeatureRows {
			if !reflect.DeepEqual(row, testIter2FeatureRows[i]) {
				equalTestRows = false
			}
			if !reflect.DeepEqual(testIter1LabelRows[i], testIter2LabelRows[i]) {
				equalTestRows = false
			}
		}

		if params.IterShouldBeEqual && equalTrainRows == false {
			t.Fatalf("Expected train calls to be equal but were not")
		}
		if params.IterShouldBeEqual && equalTestRows == false {
			t.Fatalf("Expected test calls to be equal but were not")
		}
		if !params.IterShouldBeEqual && equalTrainRows == true {
			t.Fatalf("Expected train calls to be different but were not")
		}
		if !params.IterShouldBeEqual && equalTestRows == true {
			t.Fatalf("Expected test calls to be different but were not")
		}
	}

	tests := map[string]TestCase{
		"Even Rows": {
			TestParameters: TestParameters{
				TestSize:         0.5,
				Shuffle:          true,
				RandomState:      1,
				ExpectedTestRows: 5,
			},
			TestFunction: testSplit,
		},
		"All Train Rows": {
			TestParameters: TestParameters{
				TestSize:         0.0,
				Shuffle:          true,
				RandomState:      1,
				ExpectedTestRows: 0,
			},
			TestFunction: testSplit,
		},
		"All Test Rows": {
			TestParameters: TestParameters{
				TestSize:         1,
				Shuffle:          true,
				RandomState:      1,
				ExpectedTestRows: 10,
			},
			TestFunction: testSplit,
		},
		"No Shuffle": {
			TestParameters: TestParameters{
				TestSize:          0.5,
				Shuffle:           false,
				RandomState:       0,
				RandomState2:      0,
				ExpectedTestRows:  5,
				IterShouldBeEqual: true,
			},
			TestFunction: testShuffle,
		},
		"Shuffle": {
			TestParameters: TestParameters{
				TestSize:          0.5,
				Shuffle:           true,
				RandomState:       0,
				RandomState2:      0,
				ExpectedTestRows:  5,
				IterShouldBeEqual: false,
			},
			TestFunction: testShuffle,
		},
		"Same Random State": {
			TestParameters: TestParameters{
				TestSize:          0.5,
				Shuffle:           true,
				RandomState:       1,
				RandomState2:      1,
				ExpectedTestRows:  5,
				IterShouldBeEqual: true,
			},
			TestFunction: testShuffle,
		},
		"Different Random State": {
			TestParameters: TestParameters{
				TestSize:          0.5,
				Shuffle:           true,
				RandomState:       1,
				RandomState2:      2,
				ExpectedTestRows:  5,
				IterShouldBeEqual: false,
			},
			TestFunction: testShuffle,
		},
	}

	for name, test := range tests {
		nameConst := name
		testConst := test
		t.Run(nameConst, func(t *testing.T) {
			if store.Type() != pt.ClickHouseOffline {
				t.Skip()
			}
			testConst.TestFunction(t, store, testConst.TestParameters)
		})
	}
}

func TestResumeOption(t *testing.T) {
	testSuite := map[string]func(t *testing.T){
		"Test Resume Constructor":    testResumeConstructor,
		"Test Resumable Constructor": testResumableConstructor,
		"Test Set Resume ID":         testSetResumeID,
		"Test Finish With Error":     testResumeFinishWithError,
		"Test Finish Called Twice":   testResumeFinishTwice,
		"Test Timeout":               testTimeout,
	}
	for name, test := range testSuite {
		t.Run(name, test)
	}
}

func testTimeout(t *testing.T) {
	opt := RunAsyncWithResume(time.Millisecond)
	done := make(chan struct{})
	go func() {
		if err := opt.Wait(); err == nil {
			t.Fatalf("Timeout didn't return an error")
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Wait didn't timeout")
	}
}

func testResumeFinishTwice(t *testing.T) {
	opt := RunAsyncWithResume(time.Second)
	if err := opt.finishWithError(nil); err != nil {
		t.Fatalf("First finish returned error: %s", err)
	}
	if err := opt.finishWithError(nil); err == nil {
		t.Fatalf("Second finish didn't have error")
	}
}

func testResumeFinishWithError(t *testing.T) {
	opt := RunAsyncWithResume(time.Second)
	expErr := fmt.Errorf("Expected Error")
	errChan := make(chan error)
	go func(opt *ResumeOption) {
		errChan <- opt.Wait()
	}(opt)
	if err := opt.finishWithError(expErr); err != nil {
		t.Fatalf("Finish failed: %s", err)
	}
	if err := <-errChan; err != expErr {
		t.Fatalf("Errors don't match. Want: %s. Got: %s", expErr, err)
	}

}

func testSetResumeID(t *testing.T) {
	opt := RunAsyncWithResume(time.Second)
	if opt.IsResumeIDSet() {
		t.Fatalf("ResumeID marked as set")
	}
	id := types.ResumeID("id")
	if err := opt.setResumeID(id); err != nil {
		t.Fatalf("Failed to set resume ID: %s", err)
	}
	gotID := opt.ResumeID()
	if !reflect.DeepEqual(gotID, id) {
		t.Fatalf("IDs do not match\nGot: %v\nWanted: %v", gotID, id)
	}
}

func testResumableConstructor(t *testing.T) {
	// Verify it casts correctly
	var opt TransformationOption
	opt = RunAsyncWithResume(time.Second)
	if opt == nil {
		t.Fatalf("RunAsyncWithResume returned nil")
	}
	if opt.Type() != ResumableTransformation {
		t.Fatalf("Type not set to ResumableTransformation")
	}
}

func testResumeConstructor(t *testing.T) {
	id := types.ResumeID("id")
	// Verify it casts correctly
	var opt TransformationOption
	var err error
	opt, err = ResumeOptionWithID(id, time.Second)
	if err != nil {
		t.Fatalf("Failed to create resume transfromation with ID: %s", err)
	}
	if opt.Type() != ResumableTransformation {
		t.Fatalf("Type not set to ResumableTransformation")
	}
	casted := opt.(*ResumeOption)
	gotID := casted.ResumeID()
	if !reflect.DeepEqual(gotID, id) {
		t.Fatalf("IDs do not match\nGot: %v\nWanted: %v", gotID, id)
	}
}

func TestResourceSchemaSerializationDeserialization(t *testing.T) {
	s3Filepath, err := filestore.NewEmptyFilepath(filestore.S3)
	if err != nil {
		t.Fatalf("Failed to create S3 filepath: %v", err)
	}
	if err := s3Filepath.ParseFilePath("s3://bucket/path/to/file"); err != nil {
		t.Fatalf("Failed to parse S3 filepath: %v", err)
	}

	testCases := []struct {
		name      string
		schema    *ResourceSchema
		expectErr bool
	}{
		{
			name: "SQL Location",
			schema: &ResourceSchema{
				Entity:         "entity1",
				Value:          "value1",
				TS:             "timestamp1",
				SourceTable:    pl.NewSQLLocation("test_table"),
				EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "entity", EntityColumn: "entity1"}}, ValueColumn: "value1", TimestampColumn: "timestamp1"},
			},
			expectErr: false,
		},
		{
			name: "FileStore Location",
			schema: &ResourceSchema{
				Entity:         "entity2",
				Value:          "value2",
				TS:             "timestamp2",
				SourceTable:    pl.NewFileLocation(s3Filepath),
				EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "entity", EntityColumn: "entity2"}}, ValueColumn: "value2", TimestampColumn: "timestamp2"},
			},
			expectErr: false,
		},
		{
			name: "Catalog Location",
			schema: &ResourceSchema{
				Entity:         "entity3",
				Value:          "value3",
				TS:             "timestamp3",
				SourceTable:    pl.NewCatalogLocation("test_db", "test_table", "iceberg"),
				EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "entity", EntityColumn: "entity3"}}, ValueColumn: "value3", TimestampColumn: "timestamp3"},
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.schema.Serialize()
			if (err != nil) != tc.expectErr {
				t.Errorf("Marshal() error = %v, expectErr %v", err, tc.expectErr)
				return
			}

			got := ResourceSchema{}
			err = got.Deserialize(data)
			if (err != nil) != tc.expectErr {
				t.Errorf("Unmarshal() error = %v, expectErr %v", err, tc.expectErr)
				return
			}

			assert.Equal(t, tc.schema.Entity, got.Entity)
			assert.Equal(t, tc.schema.Value, got.Value)
			assert.Equal(t, tc.schema.TS, got.TS)
			assert.Equal(t, tc.schema.SourceTable.Location(), got.SourceTable.Location())
			assert.Equal(t, tc.schema.SourceTable.Type(), got.SourceTable.Type())
			assert.DeepEqual(t, tc.schema.EntityMappings, got.EntityMappings)
		})
	}
}

func TestResourceSchemaValidation(t *testing.T) {
	tests := []struct {
		name      string
		schema    *ResourceSchema
		expectErr bool
	}{
		{
			name: "Valid Schema with SQL Location",
			schema: &ResourceSchema{
				Entity:      "entity",
				Value:       "value",
				TS:          "timestamp",
				SourceTable: pl.NewSQLLocation("test_table"),
			},
			expectErr: false,
		},
		{
			name: "Invalid Schema missing Entity and Value",
			schema: &ResourceSchema{
				Entity:      "",
				Value:       "",
				TS:          "",
				SourceTable: pl.NewSQLLocation("test_table"),
			},
			expectErr: true,
		},
		{
			name: "Invalid Schema missing SourceTable location",
			schema: &ResourceSchema{
				Entity:      "entity",
				Value:       "value",
				TS:          "",
				SourceTable: nil,
			},
			expectErr: true,
		},
		{
			name: "Invalid Schema with empty SourceTable location",
			schema: &ResourceSchema{
				Entity:      "entity",
				Value:       "value",
				TS:          "",
				SourceTable: pl.NewSQLLocation(""),
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.schema.Validate()
			if (err != nil) != tc.expectErr {
				t.Errorf("Validate() error = %v, expectErr %v", err, tc.expectErr)
				return
			}
		})
	}
}

func GenericRecordToRow(record GenericRecord) fftypes.Row {
	row := make([]fftypes.Value, len(record))
	for i, value := range record {
		row[i] = fftypes.Value{
			Value: value,
		}
	}
	return row
}

func GenericRecordsToRows(records []GenericRecord) []fftypes.Row {
	rows := make([]fftypes.Row, len(records))
	for i, record := range records {
		rows[i] = GenericRecordToRow(record)
	}
	return rows
}
