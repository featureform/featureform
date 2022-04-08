package provider

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestOfflineStores(t *testing.T) {
	testFns := map[string]func(*testing.T, OfflineStore){
		"CreateGetTable":          testCreateGetOfflineTable,
		"TableAlreadyExists":      testOfflineTableAlreadyExists,
		"TableNotFound":           testOfflineTableNotFound,
		"InvalidResourceIDs":      testInvalidResourceIDs,
		"Materializations":        testMaterializations,
		"InvalidResourceRecord":   testWriteInvalidResourceRecord,
		"InvalidMaterialization":  testInvalidMaterialization,
		"MaterializeUnknown":      testMaterializeUnknown,
		"MaterializationNotFound": testMaterializationNotFound,
		"TrainingSets":            testTrainingSet,
		"TrainingSetInvalidID":    testGetTrainingSetInvalidResourceID,
		"GetUnknownTrainingSet":   testGetUnkonwnTrainingSet,
		"InvalidTrainingSetDefs":  testInvalidTrainingSetDefs,
		"LabelTableNotFound":      testLabelTableNotFound,
		"FeatureTableNotFound":    testFeatureTableNotFound,
	}
	var postgresConfig = PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: os.Getenv("POSTGRES_DB"),
		Username: os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
	}
	serialPGConfig := postgresConfig.Serialize()
	testList := []struct {
		t               Type
		c               SerializedConfig
		integrationTest bool
	}{
		{MemoryOffline, []byte{}, false},
		{PostgresOffline, serialPGConfig, true},
	}
	for _, testItem := range testList {
		if testing.Short() && testItem.integrationTest {
			t.Logf("Skipping %s, because it is an integration test", testItem.t)
			continue
		}
		for name, fn := range testFns {
			provider, err := Get(testItem.t, testItem.c)
			if err != nil {
				t.Fatalf("Failed to get provider %s: %s", testItem.t, err)
			}
			store, err := provider.AsOfflineStore()
			if err != nil {
				t.Fatalf("Failed to use provider %s as OfflineStore: %s", testItem.t, err)
			}
			testName := fmt.Sprintf("%s_%s", testItem.t, name)
			t.Run(testName, func(t *testing.T) {
				fn(t, store)
			})
		}
	}
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
	schema := PostgresSchema{Int}
	if tab, err := store.CreateResourceTable(id, schema.Serialize()); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if tab, err := store.GetResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func testOfflineTableAlreadyExists(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	schema := PostgresSchema{Int}
	if _, err := store.CreateResourceTable(id, schema.Serialize()); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateResourceTable(id, schema.Serialize()); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*TableAlreadyExists); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func testOfflineTableNotFound(t *testing.T, store OfflineStore) {
	id := randomID(Feature, Label)
	if _, err := store.GetResourceTable(id); err == nil {
		t.Fatalf("Succeeded in getting non-existant table")
	} else if casted, valid := err.(*TableNotFound); !valid {
		t.Fatalf("Wrong error for table not found: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

func testMaterializations(t *testing.T, store OfflineStore) {
	type TestCase struct {
		WriteRecords             []ResourceRecord
		ValueType                SerializedSchema
		ExpectedRows             int64
		SegmentStart, SegmentEnd int64
		ExpectedSegment          []ResourceRecord
	}
	schemaInt := PostgresSchema{Int}
	tests := map[string]TestCase{
		"Empty": {
			WriteRecords:    []ResourceRecord{},
			ValueType:       schemaInt.Serialize(),
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
			ValueType:    schemaInt.Serialize(),
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
			},
		},
		"SubSegmentNoOverlap": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
			},
			ValueType:    schemaInt.Serialize(),
			ExpectedRows: 3,
			SegmentStart: 1,
			SegmentEnd:   2,
			ExpectedSegment: []ResourceRecord{
				{Entity: "b", Value: 2},
			},
		},
		"SimpleOverwrite": {
			WriteRecords: []ResourceRecord{
				{Entity: "a", Value: 1},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
				{Entity: "a", Value: 4},
			},
			ValueType:    schemaInt.Serialize(),
			ExpectedRows: 3,
			SegmentStart: 0,
			SegmentEnd:   3,
			ExpectedSegment: []ResourceRecord{
				{Entity: "a", Value: 4},
				{Entity: "b", Value: 2},
				{Entity: "c", Value: 3},
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
			ValueType:    schemaInt.Serialize(),
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
			ValueType:    schemaInt.Serialize(),
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
			ValueType:    schemaInt.Serialize(),
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
		for seg.Next() {
			actual := seg.Value()
			expected := test.ExpectedSegment[i]
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Value not equal in materialization: %v %v", actual, expected)
			}
			i++
		}
		if err := seg.Err(); err != nil {
			t.Fatalf("Iteration failed: %s", err)
		}
		if i < len(test.ExpectedSegment) {
			t.Fatalf("Segment is too small: %d", i)
		}
	}
	runTestCase := func(t *testing.T, test TestCase) {
		id := randomID(Feature)
		table, err := store.CreateResourceTable(id, test.ValueType)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		for _, rec := range test.WriteRecords {
			if err := table.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v: %s", rec, err)
			}
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
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}

}

func testWriteInvalidResourceRecord(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	schema := PostgresSchema{Int}
	table, err := store.CreateResourceTable(id, schema.Serialize())
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := table.Write(ResourceRecord{}); err == nil {
		t.Fatalf("Succeeded in writing invalid resource record")
	}
}

func testInvalidMaterialization(t *testing.T, store OfflineStore) {
	id := randomID(Label)
	schema := PostgresSchema{Int}
	if _, err := store.CreateResourceTable(id, schema.Serialize()); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateMaterialization(id); err == nil {
		t.Fatalf("Succeeded in materializing label")
	}
}

func testMaterializeUnknown(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	if _, err := store.CreateMaterialization(id); err == nil {
		t.Fatalf("Succeeded in materializing uninitialized resource")
	}
}

func testMaterializationNotFound(t *testing.T, store OfflineStore) {

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

func testInvalidResourceIDs(t *testing.T, store OfflineStore) {
	schema := PostgresSchema{Int}
	invalidIds := []ResourceID{
		{Type: Feature},
		{Name: uuid.NewString()},
	}
	for _, id := range invalidIds {
		if _, err := store.CreateResourceTable(id, schema.Serialize()); err == nil {
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
		FeatureSchema  [][]byte
		LabelSchema    []byte
	}
	schemaNil := PostgresSchema{NilType}
	nilByte := schemaNil.Serialize()
	schemaInt := PostgresSchema{Int}
	intByte := schemaInt.Serialize()
	schemaString := PostgresSchema{String}
	stringByte := schemaString.Serialize()
	schemaBool := PostgresSchema{Bool}
	boolByte := schemaBool.Serialize()
	tests := map[string]TestCase{
		"Empty": {
			FeatureRecords: [][]ResourceRecord{
				// One feature with no records.
				{},
			},
			LabelRecords:  []ResourceRecord{},
			FeatureSchema: [][]byte{nilByte},
			LabelSchema:   nilByte,
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
			FeatureSchema: [][]byte{intByte, stringByte},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: true},
				{Entity: "b", Value: false},
				{Entity: "c", Value: true},
			},
			LabelSchema: boolByte,
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
			FeatureSchema: [][]byte{intByte, stringByte, stringByte, stringByte, nilByte},
			LabelRecords: []ResourceRecord{
				{Entity: "a", Value: 1, TS: time.UnixMilli(10)},
				{Entity: "b", Value: 9, TS: time.UnixMilli(3)},
				{Entity: "b", Value: 5, TS: time.UnixMilli(5)},
				{Entity: "c", Value: 3, TS: time.UnixMilli(7)},
			},
			LabelSchema: intByte,
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
			for _, rec := range recs {
				if err := table.Write(rec); err != nil {
					t.Fatalf("Failed to write record %v", rec)
				}
			}
		}
		labelID := randomID(Label)
		labelTable, err := store.CreateResourceTable(labelID, test.LabelSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		for _, rec := range test.LabelRecords {
			if err := labelTable.Write(rec); err != nil {
				t.Fatalf("Failed to write record %v", rec)
			}
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
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func testGetTrainingSetInvalidResourceID(t *testing.T, store OfflineStore) {
	id := randomID(Feature)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting invalid training set ResourceID")
	}
}

func testGetUnkonwnTrainingSet(t *testing.T, store OfflineStore) {
	// This should default to TrainingSet
	id := randomID(NoType)
	if _, err := store.GetTrainingSet(id); err == nil {
		t.Fatalf("Succeeded in getting unknown training set ResourceID")
	} else if _, valid := err.(*TrainingSetNotFound); !valid {
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
		t.Run(name, func(t *testing.T) {
			if err := store.CreateTrainingSet(def); err == nil {
				t.Fatalf("Succeeded to create invalid def")
			}
		})
	}
}

func testLabelTableNotFound(t *testing.T, store OfflineStore) {
	featureID := randomID(Feature)
	schema := PostgresSchema{Int}
	if _, err := store.CreateResourceTable(featureID, schema.Serialize()); err != nil {
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
	schema := PostgresSchema{Int}
	if _, err := store.CreateResourceTable(labelID, schema.Serialize()); err != nil {
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
