// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func newSQLTransformationTest(tester offlineSqlStoreTester) *sqlTransformationTester {
	data := newTestSQLTransformationData(tester.Type(), tester.Config())
	return &sqlTransformationTester{
		tester: tester,
		data:   data,
	}
}

type sqlTransformationTester struct {
	tester offlineSqlStoreTester
	data   testSQLTransformationData
}

func newSQLMaterializationTest(tester offlineSqlStoreTester, useTimestamps bool) *sqlMaterializationTester {
	data := newTestSQLMaterializationData(useTimestamps)
	return &sqlMaterializationTester{
		tester: tester,
		data:   data,
	}
}

type sqlMaterializationTester struct {
	tester offlineMaterializationSqlStoreTester
	data   testSQLMaterializationData
}

func newSQLTrainingSetTest(tester offlineSqlStoreTester, tsDatasetType trainingSetDatasetType) *sqlTrainingSetTester {
	data := newTestSQLTrainingSetData(tester.Type(), tester.Config(), tsDatasetType)
	return &sqlTrainingSetTester{
		tester: tester,
		data:   data,
	}
}

type sqlTrainingSetTester struct {
	tester offlineTrainingSetSqlStoreTester
	data   testSQLTrainingSetData
}

func initSqlPrimaryDataset(t *testing.T, tester offlineSqlStoreDatasetTester, location pl.Location, schema TableSchema, records []GenericRecord) PrimaryTable {
	sqlLoc, isSqlLoc := location.(*pl.SQLLocation)
	if !isSqlLoc {
		t.Fatalf("expected SQL location: %v", err)
	}

	dbName := sqlLoc.GetDatabase()
	if dbName == "" {
		t.Fatalf("expected database name to be non-empty")
	}
	t.Logf("Creating Database: %s\n", dbName)
	if err := tester.CreateDatabase(dbName); err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		t.Logf("Dropping Database: %s\n", dbName)
		if err := tester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	schemaName := sqlLoc.GetSchema()
	if schemaName == "" {
		t.Fatalf("expected schema name to be non-empty")
	}
	if err := tester.CreateSchema(dbName, schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	if len(schema.Columns) == 0 {
		t.Fatalf("expected table schema to have columns")
	}
	table, err := tester.CreateTable(sqlLoc, schema)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}
	if err := table.WriteBatch(records); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}

	return table
}

func floatsAreClose(a, b, tolerance float64) bool { return math.Abs(a-b) <= tolerance }

func newIDCreator(variant string) *idCreator {
	if variant == "" {
		variant = "test"
	}
	return &idCreator{Variant: variant}
}

type idCreator struct {
	Variant string
}

func (a idCreator) create(t OfflineResourceType, name string) ResourceID {
	if name == "" {
		name = fmt.Sprintf("DUMMY_%s_%s", strings.ToUpper(t.String()), strings.ToUpper(uuid.NewString()[:5]))
	}
	switch t {
	case Primary:
		return ResourceID{Name: name, Variant: a.Variant, Type: Primary}
	case Transformation:
		return ResourceID{Name: name, Variant: a.Variant, Type: Transformation}
	case Feature:
		return ResourceID{Name: name, Variant: a.Variant, Type: Feature}
	case Label:
		return ResourceID{Name: name, Variant: a.Variant, Type: Label}
	case TrainingSet:
		return ResourceID{Name: name, Variant: a.Variant, Type: TrainingSet}
	default:
		return ResourceID{}
	}
}

func newTestSQLTransformationData(storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTransformationData {
	db := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_WIND_DATA_TABLE")
	sqlLoc := loc.(*pl.SQLLocation)
	tableLoc := sqlLoc.TableLocation()
	queryFmt := "SELECT location_id, AVG(wind_speed) as avg_daily_wind_speed, AVG(wind_duration) as avg_daily_wind_duration, AVG(fetch) as avg_daily_fetch, DATE(timestamp) as date FROM %s GROUP BY location_id, DATE(timestamp)"
	idCreator := newIDCreator("test")
	return testSQLTransformationData{
		schema: TableSchema{
			Columns: []TableColumn{
				{
					Name:      "LOCATION_ID",
					ValueType: types.String,
				},
				{
					Name:      "WIND_SPEED",
					ValueType: types.Float64,
				},
				{
					Name:      "WIND_DURATION",
					ValueType: types.Float64,
				},
				{
					Name:      "FETCH",
					ValueType: types.Float64,
				},
				{
					Name:      "TIMESTAMP",
					ValueType: types.Timestamp,
				},
			},
		},
		records: []GenericRecord{
			{
				"L0",
				10.0,
				13.0,
				200.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L1",
				15.0,
				10.0,
				150.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L2",
				20.0,
				15.0,
				250.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L1",
				6.5,
				17.25,
				350.75,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L2",
				18.33,
				12.10,
				200.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
		},
		expected: []GenericRecord{
			{
				"L0",
				10.0,
				13.0,
				200.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L1",
				10.75,
				13.625,
				250.375,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L2",
				19.165,
				13.55,
				225.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
		},
		location: loc,
		config: TransformationConfig{
			Type:          SQLTransformation,
			TargetTableID: idCreator.create(Transformation, ""),
			Query:         fmt.Sprintf(queryFmt, tableLoc.String()),
			SourceMapping: []SourceMapping{
				{
					Template:       SanitizeSqlLocation(tableLoc),
					Source:         tableLoc.String(),
					ProviderType:   storeType,
					ProviderConfig: storeConfig,
					Location:       loc,
				},
			},
		},
	}
}

type testSQLTransformationData struct {
	schema   TableSchema
	records  []GenericRecord
	expected []GenericRecord
	location pl.Location
	config   TransformationConfig
}

func (d testSQLTransformationData) Assert(t *testing.T, actual PrimaryTable) {
	entityIdx := 0
	avgWindSpeedIdx := 1
	avgWindDurationIdx := 2
	avgFetchIdx := 3
	tsIdx := 4

	numRows, err := actual.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, len(d.expected), int(numRows), "expected same number of rows")

	itr, err := actual.IterateSegment(100)
	if err != nil {
		t.Fatalf("could not get iterator: %v", err)
	}

	i := 0
	for itr.Next() {
		actual := itr.Values()
		expected := d.expected[i]
		assert.Equal(t, expected[entityIdx].(string), actual[entityIdx].(string), "expected same entity")
		assert.Equal(t, expected[avgWindSpeedIdx].(float64), actual[avgWindSpeedIdx].(float64), "expected same value for col 2")
		assert.Equal(t, expected[avgWindDurationIdx].(float64), actual[avgWindDurationIdx].(float64), "expected same value for col 3")
		assert.Equal(t, expected[avgFetchIdx].(float64), actual[avgFetchIdx].(float64), "expected same value for col 4")
		assert.Equal(t, expected[tsIdx].(time.Time).Truncate(time.Microsecond), actual[tsIdx].(time.Time).Truncate(time.Microsecond), "expected same ts")
		i++
	}
	if itr.Err() != nil {
		t.Fatalf("could not iterate over transformation: %v", itr.Err())
	}
}

func newTestSQLMaterializationData(useTimestamp bool) testSQLMaterializationData {
	db := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_WIND_DATA_TABLE")
	idCreator := newIDCreator("test")
	data := testSQLMaterializationData{
		id: idCreator.create(Feature, ""),
		schema: TableSchema{
			Columns: []TableColumn{
				{
					Name:      "LOCATION_ID",
					ValueType: types.String,
				},
				{
					Name:      "WIND_SPEED",
					ValueType: types.Float64,
				},
				{
					Name:      "WIND_DURATION",
					ValueType: types.Float64,
				},
				{
					Name:      "FETCH",
					ValueType: types.Float64,
				},
				{
					Name:      "TIMESTAMP",
					ValueType: types.Timestamp,
				},
			},
		},
		records: []GenericRecord{
			{
				"L0",
				10.0,
				13.0,
				200.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L1",
				10.75,
				13.625,
				250.375,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L2",
				19.165,
				13.55,
				225.0,
				time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				"L1",
				11.85,
				14.725,
				251.475,
				time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC),
			},
			{
				"L2",
				17.265,
				12.65,
				224.1,
				time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC),
			},
		},
		expected: []ResourceRecord{
			{
				Entity: "L0",
				Value:  10.0,
				TS:     time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC),
			},
			{
				Entity: "L1",
				Value:  11.85,
				TS:     time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC),
			},
			{
				Entity: "L2",
				Value:  17.265,
				TS:     time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC),
			},
		},
		incrementalRecords: []GenericRecord{
			{
				"L0",
				27.42,
				19.25,
				176.5,
				time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				"L1",
				6.85,
				8.33,
				450.175,
				time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				"L2",
				11.965,
				3.35,
				20.81,
				time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		incrementalExpected: []ResourceRecord{
			{
				Entity: "L0",
				Value:  27.42,
				TS:     time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				Entity: "L1",
				Value:  6.85,
				TS:     time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				Entity: "L2",
				Value:  11.965,
				TS:     time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		location: loc,
		opts: MaterializationOptions{
			Schema: ResourceSchema{
				Entity:      "LOCATION_ID",
				Value:       "WIND_SPEED",
				TS:          "TIMESTAMP",
				SourceTable: loc,
			},
		},
	}

	if !useTimestamp {
		data.opts.Schema.TS = ""
		for i := range data.records {
			data.records[i][4] = nil
		}
		// Given we cannot be sure which records will be selected when no TS is used
		// we must assume any of the original records could be selected
		noTsExpected := make([]ResourceRecord, 0)
		for _, r := range data.records {
			noTsExpected = append(noTsExpected, ResourceRecord{
				Entity: r[0].(string),
				Value:  r[1],
				TS:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			})
		}
		data.expected = noTsExpected
		for i := range data.incrementalRecords {
			data.incrementalRecords[i][4] = nil
		}
		noTsIncrementalExpected := make([]ResourceRecord, 0)
		for _, r := range data.incrementalRecords {
			noTsIncrementalExpected = append(noTsIncrementalExpected, ResourceRecord{
				Entity: r[0].(string),
				Value:  r[1],
				TS:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			})
		}
		data.incrementalExpected = noTsIncrementalExpected
	}

	return data
}

type testSQLMaterializationData struct {
	schema              TableSchema
	records             []GenericRecord
	expected            []ResourceRecord
	incrementalRecords  []GenericRecord
	incrementalExpected []ResourceRecord
	location            pl.Location
	opts                MaterializationOptions
	id                  ResourceID
}

func (d testSQLMaterializationData) Assert(t *testing.T, mat Materialization, isIncremental bool) {
	// We're not concerned with order of records in the materialization table,
	// so to verify the table contents, we'll create a map of the records by
	// entity ID and then iterate over the materialization table to verify
	expectedMap := make(map[string][]ResourceRecord)
	expected := d.expected
	if isIncremental {
		expected = d.incrementalExpected
	}
	for _, exp := range expected {
		if _, ok := expectedMap[exp.Entity]; ok {
			expectedMap[exp.Entity] = append(expectedMap[exp.Entity], exp)
		} else {
			expectedMap[exp.Entity] = []ResourceRecord{exp}
		}
	}
	numRows, err := mat.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, len(expectedMap), int(numRows), "expected same number of rows")

	itr, err := mat.IterateSegment(0, 100)
	if err != nil {
		t.Fatalf("could not get iterator: %v", err)
	}

	i := 0
	for itr.Next() {
		matRec := itr.Value()
		recs, hasRecord := expectedMap[matRec.Entity]
		if !hasRecord {
			t.Fatalf("expected with entity ID %s record to exist", matRec.Entity)
		}
		// For materializations without timestamps, we cannot guarantee that one record or another will be
		// chosen by the offline store's implicit ordering, so we'll need to check all records for a match.
		if len(recs) > 1 {
			foundMatch := false
			for _, rec := range recs {
				if rec.Entity == matRec.Entity &&
					reflect.DeepEqual(matRec.Value, rec.Value) &&
					rec.TS.Equal(matRec.TS) {
					foundMatch = true
					break
				}
			}

			if !foundMatch {
				t.Fatalf("No matching record found for entity %s with value %v and timestamp %v", matRec.Entity, matRec.Value, matRec.TS)
			}
		} else {
			rec := recs[0]
			assert.Equal(t, rec.Entity, matRec.Entity, "expected same entity")
			assert.Equal(t, matRec.Value, rec.Value)
			assert.Equal(t, rec.TS, matRec.TS, "expected same ts")
		}
		i++
	}
	if itr.Err() != nil {
		t.Fatalf("could not iterate over materialization: %v", itr.Err())
	}
}

type trainingSetDatasetType string

const (
	tsDatasetFeaturesLabelTS     trainingSetDatasetType = "features_label_ts"
	tsDatasetFeaturesTSLabelNoTS trainingSetDatasetType = "features_ts_label_no_ts"
	tsDatasetFeaturesNoTSLabelTS trainingSetDatasetType = "features_no_ts_label_ts"
	tsDatasetFeaturesLabelNoTS   trainingSetDatasetType = "features_label_no_ts"
)

func newTestSQLTrainingSetData(storeType pt.Type, storeConfig pc.SerializedConfig, tsDatasetType trainingSetDatasetType) testSQLTrainingSetData {
	switch tsDatasetType {
	case tsDatasetFeaturesLabelTS:
		return getTrainingSetDatasetTS(storeType, storeConfig)
	case tsDatasetFeaturesTSLabelNoTS:
		return getTrainingSetFeaturesTSLabelsNoTS(storeType, storeConfig)
	case tsDatasetFeaturesNoTSLabelTS:
		return getTrainingSetDatasetFeaturesNoTSLabelTS(storeType, storeConfig)
	case tsDatasetFeaturesLabelNoTS:
		return getTrainingSetDatasetNoTS(storeType, storeConfig)
	default:
		panic(fmt.Sprintf("unsupported training set dataset type: %s", tsDatasetType))
	}
}

type expectedTrainingSetRecord struct {
	Features []interface{}
	Label    interface{}
}

type testSQLTrainingSetData struct {
	id                      ResourceID
	schema                  TableSchema
	featureIDs              []ResourceID
	records                 []GenericRecord
	expected                []expectedTrainingSetRecord
	incrementalRecords      []GenericRecord
	incrementalExpected     []expectedTrainingSetRecord
	location                pl.Location
	labelID                 ResourceID
	labelSchema             TableSchema
	labelResourceSchema     ResourceSchema
	labelRecords            []GenericRecord
	incrementalLabelRecords []GenericRecord
	labelLocation           pl.Location
	opts                    []MaterializationOptions
	def                     TrainingSetDef
}

func (data testSQLTrainingSetData) Assert(t *testing.T, ts TrainingSetIterator) {
	expectedFeaturesMap := make(map[string]bool)
	for _, exp := range data.expected {
		hash, err := data.HashStruct(exp.Features)
		if err != nil {
			t.Fatalf("could not hash features: %v", err)
		}
		expectedFeaturesMap[string(hash)] = true
	}
	expectedLabelsMap := make(map[string]bool)
	for _, exp := range data.expected {
		hash, err := data.HashStruct(exp.Label)
		if err != nil {
			t.Fatalf("could not hash label: %v", err)
		}
		expectedLabelsMap[string(hash)] = true
	}
	i := 0
	for ts.Next() {
		features := ts.Features()
		label := ts.Label()
		featuresHash, err := data.HashStruct(features)
		if err != nil {
			t.Fatalf("could not hash features: %v", err)
		}
		labelHash, err := data.HashStruct(label)
		if err != nil {
			t.Fatalf("could not hash label: %v", err)
		}
		if _, ok := expectedFeaturesMap[string(featuresHash)]; !ok {
			t.Fatalf("unexpected features: %v", features)
		}
		if _, ok := expectedLabelsMap[string(labelHash)]; !ok {
			t.Fatalf("unexpected label: %v", label)
		}
		i++
	}
	if ts.Err() != nil {
		t.Fatalf("could not iterate over training set: %v", ts.Err())
	}
}

func (data testSQLTrainingSetData) HashStruct(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	hash := sha256.Sum256(buf.Bytes())
	return hash[:], nil
}

func getTrainingSetDatasetTS(storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_FEATURES_SURF_READINGS_TABLE")
	labelLoc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_LABEL_WAVE_HEIGHT_TABLE")
	idCreator := newIDCreator("test")
	id := idCreator.create(TrainingSet, "wave_height_training_set")
	labelID := idCreator.create(Label, "wave_height")
	featureIDs := []ResourceID{
		idCreator.create(Feature, "swell_direction"),
		idCreator.create(Feature, "wave_power"),
		idCreator.create(Feature, "swell_period"),
		idCreator.create(Feature, "wind_speed"),
	}
	return testSQLTrainingSetData{
		id: id,
		schema: TableSchema{
			Columns: []TableColumn{
				{Name: "LOCATION_ID", ValueType: types.String},
				{Name: "SWELL_DIRECTION", ValueType: types.String},
				{Name: "WAVE_POWER_KJ", ValueType: types.Float64},
				{Name: "SWELL_PERIOD_SEC", ValueType: types.Float64},
				{Name: "WIND_SPEED_KTS", ValueType: types.Float64},
				{Name: "MEASURED_ON", ValueType: types.Timestamp},
			},
		},
		featureIDs: featureIDs,
		records: []GenericRecord{
			{"L0", "SW", 7.0, 5.0, 10.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L0", "SW", 8.0, 7.0, 8.0, time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC)},
			{"L1", "NW", 15.0, 11.0, 13.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L1", "W", 17.0, 12.0, 12.0, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
			{"L2", "NW", 42.0, 18.0, 23.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L2", "NW", 47.0, 17.0, 26.0, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
		},
		expected: []expectedTrainingSetRecord{
			{Features: []interface{}{nil, nil, nil, nil}, Label: 4.0},      // label's ts is 2024-10-29, which doesn't correspond to any features, so they're NULL padded
			{Features: []interface{}{"SW", 7.0, 5.0, 10.0}, Label: 3.9},    // features' ts is 2024-10-30, which is equal to the label ts
			{Features: []interface{}{"SW", 7.0, 5.0, 10.0}, Label: 3.5},    // features' ts is 2024-10-31, which is equal to the label ts
			{Features: []interface{}{"NW", 15.0, 11.0, 13.0}, Label: 6.5},  // features' ts is 2024-10-30, which is equal to the label ts
			{Features: []interface{}{"W", 17.0, 12.0, 12.0}, Label: 7.0},   // features' ts is 2024-10-31, which is equal to the label ts
			{Features: []interface{}{"NW", 42.0, 18.0, 23.0}, Label: 16.0}, // features' ts is 2024-10-30, which is equal to the label ts
			{Features: []interface{}{"NW", 47.0, 17.0, 26.0}, Label: 18.0}, // features' ts is 2024-10-31, which is less than the label ts
		},
		location: loc,
		labelID:  labelID,
		labelSchema: TableSchema{
			Columns: []TableColumn{
				{Name: "LOCATION_ID", ValueType: types.String},
				{Name: "LOCATION_NAME", ValueType: types.String},
				{Name: "WAVE_HEIGHT_FT", ValueType: types.Float64},
				{Name: "OBSERVED_ON", ValueType: types.Timestamp},
			},
		},
		labelResourceSchema: ResourceSchema{Entity: "LOCATION_ID", Value: "WAVE_HEIGHT_FT", TS: "OBSERVED_ON", SourceTable: labelLoc},
		labelRecords: []GenericRecord{
			{"L0", "Linda Mar Beach", 4.0, time.Date(2024, 10, 29, 0, 0, 0, 0, time.UTC)},
			{"L0", "Linda Mar Beach", 3.9, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L0", "Linda Mar Beach", 3.5, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
			{"L1", "Ocean Beach", 6.5, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L1", "Ocean Beach", 7.0, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
			{"L2", "Mavericks", 16.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L2", "Mavericks", 18.0, time.Date(2024, 11, 01, 0, 0, 0, 0, time.UTC)},
		},
		labelLocation: labelLoc,
		opts: []MaterializationOptions{
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "SWELL_DIRECTION", TS: "MEASURED_ON", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "WAVE_POWER_KJ", TS: "MEASURED_ON", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "SWELL_PERIOD_SEC", TS: "MEASURED_ON", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "WIND_SPEED_KTS", TS: "MEASURED_ON", SourceTable: loc}},
		},
		def: TrainingSetDef{
			ID:    id,
			Label: labelID,
			LabelSourceMapping: SourceMapping{
				ProviderType:        storeType,
				ProviderConfig:      storeConfig,
				TimestampColumnName: "OBSERVED_ON",
				Location:            nil,
				Columns: metadata.ResourceVariantColumns{
					Entity: "ENTITY",
					Value:  "VALUE",
					TS:     "TS",
				},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "SWELL_DIRECTION", TS: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WAVE_POWER_KJ", TS: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "SWELL_PERIOD_SEC", TS: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WIND_SPEED_KTS", TS: "MEASURED_ON"}},
			},
		},
	}
}

func getTrainingSetFeaturesTSLabelsNoTS(storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_FEATURES_SURF_READINGS_TABLE")
	labelLoc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_LABEL_LOC_LEVEL_TABLE")
	idCreator := newIDCreator("test")
	id := idCreator.create(TrainingSet, "location_level_training_set")
	labelID := idCreator.create(Label, "location_level")
	featureIDs := []ResourceID{
		idCreator.create(Feature, "wave_height_ft"),
		idCreator.create(Feature, "wave_power_kj"),
		idCreator.create(Feature, "swell_period_sec"),
		idCreator.create(Feature, "wind_speed_kts"),
	}

	return testSQLTrainingSetData{
		id: id,
		schema: TableSchema{
			Columns: []TableColumn{
				{Name: "LOCATION_ID", ValueType: types.String},
				{Name: "WAVE_HEIGHT_FT", ValueType: types.Float64},
				{Name: "WAVE_POWER_KJ", ValueType: types.Float64},
				{Name: "SWELL_PERIOD_SEC", ValueType: types.Float64},
				{Name: "WIND_SPEED_KTS", ValueType: types.Float64},
				{Name: "MEASURED_ON", ValueType: types.Timestamp},
			},
		},
		featureIDs: featureIDs,
		records: []GenericRecord{
			{"L0", 3.0, 7.0, 5.0, 10.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L0", 4.0, 8.0, 7.0, 8.0, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
			{"L1", 7.0, 15.0, 11.0, 13.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L1", 8.0, 17.0, 12.0, 12.0, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
			{"L2", 14.0, 42.0, 18.0, 23.0, time.Date(2024, 10, 30, 0, 0, 0, 0, time.UTC)},
			{"L2", 17.0, 47.0, 17.0, 26.0, time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)},
		},
		expected: []expectedTrainingSetRecord{
			{Features: []interface{}{4.0, 8.0, 7.0, 8.0}, Label: "Beginner"},
			{Features: []interface{}{8.0, 17.0, 12.0, 12.0}, Label: "Intermediate"},
			{Features: []interface{}{17.0, 47.0, 17.0, 26.0}, Label: "Advanced"},
		},
		location: loc,
		labelID:  labelID,
		labelSchema: TableSchema{
			Columns: []TableColumn{
				{Name: "LOCATION_ID", ValueType: types.String},
				{Name: "LOCATION_NAME", ValueType: types.String},
				{Name: "LEVEL", ValueType: types.String},
			},
		},
		labelResourceSchema: ResourceSchema{Entity: "LOCATION_ID", Value: "LEVEL", SourceTable: labelLoc},
		labelRecords: []GenericRecord{
			{"L0", "Linda Mar Beach", "Beginner"},
			{"L1", "Ocean Beach", "Intermediate"},
			{"L2", "Mavericks", "Advanced"},
		},
		labelLocation: labelLoc,
		opts: []MaterializationOptions{
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "WAVE_HEIGHT_FT", TS: "MEASURED_ON", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "WAVE_POWER_KJ", TS: "MEASURED_ON", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "SWELL_PERIOD_SEC", TS: "MEASURED_ON", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "LOCATION_ID", Value: "WIND_SPEED_KTS", TS: "MEASURED_ON", SourceTable: loc}},
		},
		def: TrainingSetDef{
			ID:    id,
			Label: labelID,
			LabelSourceMapping: SourceMapping{
				ProviderType:   storeType,
				ProviderConfig: storeConfig,
				Location:       nil,
				Columns:        metadata.ResourceVariantColumns{Entity: "ENTITY", Value: "VALUE"},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WAVE_HEIGHT_FT", TS: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WAVE_POWER_KJ", TS: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "SWELL_PERIOD_SEC", TS: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WIND_SPEED_KTS", TS: "MEASURED_ON"}},
			},
		},
	}
}

func getTrainingSetDatasetFeaturesNoTSLabelTS(storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_FEATURES_SURFERS_TABLE")
	labelLoc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_LABEL_RIDES_TABLE")
	idCreator := newIDCreator("test")
	id := idCreator.create(TrainingSet, "successful_rides_training_set")
	labelID := idCreator.create(Label, "successful_rides")
	featureIDs := []ResourceID{
		idCreator.create(Feature, "experience_level"),
		idCreator.create(Feature, "preferred_board_type"),
		idCreator.create(Feature, "weight_kg"),
		idCreator.create(Feature, "height_cm"),
	}

	return testSQLTrainingSetData{
		id: id,
		schema: TableSchema{
			Columns: []TableColumn{
				{Name: "SURFER_ID", ValueType: types.String},
				{Name: "EXPERIENCE_LEVEL", ValueType: types.String},
				{Name: "PREFERRED_BOARD_TYPE", ValueType: types.String},
				{Name: "WEIGHT_KG", ValueType: types.Float64},
				{Name: "HEIGHT_CM", ValueType: types.Float64},
			},
		},
		featureIDs: featureIDs,
		records: []GenericRecord{
			{"101", "Beginner", "Longboard", 75.0, 180.0},
			{"102", "Intermediate", "Shortboard", 68.0, 175.0},
			{"103", "Advanced", "Fish", 82.0, 185.0},
		},
		expected: []expectedTrainingSetRecord{
			{Features: []interface{}{"Beginner", "Longboard", 75.0, 180.0}, Label: 5},
			{Features: []interface{}{"Beginner", "Longboard", 75.0, 180.0}, Label: 6},
			{Features: []interface{}{"Intermediate", "Shortboard", 68.0, 175.0}, Label: 7},
			{Features: []interface{}{"Intermediate", "Shortboard", 68.0, 175.0}, Label: 8},
			{Features: []interface{}{"Advanced", "Fish", 82.0, 185.0}, Label: 10},
			{Features: []interface{}{"Advanced", "Fish", 82.0, 185.0}, Label: 11},
		},
		location: loc,
		labelID:  labelID,
		labelSchema: TableSchema{
			Columns: []TableColumn{
				{Name: "SESSION_ID", ValueType: types.String},
				{Name: "SURFER_ID", ValueType: types.String},
				{Name: "SESSION_DATE", ValueType: types.Timestamp},
				{Name: "WAVE_HEIGHT_M", ValueType: types.Float64},
				{Name: "SUCCESSFUL_RIDES", ValueType: types.Int64},
			},
		},
		labelResourceSchema: ResourceSchema{
			Entity:      "SURFER_ID",
			Value:       "SUCCESSFUL_RIDES",
			TS:          "SESSION_DATE",
			SourceTable: labelLoc,
		},
		labelRecords: []GenericRecord{
			{"1001", "101", time.Date(2024, 10, 15, 0, 0, 0, 0, time.UTC), 1.2, 5},
			{"1002", "102", time.Date(2024, 10, 16, 0, 0, 0, 0, time.UTC), 2.5, 7},
			{"1003", "103", time.Date(2024, 10, 17, 0, 0, 0, 0, time.UTC), 3.8, 10},
			{"1004", "101", time.Date(2024, 10, 16, 0, 0, 0, 0, time.UTC), 1.6, 6},
			{"1005", "102", time.Date(2024, 10, 18, 0, 0, 0, 0, time.UTC), 2.9, 8},
			{"1006", "103", time.Date(2024, 10, 20, 0, 0, 0, 0, time.UTC), 3.7, 11},
		},
		labelLocation: labelLoc,
		opts: []MaterializationOptions{
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "EXPERIENCE_LEVEL", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "PREFERRED_BOARD_TYPE", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "WEIGHT_KG", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "HEIGHT_CM", SourceTable: loc}},
		},
		def: TrainingSetDef{
			ID:    id,
			Label: labelID,
			LabelSourceMapping: SourceMapping{
				ProviderType:        storeType,
				ProviderConfig:      storeConfig,
				TimestampColumnName: "SESSION_DATE",
				Location:            labelLoc,
				Columns:             metadata.ResourceVariantColumns{Entity: "ENTITY", Value: "VALUE", TS: "TS"},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "EXPERIENCE_LEVEL"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "PREFERRED_BOARD_TYPE"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "WEIGHT_KG"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "HEIGHT_CM"}},
			},
		},
	}
}

func getTrainingSetDatasetNoTS(storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_FEATURES_FAV_SPOT_TABLE")
	labelLoc := pl.NewSQLLocationWithDBSchemaTable(db, schema, "TEST_LABEL_SURFER_LEVEL_TABLE")
	idCreator := newIDCreator("test")
	id := idCreator.create(TrainingSet, "surfer_level_training_set")
	labelID := idCreator.create(Label, "surfer_level")
	featureIDs := []ResourceID{
		idCreator.create(Feature, "avg_wave_height_m"),
		idCreator.create(Feature, "avg_success_rate"),
		idCreator.create(Feature, "fav_spot"),
		idCreator.create(Feature, "most_used_board_type"),
	}

	return testSQLTrainingSetData{
		id: id,
		schema: TableSchema{
			Columns: []TableColumn{
				{Name: "SURFER_ID", ValueType: types.String},
				{Name: "AVG_WAVE_HEIGHT_M", ValueType: types.Float64},
				{Name: "AVG_SUCCESS_RATE", ValueType: types.Float64},
				{Name: "FAV_SPOT", ValueType: types.String},
				{Name: "MOST_USED_BOARD_TYPE", ValueType: types.String},
			},
		},
		featureIDs: featureIDs,
		records: []GenericRecord{
			{"101", 1.5, 70.0, "Linda Mar Beach", "Longboard"},
			{"102", 2.3, 85.0, "Ocean Beach", "Shortboard"},
			{"103", 1.8, 75.0, "Pleasure Point", "Fish"},
		},
		expected: []expectedTrainingSetRecord{
			{Features: []interface{}{1.5, 70.0, "Linda Mar Beach", "Longboard"}, Label: "Intermediate"},
			{Features: []interface{}{2.3, 85.0, "Ocean Beach", "Shortboard"}, Label: "Advanced"},
			{Features: []interface{}{1.8, 75.0, "Pleasure Point", "Fish"}, Label: "Intermediate"},
		},
		location: loc,
		labelID:  labelID,
		labelSchema: TableSchema{
			Columns: []TableColumn{
				{Name: "SURFER_ID", ValueType: types.String},
				{Name: "SKILL_LEVEL", ValueType: types.String},
				{Name: "COMPETITION_READY", ValueType: types.String},
				{Name: "RISK_TOLERANCE", ValueType: types.String},
			},
		},
		labelResourceSchema: ResourceSchema{
			Entity:      "SURFER_ID",
			Value:       "SKILL_LEVEL",
			SourceTable: labelLoc,
		},
		labelRecords: []GenericRecord{
			{"101", "Intermediate", "No", "Medium"},
			{"102", "Advanced", "Yes", "High"},
			{"103", "Intermediate", "Yes", "Medium"},
		},
		labelLocation: labelLoc,
		opts: []MaterializationOptions{
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "AVG_WAVE_HEIGHT_M", TS: "", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "AVG_SUCCESS_RATE", TS: "", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "FAV_SPOT", TS: "", SourceTable: loc}},
			{Schema: ResourceSchema{Entity: "SURFER_ID", Value: "MOST_USED_BOARD_TYPE", TS: "", SourceTable: loc}},
		},
		def: TrainingSetDef{
			ID:    id,
			Label: labelID,
			LabelSourceMapping: SourceMapping{
				ProviderType:   storeType,
				ProviderConfig: storeConfig,
				Location:       labelLoc,
				Columns:        metadata.ResourceVariantColumns{Entity: "ENTITY", Value: "VALUE"},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "AVG_WAVE_HEIGHT_M"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "AVG_SUCCESS_RATE"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "FAV_SPOT"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "MOST_USED_BOARD_TYPE"}},
			},
		},
	}
}
