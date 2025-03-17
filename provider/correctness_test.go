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

	"github.com/featureform/fferr"
	ps "github.com/featureform/provider/provider_schema"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

func TestTransformations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester              offlineSqlTest
		transformationQuery string
	}{
		{
			getConfiguredBigQueryTester(t),
			"SELECT location_id, AVG(wind_speed) as avg_daily_wind_speed, AVG(wind_duration) as avg_daily_wind_duration, AVG(fetch_value) as avg_daily_fetch, TIMESTAMP(timestamp) as date FROM %s GROUP BY location_id, TIMESTAMP(timestamp)",
		},
		{
			getConfiguredSnowflakeTester(t),
			"SELECT location_id, AVG(wind_speed) as avg_daily_wind_speed, AVG(wind_duration) as avg_daily_wind_duration, AVG(fetch_value) as avg_daily_fetch, DATE(timestamp) as date FROM %s GROUP BY location_id, DATE(timestamp)",
		},
		{
			getConfiguredPostgresTester(t),
			"SELECT LOCATION_ID, AVG(WIND_SPEED) as AVG_DAILY_WIND_SPEED, AVG(WIND_DURATION) as AVG_DAILY_WIND_DURATION, AVG(FETCH_VALUE) as AVG_DAILY_FETCH, DATE(TIMESTAMP) as DATE FROM %s GROUP BY LOCATION_ID, DATE(TIMESTAMP)",
		},
		{
			getConfiguredClickHouseTester(t),
			"SELECT LOCATION_ID, AVG(WIND_SPEED) as AVG_DAILY_WIND_SPEED, AVG(WIND_DURATION) as AVG_DAILY_WIND_DURATION, AVG(FETCH_VALUE) as AVG_DAILY_FETCH, DATE(TIMESTAMP) as DATE FROM %s GROUP BY LOCATION_ID, DATE(TIMESTAMP)",
		},
	}

	testSuite := map[string]func(t *testing.T, storeTester offlineSqlTest, transformationQuery string){
		"RegisterTransformationOnPrimaryDatasetTest": RegisterTransformationOnPrimaryDatasetTest,
		"RegisterChainedTransformationsTest":         RegisterChainedTransformationsTest,
	}

	for _, infra := range testInfra {
		for testName, testCase := range testSuite {
			providerName := infra.tester.storeTester.Type()
			name := fmt.Sprintf("%s:%s", providerName, testName)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testCase(t, infra.tester, infra.transformationQuery)
			})
		}
	}
}

func TestMaterializations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		{getConfiguredBigQueryTester(t)},
		{getConfiguredSnowflakeTester(t)},
		{getConfiguredPostgresTester(t)},
		{getConfiguredClickHouseTester(t)},
	}

	testSuite := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterMaterializationNoTimestampTest": RegisterMaterializationNoTimestampTest,
		"RegisterMaterializationTimestampTest":   RegisterMaterializationTimestampTest,
	}

	for _, infra := range testInfra {
		for testName, testCase := range testSuite {
			providerName := infra.tester.storeTester.Type()
			name := fmt.Sprintf("%s:%s", providerName, testName)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testCase(t, infra.tester)
			})
		}
	}
}

func TestTrainingSets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		{
			getConfiguredBigQueryTester(t),
		},
		{
			getConfiguredSnowflakeTester(t),
		},
		{
			getConfiguredPostgresTester(t),
		},
		{
			getConfiguredClickHouseTester(t),
		},
	}

	testSuite := []trainingSetDatasetType{
		tsDatasetFeaturesLabelTS,
		tsDatasetFeaturesTSLabelNoTS,
		tsDatasetFeaturesNoTSLabelTS,
		tsDatasetFeaturesLabelNoTS,
	}

	for _, infra := range testInfra {
		for _, testCase := range testSuite {
			providerName := infra.tester.storeTester.Type()
			name := fmt.Sprintf("%s:%s", providerName, string(testCase))
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				RegisterTrainingSet(t, infra.tester, testCase)
			})
		}
	}
}

func TestResourceTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		// TODO: Fix and enable
		//{getConfiguredBigQueryTester(t, false)},
		{getConfiguredSnowflakeTester(t)},
		// TODO: Fix and enable
		//{getConfiguredPostgresTester(t, false)},
		{getConfiguredClickHouseTester(t)},
	}

	tsDatasetTypes := []trainingSetDatasetType{
		tsDatasetFeaturesLabelTS,
		tsDatasetFeaturesLabelNoTS,
	}

	for _, infra := range testInfra {
		for _, testCase := range tsDatasetTypes {
			testName := string(testCase)
			providerName := infra.tester.storeTester.Type()
			name := fmt.Sprintf("%s:%s", providerName, testName)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				RegisterValidFeatureAndLabel(t, infra.tester, testCase)
				RegisterInValidFeatureAndLabel(t, infra.tester, testCase)
			})
		}
	}
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		// TODO: Fix and enable
		//{getConfiguredBigQueryTester(t, false)},
		{getConfiguredSnowflakeTester(t)},
		// TODO: Fix and enable
		//{getConfiguredPostgresTester(t, false)},
		{getConfiguredClickHouseTester(t)},
	}

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"DeleteTableTest":            DeleteTableTest,
		"DeleteNotExistingTableTest": DeleteNotExistingTableTest,
	}

	for _, infra := range testInfra {
		for testName, testCase := range testCases {
			providerName := infra.tester.storeTester.Type()
			name := fmt.Sprintf("%s:%s", providerName, testName)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testCase(t, infra.tester)
			})
		}
	}
}

func TestSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		{
			getConfiguredSnowflakeTester(t),
		},
	}

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTableInDifferentDatabaseTest":           RegisterTableInDifferentDatabaseTest,
		"RegisterTableInSameDatabaseDifferentSchemaTest": RegisterTableInSameDatabaseDifferentSchemaTest,
		"RegisterTwoTablesInSameSchemaTest":              RegisterTwoTablesInSameSchemaTest,
	}

	for _, infra := range testInfra {
		providerName := infra.tester.storeTester.Type()
		for testName, testCase := range testCases {
			name := fmt.Sprintf("%s:%s", providerName, testName)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testCase(t, infra.tester)
			})
		}
	}
}

func TestCrossDatabaseJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		{
			getConfiguredSnowflakeTester(t),
		},
	}

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"CrossDatabaseJoinTest": CrossDatabaseJoinTest,
	}

	for _, infra := range testInfra {
		providerName := infra.tester.storeTester.Type()
		for testName, testCase := range testCases {
			name := fmt.Sprintf("%s:%s", providerName, testName)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testCase(t, infra.tester)
			})
		}
	}
}

func TestTrainingSetTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	testInfra := []struct {
		tester offlineSqlTest
	}{
		{getConfiguredSnowflakeTester(t)},
	}

	tsTypes := map[metadata.TrainingSetType]trainingSetDatasetType{
		metadata.DynamicTrainingSet: tsDatasetFeaturesLabelTS,
		metadata.StaticTrainingSet:  tsDatasetFeaturesTSLabelNoTS,
		metadata.ViewTrainingSet:    tsDatasetFeaturesNoTSLabelTS,
	}

	for _, infra := range testInfra {
		for tsType, dataSetType := range tsTypes {
			providerName := infra.tester.storeTester.Type()
			name := fmt.Sprintf("%s:%s", providerName, tsType)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				RegisterTrainingSetWithType(t, infra.tester, dataSetType, tsType)
			})
		}
	}
}

func newSQLTransformationTest(tester offlineSqlTest, transformationQuery string) *sqlTransformationTester {
	data := newTestSQLTransformationData(tester, transformationQuery)
	return &sqlTransformationTester{
		tester:     tester.storeTester,
		testConfig: tester.testConfig,
		data:       data,
	}
}

type sqlTransformationTester struct {
	tester     offlineSqlStoreTester
	testConfig offlineSqlTestConfig
	data       testSQLTransformationData
}

func newSQLMaterializationTest(tester offlineSqlTest, useTimestamps bool) *sqlMaterializationTester {
	data := newTestSQLMaterializationData(tester, useTimestamps)
	return &sqlMaterializationTester{
		tester:     tester.storeTester,
		testConfig: tester.testConfig,
		data:       data,
	}
}

type sqlMaterializationTester struct {
	tester     offlineMaterializationSqlStoreTester
	testConfig offlineSqlTestConfig
	data       testSQLMaterializationData
}

func newSQLTrainingSetTest(test offlineSqlTest, tsDatasetType trainingSetDatasetType) *sqlTrainingSetTester {
	data := newTestSQLTrainingSetData(test, test.storeTester.Type(), test.storeTester.Config(), tsDatasetType)
	return &sqlTrainingSetTester{
		tester:     test.storeTester,
		testConfig: test.testConfig,
		data:       data,
	}
}

type sqlTrainingSetTester struct {
	tester     offlineTrainingSetSqlStoreTester
	testConfig offlineSqlTestConfig
	data       testSQLTrainingSetData
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

	schemaName := sqlLoc.GetSchema()
	if schemaName != "" {
		if err := tester.CreateSchema(dbName, schemaName); err != nil {
			t.Fatalf("could not create schema: %v", err)
		}
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

func newTestSQLTransformationData(test offlineSqlTest, transformationQuery string) testSQLTransformationData {
	db := test.storeTester.GetTestDatabase()
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	sqlLoc := newSqlLocation(test.testConfig, db, schema, "TEST_WIND_DATA_TABLE")
	tableLoc := sqlLoc.TableLocation()
	queryFmt := transformationQuery
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
					Name:      "FETCH_VALUE",
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
		location: sqlLoc,
		config: TransformationConfig{
			Type:          SQLTransformation,
			TargetTableID: idCreator.create(Transformation, ""),
			Query:         fmt.Sprintf(queryFmt, test.testConfig.sanitizeTableName(tableLoc)),
			SourceMapping: []SourceMapping{
				{
					Template:       SanitizeSqlLocation(tableLoc),
					Source:         tableLoc.String(),
					ProviderType:   test.storeTester.Type(),
					ProviderConfig: test.storeTester.Config(),
					Location:       sqlLoc,
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

	var expectedMap = map[string]GenericRecord{}
	for i := 0; i < len(d.expected); i++ {
		expectedMap[d.expected[i][entityIdx].(string)] = d.expected[i]
	}

	i := 0
	for itr.Next() {
		actual := itr.Values()
		expected := expectedMap[actual[entityIdx].(string)]
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

func newTestSQLMaterializationData(test offlineSqlTest, useTimestamp bool) testSQLMaterializationData {
	db := test.storeTester.GetTestDatabase()
	schema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := newSqlLocation(test.testConfig, db, schema, "TEST_WIND_DATA_TABLE")
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
					Name:      "FETCH_VALUE",
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

func newTestSQLTrainingSetData(test offlineSqlTest, storeType pt.Type, storeConfig pc.SerializedConfig, tsDatasetType trainingSetDatasetType) testSQLTrainingSetData {
	switch tsDatasetType {
	case tsDatasetFeaturesLabelTS:
		return getTrainingSetDatasetTS(test, storeType, storeConfig)
	case tsDatasetFeaturesTSLabelNoTS:
		return getTrainingSetFeaturesTSLabelsNoTS(test, storeType, storeConfig)
	case tsDatasetFeaturesNoTSLabelTS:
		return getTrainingSetDatasetFeaturesNoTSLabelTS(test, storeType, storeConfig)
	case tsDatasetFeaturesLabelNoTS:
		return getTrainingSetDatasetNoTS(test, storeType, storeConfig)
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

func getTrainingSetDatasetTS(test offlineSqlTest, storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := test.storeTester.GetTestDatabase()
	locSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := newSqlLocation(test.testConfig, db, locSchema, "TEST_FEATURES_ALL_TIMESTAMPS")
	labelSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	labelLoc := newSqlLocation(test.testConfig, db, labelSchema, "TEST_LABELS_ALL_TIMESTAMPS")
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
		labelResourceSchema: ResourceSchema{SourceTable: labelLoc, EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "WAVE_HEIGHT_FT", TimestampColumn: "OBSERVED_ON"}},
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
				Location:            labelLoc,
				EntityMappings: &metadata.EntityMappings{
					Mappings: []metadata.EntityMapping{
						{
							Name:         "Location",
							EntityColumn: "LOCATION_ID",
						},
					},
					ValueColumn:     "WAVE_HEIGHT_FT",
					TimestampColumn: "OBSERVED_ON",
				},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "SWELL_DIRECTION", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "SWELL_DIRECTION", TimestampColumn: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WAVE_POWER_KJ", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "WAVE_POWER_KJ", TimestampColumn: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "SWELL_PERIOD_SEC", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "SWELL_PERIOD_SEC", TimestampColumn: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WIND_SPEED_KTS", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "WIND_SPEED_KTS", TimestampColumn: "MEASURED_ON"}},
			},
			Type: metadata.DynamicTrainingSet,
		},
	}
}

func getTrainingSetFeaturesTSLabelsNoTS(test offlineSqlTest, storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := test.storeTester.GetTestDatabase()
	locSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := newSqlLocation(test.testConfig, db, locSchema, "TEST_FEATURES_FEATURE_TIMESTAMPS")
	labelSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	labelLoc := newSqlLocation(test.testConfig, db, labelSchema, "TEST_LABELS_FEATURE_TIMESTAMPS")
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
		labelResourceSchema: ResourceSchema{SourceTable: labelLoc, EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "LEVEL"}},
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
				Location:       labelLoc,
				EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "LEVEL"},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WAVE_HEIGHT_FT", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "WAVE_HEIGHT_FT", TimestampColumn: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WAVE_POWER_KJ", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "WAVE_POWER_KJ", TimestampColumn: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "SWELL_PERIOD_SEC", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "SWELL_PERIOD_SEC", TimestampColumn: "MEASURED_ON"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, TimestampColumnName: "MEASURED_ON", Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "LOCATION_ID", Value: "WIND_SPEED_KTS", TS: "MEASURED_ON"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "Location", EntityColumn: "LOCATION_ID"}}, ValueColumn: "WIND_SPEED_KTS", TimestampColumn: "MEASURED_ON"}},
			},
			Type: metadata.DynamicTrainingSet,
		},
	}
}

func getTrainingSetDatasetFeaturesNoTSLabelTS(test offlineSqlTest, storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := test.storeTester.GetTestDatabase()
	locSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := newSqlLocation(test.testConfig, db, locSchema, "TEST_FEATURES_LABEL_TIMESTAMPS")
	labelSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	labelLoc := newSqlLocation(test.testConfig, db, labelSchema, "TEST_LABELS_LABEL_TIMESTAMPS")
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
		labelResourceSchema: ResourceSchema{SourceTable: labelLoc, EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "SUCCESSFUL_RIDES", TimestampColumn: "SESSION_DATE"}},
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
				EntityMappings: &metadata.EntityMappings{
					Mappings: []metadata.EntityMapping{
						{Name: "surfer", EntityColumn: "SURFER_ID"},
					},
					ValueColumn:     "SUCCESSFUL_RIDES",
					TimestampColumn: "SESSION_DATE",
				},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "EXPERIENCE_LEVEL"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "EXPERIENCE_LEVEL"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "PREFERRED_BOARD_TYPE"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "PREFERRED_BOARD_TYPE"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "WEIGHT_KG"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "WEIGHT_KG"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "HEIGHT_CM"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "HEIGHT_CM"}},
			},
			Type: metadata.DynamicTrainingSet,
		},
	}
}

func getTrainingSetDatasetNoTS(test offlineSqlTest, storeType pt.Type, storeConfig pc.SerializedConfig) testSQLTrainingSetData {
	db := test.storeTester.GetTestDatabase()
	locSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	loc := newSqlLocation(test.testConfig, db, locSchema, "TEST_FEATURES_NO_TIMESTAMPS")
	labelSchema := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	labelLoc := newSqlLocation(test.testConfig, db, labelSchema, "TEST_LABELS_NO_TIMESTAMPS")
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
		labelResourceSchema: ResourceSchema{SourceTable: labelLoc, EntityMappings: metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "SKILL_LEVEL"}},
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
				EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "SKILL_LEVEL"},
			},
			Features: featureIDs,
			FeatureSourceMappings: []SourceMapping{
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "AVG_WAVE_HEIGHT_M"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "AVG_WAVE_HEIGHT_M"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "AVG_SUCCESS_RATE"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "AVG_SUCCESS_RATE"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "FAV_SPOT"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "FAV_SPOT"}},
				{ProviderType: storeType, ProviderConfig: storeConfig, Location: loc, Columns: &metadata.ResourceVariantColumns{Entity: "SURFER_ID", Value: "MOST_USED_BOARD_TYPE"}, EntityMappings: &metadata.EntityMappings{Mappings: []metadata.EntityMapping{{Name: "surfer", EntityColumn: "SURFER_ID"}}, ValueColumn: "MOST_USED_BOARD_TYPE"}},
			},
			Type: metadata.DynamicTrainingSet,
		},
	}
}

func RegisterTransformationOnPrimaryDatasetTest(t *testing.T, tester offlineSqlTest, transformationQuery string) {
	test := newSQLTransformationTest(tester, transformationQuery)
	_ = initSqlPrimaryDataset(t, test.tester, test.data.location, test.data.schema, test.data.records)
	if err := test.tester.CreateTransformation(test.data.config); err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
	actual, err := test.tester.GetTransformationTable(test.data.config.TargetTableID)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}
	test.data.Assert(t, actual)
}

func RegisterChainedTransformationsTest(t *testing.T, test offlineSqlTest, transformationQuery string) {
	transformationTest := newSQLTransformationTest(test, transformationQuery)
	_ = initSqlPrimaryDataset(t, transformationTest.tester, transformationTest.data.location, transformationTest.data.schema, transformationTest.data.records)
	if err := transformationTest.tester.CreateTransformation(transformationTest.data.config); err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
	// CHAIN `SELECT *` TRANSFORMATION ON 1ST TRANSFORMATION
	// Create chained transformation resource ID and table name
	id := ResourceID{Name: "DUMMY_TABLE_TF2", Variant: "transformationTest", Type: Transformation}
	table, err := ps.ResourceToTableName(Transformation.String(), id.Name, id.Variant)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}
	// Get the table name of the first transformation and create a SQL location for sanitization
	srcDataset, err := ps.ResourceToTableName(Transformation.String(), transformationTest.data.config.TargetTableID.Name, transformationTest.data.config.TargetTableID.Variant)
	if err != nil {
		t.Fatalf("could not get transformation table name from resource ID: %v", err)
	}
	srcLoc := pl.NewSQLLocation(srcDataset)
	// Copy the original config and modify the query and source mapping
	config := transformationTest.data.config
	config.TargetTableID = id
	config.Query = fmt.Sprintf("SELECT * FROM %s", test.testConfig.sanitizeTableName(srcLoc.TableLocation()))
	config.SourceMapping[0].Location = pl.NewSQLLocation(table)
	// Create, get and assert the chained transformation
	if err := transformationTest.tester.CreateTransformation(config); err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
	actual, err := transformationTest.tester.GetTransformationTable(id)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}
	transformationTest.data.Assert(t, actual)
}

func RegisterTrainingSet(t *testing.T, test offlineSqlTest, tsDatasetType trainingSetDatasetType) {
	tsTest := newSQLTrainingSetTest(test, tsDatasetType)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.location, tsTest.data.schema, tsTest.data.records)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.labelLocation, tsTest.data.labelSchema, tsTest.data.labelRecords)

	if err := tsTest.tester.CreateTrainingSet(tsTest.data.def); err != nil {
		t.Fatalf("could not create training set: %v", err)
	}
	ts, err := tsTest.tester.GetTrainingSet(tsTest.data.id)
	if err != nil {
		t.Fatalf("could not get training set: %v", err)
	}
	tsTest.data.Assert(t, ts)
}

func RegisterMaterializationNoTimestampTest(t *testing.T, tester offlineSqlTest) {
	useTimestamps := false
	isIncremental := false
	matTest := newSQLMaterializationTest(tester, useTimestamps)
	_ = initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}
	mat, err = matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, mat, isIncremental)
}

func RegisterMaterializationTimestampTest(t *testing.T, tester offlineSqlTest) {
	useTimestamps := true
	isIncremental := false
	matTest := newSQLMaterializationTest(tester, useTimestamps)
	_ = initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}
	mat, err = matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, mat, isIncremental)
}

func RegisterValidFeatureAndLabel(t *testing.T, test offlineSqlTest, tsDatasetType trainingSetDatasetType) {
	tsTest := newSQLTrainingSetTest(test, tsDatasetType)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.location, tsTest.data.schema, tsTest.data.records)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.labelLocation, tsTest.data.labelSchema, tsTest.data.labelRecords)

	featureTblSCols := tsTest.data.schema.Columns
	featureResourceSchema := ResourceSchema{
		Entity:      featureTblSCols[0].Name,
		Value:       featureTblSCols[1].Name,
		SourceTable: tsTest.data.location,
	}
	if featureTblSCols[len(featureTblSCols)-1].ValueType == types.Timestamp {
		featureResourceSchema.TS = featureTblSCols[len(featureTblSCols)-1].Name
	}
	if _, err := tsTest.tester.RegisterResourceFromSourceTable(tsTest.data.featureIDs[0], featureResourceSchema); err != nil {
		t.Fatalf("could not register feature table: %v", err)
	}
	if _, err := tsTest.tester.RegisterResourceFromSourceTable(tsTest.data.labelID, tsTest.data.labelResourceSchema); err != nil {
		t.Fatalf("could not register label table: %v", err)
	}
}

func RegisterInValidFeatureAndLabel(t *testing.T, test offlineSqlTest, tsDatasetType trainingSetDatasetType) {
	tsTest := newSQLTrainingSetTest(test, tsDatasetType)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.location, tsTest.data.schema, tsTest.data.records)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.labelLocation, tsTest.data.labelSchema, tsTest.data.labelRecords)

	featureTblSCols := tsTest.data.schema.Columns
	featureResourceSchema := ResourceSchema{
		Entity:      "invalid",
		Value:       featureTblSCols[1].Name,
		SourceTable: tsTest.data.location,
	}
	if featureTblSCols[len(featureTblSCols)-1].ValueType == types.Timestamp {
		featureResourceSchema.TS = featureTblSCols[len(featureTblSCols)-1].Name
	}
	if _, err := tsTest.tester.RegisterResourceFromSourceTable(tsTest.data.featureIDs[0], featureResourceSchema); err == nil {
		t.Fatal("expected error registering feature resource table")
	}
	tsTest.data.labelResourceSchema.EntityMappings.ValueColumn = "invalid"
	if _, err := tsTest.tester.RegisterResourceFromSourceTable(tsTest.data.labelID, tsTest.data.labelResourceSchema); err == nil {
		t.Fatal("expected error registering label resource table")
	}
}

func DeleteTableTest(t *testing.T, test offlineSqlTest) {
	storeTester, ok := test.storeTester.(offlineSqlStoreCreateDb)
	if !ok {
		t.Skip(fmt.Sprintf("%T does not implement offlineSqlStoreCreateDb. Skipping test", test.storeTester))
	}

	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	storeTester, err := storeTester.CreateDatabase(t, dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	t.Cleanup(func() {
		if err := storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	// Create the table
	tableName := "DUMMY_TABLE"
	location := newSqlLocation(test.testConfig, dbName, "PUBLIC", tableName)

	if _, err = createDummyTable(storeTester, location, 3); err != nil {
		t.Fatalf("could not create table: %v", err)
	}
	if err := storeTester.Delete(location); err != nil {
		t.Fatalf("could not delete table: %v", err)
	}
}

func DeleteNotExistingTableTest(t *testing.T, test offlineSqlTest) {
	storeTester, ok := test.storeTester.(offlineSqlStoreCreateDb)
	if !ok {
		t.Skip(fmt.Sprintf("%T does not implement offlineSqlStoreCreateDb. Skipping test", test.storeTester))
	}

	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	storeTester, err := storeTester.CreateDatabase(t, dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	loc := newSqlLocation(test.testConfig, dbName, "PUBLIC", "NOT_EXISTING_TABLE")

	deleteErr := test.storeTester.Delete(loc)
	if deleteErr == nil {
		t.Fatalf("expected error deleting table")
	}
	if _, ok := deleteErr.(*fferr.DatasetNotFoundError); !ok {
		t.Fatalf("expected DatasetNotFoundError")
	}
}

func createDummyTable(storeTester offlineSqlStoreTester, location pl.Location, numRows int) ([]GenericRecord, error) {
	// Create the table
	// create simple Schema
	schema := TableSchema{
		Columns: []TableColumn{
			{
				Name:      "ID",
				ValueType: types.Int,
			},
			{
				Name:      "NAME",
				ValueType: types.String,
			},
		},
	}

	primaryTable, err := storeTester.CreateTable(location, schema)
	if err != nil {
		return nil, err
	}

	genericRecords := make([]GenericRecord, 0)
	randomNum := uuid.NewString()[:5]
	for i := 0; i < numRows; i++ {
		genericRecords = append(genericRecords, []interface{}{i, fmt.Sprintf("Name_%d_%s", i, randomNum)})
	}

	if err := primaryTable.WriteBatch(genericRecords); err != nil {
		return nil, err
	}

	return genericRecords, nil
}

func verifyPrimaryTable(t *testing.T, primary PrimaryTable, records []GenericRecord) {
	t.Helper()
	numRows, err := primary.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	if numRows == 0 {
		t.Fatalf("expected more than 0 rows")
	}

	iterator, err := primary.IterateSegment(100)
	if err != nil {
		t.Fatalf("Could not get generic iterator: %v", err)
	}

	i := 0
	for iterator.Next() {
		for j, v := range iterator.Values() {
			// NOTE: we're handling float64 differently here given the values returned by Snowflake have less precision
			// and therefore are not equal unless we round them; if tests require handling of other types, we can add
			// additional cases here, otherwise the default case will cover all other types
			switch v.(type) {
			case float64:
				assert.True(t, floatsAreClose(v.(float64), records[i][j].(float64), floatTolerance), "expected same values")
			case time.Time:
				assert.Equal(t, records[i][j].(time.Time).Truncate(time.Microsecond), v.(time.Time).Truncate(time.Microsecond), "expected same values")
			default:
				assert.Equal(t, v, records[i][j], "expected same values")
			}
		}
		i++
	}
}

func RegisterTableInDifferentDatabaseTest(t *testing.T, tester offlineSqlTest) {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))

	storeTester, ok := tester.storeTester.(offlineSqlStoreCreateDb)
	if !ok {
		t.Skip(fmt.Sprintf("%T does not implement offlineSqlStoreCreateDb. Skipping test", tester.storeTester))
	}

	storeTester, err := storeTester.CreateDatabase(t, dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		if err := storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	// Create the schema
	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := tester.storeTester.CreateSchema(dbName, schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := pl.NewSQLLocationFromParts(dbName, schemaName, tableName)
	records, err := createDummyTable(tester.storeTester, sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary, records)
}

func RegisterTableInSameDatabaseDifferentSchemaTest(t *testing.T, storeTester offlineSqlTest) {
	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := storeTester.storeTester.CreateSchema("", schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := pl.NewSQLLocationFromParts("", schemaName, tableName)
	records, err := createDummyTable(storeTester.storeTester, sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := storeTester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary, records)
}

func RegisterTwoTablesInSameSchemaTest(t *testing.T, tester offlineSqlTest) {
	schemaName1 := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	schemaName2 := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := tester.storeTester.CreateSchema("", schemaName1); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	if err := tester.storeTester.CreateSchema("", schemaName2); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the first table
	tableName := "DUMMY_TABLE"
	sqlLocation := pl.NewSQLLocationFromParts("", schemaName1, tableName)
	records, err := createDummyTable(tester.storeTester, sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	// Create the second table using the same table name
	sqlLocation2 := pl.NewSQLLocationFromParts("", schemaName2, tableName)
	records2, err := createDummyTable(tester.storeTester, sqlLocation2, 10)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary1, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: "tb1", Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	primary2, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: "tb2", Variant: "test", Type: Primary},
		sqlLocation2,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary1, records)
	verifyPrimaryTable(t, primary2, records2)
}

func CrossDatabaseJoinTest(t *testing.T, test offlineSqlTest) {
	storeTester, ok := test.storeTester.(offlineSqlStoreCreateDb)
	if !ok {
		t.Skip(fmt.Sprintf("%T does not implement offlineSqlStoreCreateDb. Skipping test", test.storeTester))
	}

	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	storeTester1, err := storeTester.CreateDatabase(t, dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	dbName2 := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name2: %s\n", dbName2)
	storeTester2, err := storeTester.CreateDatabase(t, dbName2)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		if err := storeTester1.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
		if err := storeTester2.DropDatabase(dbName2); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	tableName1 := "DUMMY_TABLE"
	sqlLocation := pl.NewSQLLocationFromParts(dbName, "PUBLIC", tableName1)
	records, err := createDummyTable(storeTester1, sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	tableName2 := "DUMMY_TABLE2"
	sqlLocation2 := pl.NewSQLLocationFromParts(dbName2, "PUBLIC", tableName2)
	records2, err := createDummyTable(storeTester2, sqlLocation2, 10)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	// Register the tables
	primary1, primaryErr := storeTester1.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName1, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	primary2, primaryErr := storeTester2.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName2, Variant: "test", Type: Primary},
		sqlLocation2,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary1, records)
	verifyPrimaryTable(t, primary2, records2)

	targetTableId := ResourceID{Name: "DUMMY_TABLE_TF", Variant: "test", Type: Transformation}
	// union the tables using a transformation
	tfConfig := TransformationConfig{
		Type:          SQLTransformation,
		TargetTableID: targetTableId,
		Query:         fmt.Sprintf("SELECT NAME FROM %s UNION SELECT NAME FROM %s", sqlLocation.TableLocation().String(), sqlLocation2.TableLocation().String()),
	}

	err = storeTester1.CreateTransformation(tfConfig)
	if err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}

	// Verify the union table contents
	tfTable, err := storeTester2.GetTransformationTable(targetTableId)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}

	numRows, err := tfTable.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, int64(13), numRows, "expected 13 rows")
}

func newSqlLocation(config offlineSqlTestConfig, db, schema, table string) *pl.SQLLocation {
	if config.removeSchemaFromLocation {
		schema = ""
	}
	return pl.NewSQLLocationFromParts(db, schema, table)
}
