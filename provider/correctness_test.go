// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	pl "github.com/featureform/provider/location"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type dataSetType string

const (
	simpleTransactionsTransformation          dataSetType = "transactions_transformation"
	simpleTransactionsMaterialization         dataSetType = "transactions_materialization"
	transactionsDuplicatesNoTSMaterialization dataSetType = "transactions_duplicates_no_ts_materialization"
	transactionsDuplicatesTSMaterialization   dataSetType = "transactions_duplicates_ts_materialization"
	// Given GenericRecord, which is an alias for []interface{}, is used for all datasets
	// prior to features, labels and training sets, we cannot know which columns should be
	// used for the entity, value, timestamp, and label fields. To make registration of
	// features, labels, and training sets easier, we'll use the convention of creating
	// datasets that have these fields in the first four columns, with additional columns
	// following these fields.
	entityIdx int = 0
	valueIdx  int = 1
	tsIdx     int = 2
	labelIdx  int = 3
)

func newMaterializationSQLTest(tester offlineMaterializationSqlStoreTester, dataSetType dataSetType, numRecords int) *materializationSQLTest {
	datasetCreator := newDatasetCreator(dataSetType)
	idCreator := newIDCreator("test")
	dataset := datasetCreator.create(numRecords)

	return &materializationSQLTest{
		sqlStoreDatasetCoreTest: sqlStoreDatasetCoreTest{
			tester:         tester,
			datasetCreator: datasetCreator,
			dataset:        dataset,
			primary:        nil,
			idCreator:      idCreator,
		},
		featureID: idCreator.create(Feature, ""),
	}
}

// materializationSQLTest is a helper struct that provides methods to
// arrange, act, and assert tests for materialization created by SQL
// offline stores
type materializationSQLTest struct {
	sqlStoreDatasetCoreTest
	featureID ResourceID
}

func (test *materializationSQLTest) createTestMaterialization(t *testing.T, opts *MaterializationOptions) {
	if opts == nil {
		opts = &MaterializationOptions{
			Schema: ResourceSchema{
				Entity:      test.dataset.schema.Columns[entityIdx].Name,
				Value:       test.dataset.schema.Columns[valueIdx].Name,
				TS:          test.dataset.schema.Columns[tsIdx].Name,
				SourceTable: &test.dataset.location,
			},
		}
	}
	matTester, isMatTester := test.tester.(offlineMaterializationSqlStoreTester)
	if !isMatTester {
		t.Fatalf("tester does not implement offlineMaterializationSqlStoreTester")
	}
	_, err := matTester.CreateMaterialization(test.featureID, *opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}
}

func (test *materializationSQLTest) assertTestMaterialization(t *testing.T) {
	id, err := NewMaterializationID(test.featureID)
	if err != nil {
		t.Fatalf("could not create materialization id: %v", err)
	}
	matTester, isMatTester := test.tester.(offlineMaterializationSqlStoreTester)
	if !isMatTester {
		t.Fatalf("tester does not implement offlineMaterializationSqlStoreTester")
	}
	materialization, err := matTester.GetMaterialization(id)
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	// We're not concerned with order of records in the materialization table,
	// so to verify the table contents, we'll create a map of the records by
	// entity ID and then iterate over the materialization table to verify
	expectedMap := make(map[string]ResourceRecord)
	for _, rec := range test.dataset.expected {
		expected, isResourceRecord := rec.(ResourceRecord)
		if !isResourceRecord {
			t.Fatalf("expected record to be of type ResourceRecord")
		}
		expectedMap[expected.Entity] = expected
	}
	numRows, err := materialization.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, len(test.dataset.expected), int(numRows), "expected same number of rows")

	itr, err := materialization.IterateSegment(0, 100)
	if err != nil {
		t.Fatalf("could not get iterator: %v", err)
	}

	i := 0
	for itr.Next() {
		matRec := itr.Value()
		rec, hasRecord := expectedMap[matRec.Entity]
		if !hasRecord {
			t.Fatalf("expected with entity ID %s record to exist", matRec.Entity)
		}

		assert.Equal(t, rec.Entity, matRec.Entity, "expected same entity")
		test.assertValue(t, matRec.Value, rec.Value)
		assert.Equal(t, rec.TS.Truncate(time.Microsecond), matRec.TS.Truncate(time.Microsecond), "expected same ts")
		i++
	}
}

func newDatasetSQLTest(tester offlineSqlStoreTester, dataSetType dataSetType, numRecords int) *datasetSQLTest {
	datasetCreator := newDatasetCreator(dataSetType)
	idCreator := newIDCreator("test")
	dataset := datasetCreator.create(numRecords)

	return &datasetSQLTest{
		sqlStoreDatasetCoreTest: sqlStoreDatasetCoreTest{
			tester:         tester,
			datasetCreator: datasetCreator,
			dataset:        dataset,
			primary:        nil,
			idCreator:      idCreator,
		},
		transformationIDs: []ResourceID{idCreator.create(Transformation, "")},
	}
}

type datasetSQLTest struct {
	sqlStoreDatasetCoreTest
	transformationIDs []ResourceID
}

func (test datasetSQLTest) createTestTransformation(t *testing.T, config *TransformationConfig) {
	if config == nil {
		loc := test.dataset.location.TableLocation()
		store, err := test.tester.AsOfflineStore()
		if err != nil {
			t.Fatalf("could not get offline store: %v", err)
		}
		if len(test.transformationIDs) == 0 {
			t.Fatalf("expected at least one transformation ID")
		}
		config = &TransformationConfig{
			Type:          SQLTransformation,
			TargetTableID: test.transformationIDs[0],
			Query:         fmt.Sprintf(test.dataset.query, loc.String()),
			SourceMapping: []SourceMapping{
				{
					Template:       SanitizeSqlLocation(loc),
					Source:         loc.String(),
					ProviderType:   store.Type(),
					ProviderConfig: store.Config(),
					Location:       &test.dataset.location,
				},
			},
		}
	}
	err := test.tester.CreateTransformation(*config)
	if err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
}

func (test *datasetSQLTest) createChainedTestTransformation(t *testing.T, config *TransformationConfig) {
	if config == nil {
		if len(test.transformationIDs) == 0 {
			t.Fatalf("expected at least one transformation ID")
		}
		table, err := test.tester.GetTransformationTable(test.transformationIDs[0])
		if err != nil {
			t.Fatalf("could not get transformation table: %v", err)
		}
		sqlTable, isSqlTable := table.(*sqlPrimaryTable)
		if !isSqlTable {
			t.Fatalf("expected table to be of type sqlPrimaryTable")
		}
		loc := sqlTable.sqlLocation.TableLocation()
		store, err := test.tester.AsOfflineStore()
		if err != nil {
			t.Fatalf("could not get offline store: %v", err)
		}
		id := test.idCreator.create(Transformation, "DUMMY_TABLE_TF_2")
		test.transformationIDs = append(test.transformationIDs, id)
		config = &TransformationConfig{
			Type:          SQLTransformation,
			TargetTableID: id,
			Query:         fmt.Sprintf("SELECT * FROM %s", SanitizeSqlLocation(loc)),
			SourceMapping: []SourceMapping{
				{
					Template:       SanitizeSqlLocation(loc),
					Source:         loc.String(),
					ProviderType:   store.Type(),
					ProviderConfig: store.Config(),
					Location:       &test.dataset.location,
				},
			},
		}
	}
	err := test.tester.CreateTransformation(*config)
	if err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
}

func (test datasetSQLTest) assertTransformationDataset(t *testing.T, idIdx int) {
	if idIdx >= len(test.transformationIDs) {
		t.Fatalf("expected transformation ID index to be less than the number of transformation IDs")
	}
	dataset, err := test.tester.GetTransformationTable(test.transformationIDs[idIdx])
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}

	// We're not concerned with order of records in the materialization table,
	// so to verify the table contents, we'll create a map of the records by
	// entity ID and then iterate over the materialization table to verify
	expectedMap := make(map[string]GenericRecord)
	for _, rec := range test.dataset.expected {
		expected, isGenericRecord := rec.(GenericRecord)
		if !isGenericRecord {
			t.Fatalf("expected record to be of type GenericRecord")
		}
		expectedMap[expected[entityIdx].(string)] = expected
	}
	numRows, err := dataset.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, len(test.dataset.expected), int(numRows), "expected same number of rows")

	itr, err := dataset.IterateSegment(100)
	if err != nil {
		t.Fatalf("could not get iterator: %v", err)
	}

	i := 0
	for itr.Next() {
		transRec := itr.Values()
		rec, hasRecord := expectedMap[transRec[entityIdx].(string)]
		if !hasRecord {
			t.Fatalf("expected with entity ID %s record to exist", transRec[entityIdx].(string))
		}

		assert.Equal(t, rec[entityIdx].(string), transRec[entityIdx].(string), "expected same entity")
		test.assertValue(t, transRec[valueIdx], rec[valueIdx])
		assert.Equal(t, rec[tsIdx].(time.Time).Truncate(time.Microsecond), transRec[tsIdx].(time.Time).Truncate(time.Microsecond), "expected same ts")
		i++
	}
}

// sqlStoreDatasetCoreTest contains the core functionality for testing
// SQL offline stores. It provides methods to initialize the database
// schema, append records to the dataset, and assert values in the dataset,
// which is useful for testing transformations, materializations, and
// training sets.
type sqlStoreDatasetCoreTest struct {
	tester         offlineSqlStoreDatasetTester
	datasetCreator *datasetCreator
	dataset        *testDataset
	primary        PrimaryTable
	idCreator      *idCreator
}

func (test sqlStoreDatasetCoreTest) initDatabaseSchemaTable(t *testing.T) {
	dbName := test.dataset.location.GetDatabase()
	t.Logf("Creating Database: %s\n", dbName)
	err := test.tester.CreateDatabase(dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Dropping Database: %s\n", dbName)
		if err := test.tester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})
	schemaName := test.dataset.location.GetSchema()
	if err := test.tester.CreateSchema(dbName, schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	table, err := test.tester.CreateTable(&test.dataset.location, test.dataset.schema)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}
	if err := table.WriteBatch(test.dataset.records); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}
}

func (test *sqlStoreDatasetCoreTest) initPrimaryDataset(t *testing.T) {
	primary, err := test.tester.RegisterPrimaryFromSourceTable(
		test.idCreator.create(Primary, test.dataset.location.GetTable()),
		&test.dataset.location,
	)
	if err != nil {
		t.Fatalf("could not register primary: %v", err)
	}
	test.primary = primary
}

func (test *sqlStoreDatasetCoreTest) appendToDataset(t *testing.T, numRecords int) {
	records, expected := test.datasetCreator.createRecords(numRecords)
	test.dataset.expected = append(test.dataset.expected, expected...)
	if err := test.primary.WriteBatch(records); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}
	test.dataset.records = append(test.dataset.records, records...)
}

func (test sqlStoreDatasetCoreTest) assertValue(t *testing.T, actual, expected any) {
	switch expected.(type) {
	case float64:
		assert.Equal(t, test.roundFloat(expected.(float64), floatRoundingDecimals), test.roundFloat(actual.(float64), floatRoundingDecimals), "expected same value")
	default:
		t.Fatalf("expected value type not supported: %T", expected)
	}
}

func (test sqlStoreDatasetCoreTest) roundFloat(f float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(f*pow) / pow
}

// newDatasetCreator creates a new datasetCreator with the given dataSetType.
func newDatasetCreator(dataSetType dataSetType) *datasetCreator {
	return &datasetCreator{
		dataSetType: dataSetType,
	}
}

// datasetCreator is a helper struct that creates datasets
// that use the GenericRecord type (e.g. transformations).
// dataSetType is used to determine the types of records to generate;
// currently, only TRANSACTIONS is supported, which mirrors the data
// used in our quickstart guide.
type datasetCreator struct {
	dataSetType dataSetType
}

type datasetCreatorOptions struct {
	shouldHaveDuplicates bool
	shouldHaveTimestamp  bool
}

type testDataset struct {
	schema   TableSchema
	records  []GenericRecord
	location pl.SQLLocation
	expected []any
	query    string
}

// create creates a TableSchema and a slice of GenericRecords based on the dataSetType provided
// at the time of creator initialization. numRecords is the number of records to generate in the
// original dataset.
func (creator datasetCreator) create(numRecords int) *testDataset {
	records, expected := creator.createRecords(numRecords)
	return &testDataset{
		schema:   creator.createSchema(),
		location: creator.createLocation(),
		records:  records,
		expected: expected,
		query:    creator.createQuery(),
	}
}

func (creator datasetCreator) createRecords(numRecords int) ([]GenericRecord, []any) {
	switch creator.dataSetType {
	case simpleTransactionsTransformation:
		records := creator.createTransactionsRecords(numRecords, datasetCreatorOptions{shouldHaveTimestamp: true})
		expected := creator.createExpectedRecords(records)
		return records, expected
	case simpleTransactionsMaterialization:
		records := creator.createTransactionsRecords(numRecords, datasetCreatorOptions{shouldHaveTimestamp: true})
		expected := creator.createExpectedRecords(records)
		return records, expected
	case transactionsDuplicatesNoTSMaterialization:
		records := creator.createTransactionsRecords(numRecords, datasetCreatorOptions{shouldHaveDuplicates: true})
		expected := creator.createExpectedRecords(records)
		return records, expected
	case transactionsDuplicatesTSMaterialization:
		records := creator.createTransactionsRecords(numRecords, datasetCreatorOptions{shouldHaveDuplicates: true, shouldHaveTimestamp: true})
		expected := creator.createExpectedRecords(records)
		return records, expected
	default:
		return []GenericRecord{}, []any{}
	}
}

func (creator datasetCreator) createExpectedRecords(records []GenericRecord) []any {
	switch creator.dataSetType {
	case simpleTransactionsTransformation:
		expected := make([]any, 0)
		for _, rec := range records {
			expected = append(expected, GenericRecord{
				rec[entityIdx].(string),
				rec[valueIdx].(float64),
				rec[tsIdx].(time.Time),
			})
		}
		return expected
	case simpleTransactionsMaterialization:
		expected := make([]any, 0)
		for _, rec := range records {
			expected = append(expected, ResourceRecord{
				Entity: rec[entityIdx].(string),
				Value:  rec[valueIdx].(float64),
				TS:     rec[tsIdx].(time.Time),
			})
		}
		return expected
	case transactionsDuplicatesNoTSMaterialization:
		expected := make([]any, 0)
		seen := make(map[string]bool)
		for _, rec := range records {
			if _, ok := seen[rec[entityIdx].(string)]; ok {
				continue
			}
			seen[rec[entityIdx].(string)] = true
			expected = append(expected, ResourceRecord{
				Entity: rec[entityIdx].(string),
				Value:  rec[valueIdx].(float64),
			})
		}
		return expected
	case transactionsDuplicatesTSMaterialization:
		expected := make([]any, 0)
		sort.Slice(records, func(i, j int) bool {
			return records[i][tsIdx].(time.Time).After(records[j][tsIdx].(time.Time))
		})
		seen := make(map[string]bool)
		for _, rec := range records {
			if _, ok := seen[rec[entityIdx].(string)]; ok {
				continue
			}
			seen[rec[entityIdx].(string)] = true
			expected = append(expected, ResourceRecord{
				Entity: rec[entityIdx].(string),
				Value:  rec[valueIdx].(float64),
				TS:     rec[tsIdx].(time.Time),
			})
		}
		return expected
	default:
		return []any{}
	}
}

// createSchema creates a TableSchema based on the dataSetType provided at the time of creator initialization.
// Given we'll need a connection between the columns and the entity, value, ts, and label fields, ensure that these
// fields are present in the schema in this order; you can create additional columns as needed after these fields are
// set.
// Currently, only TRANSACTIONS is supported, which mirrors the data used in our quickstart guide.
func (creator datasetCreator) createSchema() TableSchema {
	switch creator.dataSetType {
	case simpleTransactionsMaterialization, transactionsDuplicatesNoTSMaterialization, transactionsDuplicatesTSMaterialization, simpleTransactionsTransformation:
		return creator.creatorTransactionsSchema()
	default:
		return TableSchema{}
	}
}

func (creator datasetCreator) createLocation() pl.SQLLocation {
	switch creator.dataSetType {
	case simpleTransactionsMaterialization, transactionsDuplicatesNoTSMaterialization, transactionsDuplicatesTSMaterialization, simpleTransactionsTransformation:
		dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
		schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
		return *pl.NewSQLLocationWithDBSchemaTable(dbName, schemaName, "TEST_TRANSACTIONS_TABLE").(*pl.SQLLocation)
	default:
		return *pl.NewSQLLocation("TEST_TABLE").(*pl.SQLLocation)
	}
}

func (creator datasetCreator) creatorTransactionsSchema() TableSchema {
	return TableSchema{
		Columns: []TableColumn{
			{
				Name:      "CUSTOMER_ID",
				ValueType: types.String,
			},
			{
				Name:      "TRANSACTION_AMOUNT",
				ValueType: types.Float64,
			},
			{
				Name:      "TIMESTAMP",
				ValueType: types.Timestamp,
			},
			{
				Name:      "IS_FRAUD",
				ValueType: types.Bool,
			},
		},
	}
}

func (creator datasetCreator) createTransactionsRecords(numRecords int, opts datasetCreatorOptions) []GenericRecord {
	genericRecords := make([]GenericRecord, 0)
	for i := 0; i < numRecords; i++ {
		id := fmt.Sprintf("C%d%s", i, uuid.NewString()[:5])
		recs := []GenericRecord{{
			// CUSTOMER_ID
			id,
			// TRANSACTION_AMOUNT
			rand.Float64() * 1000,
			// TIMESTAMP
			nil,
			// IS_FRAUD
			i%2 == 0,
		}}

		if opts.shouldHaveDuplicates && i+1 < numRecords {
			recs = append(recs, GenericRecord{
				// CUSTOMER_ID
				id,
				// TRANSACTION_AMOUNT
				rand.Float64() * 1000,
				// TIMESTAMP
				nil,
				// IS_FRAUD
				i%2 == 0,
			})
			i++
		}

		if opts.shouldHaveTimestamp {
			for r := range recs {
				recs[r][tsIdx] = time.Now().UTC().Add(time.Duration(i) * time.Second)
			}
		}

		genericRecords = append(genericRecords, recs...)
	}
	return genericRecords
}

func (creator datasetCreator) createQuery() string {
	switch creator.dataSetType {
	case simpleTransactionsTransformation:
		return "SELECT customer_id AS user_id, avg(transaction_amount) AS avg_transaction_amt, CAST(timestamp AS TIMESTAMP_NTZ(6)) AS ts FROM %s GROUP BY user_id, ts ORDER BY ts"
	default:
		return ""
	}
}

// TODO: put on base correctness tester struct
func roundFloat(f float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(f*pow) / pow
}

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
	default:
		return ResourceID{}
	}
}
