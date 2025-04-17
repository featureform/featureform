// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package runner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/google/uuid"

	fs "github.com/featureform/filestore"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

const mockChunkSize = 2

type MaterializedFeaturesNumChunksBroken struct {
	id provider.MaterializationID
}

func (m *MaterializedFeaturesNumChunksBroken) ID() provider.MaterializationID {
	return m.id
}

func (m *MaterializedFeaturesNumChunksBroken) NumRows() (int64, error) {
	return 0, fmt.Errorf("cannot fetch number of rows")
}

func (m *MaterializedFeaturesNumChunksBroken) IterateSegment(begin int64, end int64) (provider.FeatureIterator, error) {
	return nil, nil
}

func (m *MaterializedFeaturesNumChunksBroken) NumChunks() (int, error) {
	return 0, fmt.Errorf("cannot fetch number of chunks")
}

func (m *MaterializedFeaturesNumChunksBroken) IterateChunk(idx int) (provider.FeatureIterator, error) {
	return nil, nil
}

func (m *MaterializedFeaturesNumChunksBroken) Close() error {
	return nil
}

func (m MaterializedFeaturesNumChunksBroken) Location() pl.Location {
	return nil
}

type MaterializedFeaturesIterateBroken struct {
	id provider.MaterializationID
}

func (m *MaterializedFeaturesIterateBroken) ID() provider.MaterializationID {
	return m.id
}

func (m *MaterializedFeaturesIterateBroken) NumRows() (int64, error) {
	return 1, nil
}

func (m *MaterializedFeaturesIterateBroken) IterateSegment(begin int64, end int64) (provider.FeatureIterator, error) {
	return nil, errors.New("cannot create feature iterator")
}

func (m *MaterializedFeaturesIterateBroken) NumChunks() (int, error) {
	return 1, nil
}

func (m *MaterializedFeaturesIterateBroken) IterateChunk(idx int) (provider.FeatureIterator, error) {
	return nil, errors.New("cannot create feature iterator")
}

func (m *MaterializedFeaturesIterateBroken) Location() pl.Location {
	return nil
}

type MaterializedFeaturesIterateRunBroken struct {
	id provider.MaterializationID
}

func (m *MaterializedFeaturesIterateRunBroken) ID() provider.MaterializationID {
	return m.id
}

func (m *MaterializedFeaturesIterateRunBroken) NumRows() (int64, error) {
	return 1, nil
}

func (m *MaterializedFeaturesIterateRunBroken) IterateSegment(begin int64, end int64) (provider.FeatureIterator, error) {
	return &BrokenFeatureIterator{}, nil
}

func (m *MaterializedFeaturesIterateRunBroken) NumChunks() (int, error) {
	return 1, nil
}
func (m *MaterializedFeaturesIterateRunBroken) IterateChunk(idx int) (provider.FeatureIterator, error) {
	return &BrokenFeatureIterator{}, nil
}

func (m *MaterializedFeaturesIterateRunBroken) Location() pl.Location {
	return nil
}

type MockOnlineTable struct {
	DataTable sync.Map
}

func (m *MockOnlineTable) Set(entity string, value interface{}) error {
	m.DataTable.Store(entity, value)
	return nil
}

func (m *MockOnlineTable) Get(entity string) (interface{}, error) {
	value, exists := m.DataTable.Load(entity)
	if !exists {
		return nil, errors.New("Value does not exist in online table")
	}
	return value, nil
}

type mockOnlineTableBatch struct {
	DataTable sync.Map
}

func (m *mockOnlineTableBatch) BatchSet(ctx context.Context, items []provider.SetItem) error {
	for _, item := range items {
		m.DataTable.Store(item.Entity, item.Value)
	}
	return nil
}

func (m *mockOnlineTableBatch) MaxBatchSize() (int, error) {
	return 3, nil
}

func (m *mockOnlineTableBatch) Set(entity string, value interface{}) error {
	return errors.New("mockOnlineTableBatch Not using batch set")
}

func (m *mockOnlineTableBatch) Get(entity string) (interface{}, error) {
	value, exists := m.DataTable.Load(entity)
	if !exists {
		return nil, errors.New("Value does not exist in online table")
	}
	return value, nil
}

type BrokenOnlineTable struct {
}

func (m *BrokenOnlineTable) Set(entity string, value interface{}) error {
	return errors.New("cannot set feature value")
}

func (m *BrokenOnlineTable) Get(entity string) (interface{}, error) {
	return nil, errors.New("cannot get feature value")
}

type MockFeatureIterator struct {
	CurrentIndex int
	Slice        []provider.ResourceRecord
}

func (m *MockFeatureIterator) Next() bool {
	m.CurrentIndex++
	return m.CurrentIndex < len(m.Slice)

}

func (m *MockFeatureIterator) Err() error {
	return nil
}

func (m *MockFeatureIterator) Close() error {
	return nil
}

func (m *MockFeatureIterator) Value() provider.ResourceRecord {
	return m.Slice[m.CurrentIndex]
}

type BrokenFeatureIterator struct{}

func (m *BrokenFeatureIterator) Next() bool {
	return false
}

func (m *BrokenFeatureIterator) Err() error {
	return errors.New("error iterating over features")
}

func (m *BrokenFeatureIterator) Close() error {
	return nil
}

func (m *BrokenFeatureIterator) Value() provider.ResourceRecord {
	return provider.ResourceRecord{}
}

type TestError struct {
	Outcome string
	Err     error
}

func (m *TestError) Error() string {
	return fmt.Sprintf("%v: %s", m.Err, m.Outcome)
}

type JobTestParams struct {
	TestName     string
	Materialized provider.MemoryMaterialization
	ChunkIdx     int
}

type ErrorJobTestParams struct {
	ErrorName    string
	Materialized provider.Materialization
	Table        provider.OnlineStoreTable
	ChunkIdx     int
}

func testParams(params JobTestParams) error {
	table := &MockOnlineTable{
		DataTable: sync.Map{},
	}
	if err := testParamsOnTable(params, table); err != nil {
		return err
	}
	// At the time of writing, batch writes have a different code path
	batchTable := &mockOnlineTableBatch{
		DataTable: sync.Map{},
	}
	return testParamsOnTable(params, batchTable)
}

// This exists because we have MockOnlineTable and mockBatchOnlineTable.
// Once we unify those two, we will not need this anymore.
func testParamsOnTable(params JobTestParams, table provider.OnlineStoreTable) error {
	online := NewMockOnlineStore()
	featureRows := params.Materialized.Data
	job := &MaterializedChunkRunner{
		Materialized: provider.NewLegacyMaterializationAdapterWithEmptySchema(&params.Materialized),
		Table:        table,
		Store:        online,
		ChunkIdx:     params.ChunkIdx,
	}
	completionWatcher, err := job.Run()
	if err != nil {
		return &TestError{Outcome: "Job failed to start.", Err: err}
	}
	err = completionWatcher.Wait()
	if err != nil {
		return &TestError{Outcome: "Job failed while running.", Err: err}
	}
	complete := completionWatcher.Complete()
	if !complete {
		return &TestError{Outcome: "Job failed to set flag complete.", Err: nil}
	}
	if returnString := completionWatcher.String(); len(returnString) == 0 {
		return fmt.Errorf("string() method returns empty string")
	}
	rowStart := params.ChunkIdx * mockChunkSize
	rowEnd := rowStart + mockChunkSize
	if rowEnd > len(featureRows) {
		rowEnd = len(featureRows)
	}
	for i := rowStart; i < rowEnd; i++ {
		tableValue, err := table.Get(featureRows[i].Entity)
		if err != nil {
			return &TestError{Outcome: fmt.Sprintf("Cannot fetch table value for entity %v", featureRows[i].Value), Err: err}
		}
		if !reflect.DeepEqual(tableValue, featureRows[i].Value) {
			return &TestError{Outcome: fmt.Sprintf("%v becomes %v in table copy", featureRows[i].Value, tableValue), Err: nil}
		}
	}
	return nil
}

func testBreakingParams(params ErrorJobTestParams) error {
	online := NewMockOnlineStore()
	job := &MaterializedChunkRunner{
		Materialized: provider.NewLegacyMaterializationAdapterWithEmptySchema(params.Materialized),
		Table:        params.Table,
		Store:        online,
		ChunkIdx:     params.ChunkIdx,
	}
	completionWatcher, err := job.Run()
	if err != nil {
		return &TestError{Outcome: "Job failed to start.", Err: err}
	}
	if err := completionWatcher.Wait(); err == nil {
		return fmt.Errorf("Failed to catch %s", params.ErrorName)
	}
	if err := completionWatcher.Err(); err == nil {
		return fmt.Errorf("Failed to set error")
	}
	if returnString := completionWatcher.String(); len(returnString) == 0 {
		return fmt.Errorf("string() method returns empty string")
	}
	return nil
}

type CopyTestData struct {
	Rows []interface{}
}

func CreateMockFeatureRows(data []interface{}) provider.MemoryMaterialization {
	featureRows := make([]provider.ResourceRecord, len(data))
	for i, row := range data {
		featureRows[i] = provider.ResourceRecord{Entity: fmt.Sprintf("entity_%d", i), Value: row}
	}
	return provider.MemoryMaterialization{Id: provider.MaterializationID(uuid.NewString()), Data: featureRows, RowsPerChunk: mockChunkSize}
}

func TestErrorCoverage(t *testing.T) {
	minimalMockFeatureRows := CreateMockFeatureRows([]interface{}{1})
	errorJobs := []ErrorJobTestParams{
		{
			ErrorName:    "iterator run error",
			Materialized: &MaterializedFeaturesIterateRunBroken{provider.MaterializationID(uuid.NewString())},
			Table:        &BrokenOnlineTable{},
			ChunkIdx:     0,
		},
		{
			ErrorName:    "table set error",
			Materialized: &minimalMockFeatureRows,
			Table:        &BrokenOnlineTable{},
			ChunkIdx:     0,
		},
		{
			ErrorName:    "create iterator error",
			Materialized: &MaterializedFeaturesIterateBroken{provider.MaterializationID(uuid.NewString())},
			Table:        &BrokenOnlineTable{},
			ChunkIdx:     0,
		},
	}

	for _, param := range errorJobs {
		if err := testBreakingParams(param); err != nil {
			t.Fatalf("Error Test Job Failed: %s, %v\n", param.ErrorName, err)
		}
	}

}

type ErrorChunkRunnerFactoryConfigs struct {
	Name        string
	ErrorConfig Config
}

func testErrorConfigsFactory(config Config) error {
	_, err := Create(COPY_TO_ONLINE, config)
	return err
}

func brokenNumChunksOfflineFactory(pc.SerializedConfig) (provider.Provider, error) {
	return &BrokenNumChunksOfflineStore{}, nil
}

func brokenGetTableOnlineFactory(pc.SerializedConfig) (provider.Provider, error) {
	return &BrokenGetTableOnlineStore{}, nil
}

type BrokenNumChunksOfflineStore struct {
	provider.BaseProvider
}

func (store *BrokenNumChunksOfflineStore) AsOfflineStore() (provider.OfflineStore, error) {
	return store, nil
}

func (store *BrokenNumChunksOfflineStore) CreatePrimaryTable(id provider.ResourceID, schema provider.TableSchema) (dataset.Dataset, error) {
	return nil, nil
}

func (store *BrokenNumChunksOfflineStore) GetPrimaryTable(id provider.ResourceID, source metadata.SourceVariant) (dataset.Dataset, error) {
	return nil, nil
}

func (store *BrokenNumChunksOfflineStore) RegisterResourceFromSourceTable(id provider.ResourceID, schema provider.ResourceSchema, opts ...provider.ResourceOption) (provider.OfflineTable, error) {
	return nil, nil
}
func (store *BrokenNumChunksOfflineStore) RegisterPrimaryFromSourceTable(id provider.ResourceID, tableLocation pl.Location) (dataset.Dataset, error) {
	return nil, nil
}

func (store *BrokenNumChunksOfflineStore) SupportsTransformationOption(opt provider.TransformationOptionType) (bool, error) {
	return false, nil
}

func (store *BrokenNumChunksOfflineStore) CreateTransformation(config provider.TransformationConfig, opt ...provider.TransformationOption) error {
	return nil
}
func (b BrokenNumChunksOfflineStore) UpdateTransformation(config provider.TransformationConfig, opts ...provider.TransformationOption) error {
	return nil
}

func (store *BrokenNumChunksOfflineStore) GetTransformationTable(id provider.ResourceID) (dataset.Dataset, error) {
	return nil, nil
}

func (b BrokenNumChunksOfflineStore) CreateResourceTable(id provider.ResourceID, schema provider.TableSchema) (provider.OfflineTable, error) {
	return nil, nil
}
func (b BrokenNumChunksOfflineStore) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}

func (b BrokenNumChunksOfflineStore) CreateMaterialization(id provider.ResourceID, opts provider.MaterializationOptions) (dataset.Materialization, error) {
	return dataset.Materialization{}, nil
}

func (store BrokenNumChunksOfflineStore) SupportsMaterializationOption(opt provider.MaterializationOptionType) (bool, error) {
	return false, nil
}

func (b BrokenNumChunksOfflineStore) UpdateMaterialization(id provider.ResourceID, opts provider.MaterializationOptions) (dataset.Materialization, error) {
	return dataset.Materialization{}, nil
}

func (b BrokenNumChunksOfflineStore) GetMaterialization(id provider.MaterializationID) (dataset.Materialization, error) {
	return provider.NewLegacyMaterializationAdapterWithEmptySchema(&MaterializedFeaturesNumChunksBroken{""}), nil
}
func (b BrokenNumChunksOfflineStore) DeleteMaterialization(id provider.MaterializationID) error {
	return nil
}
func (b BrokenNumChunksOfflineStore) CreateTrainingSet(provider.TrainingSetDef) error {
	return nil
}

func (b BrokenNumChunksOfflineStore) UpdateTrainingSet(provider.TrainingSetDef) error {
	return nil
}

func (b BrokenNumChunksOfflineStore) GetTrainingSet(id provider.ResourceID) (provider.TrainingSetIterator, error) {
	return nil, nil
}

func (b BrokenNumChunksOfflineStore) CreateTrainTestSplit(def provider.TrainTestSplitDef) (func() error, error) {
	return nil, fmt.Errorf("not Implemented")
}

func (b BrokenNumChunksOfflineStore) GetTrainTestSplit(def provider.TrainTestSplitDef) (provider.TrainingSetIterator, provider.TrainingSetIterator, error) {
	return nil, nil, fmt.Errorf("not Implemented")
}

func (b BrokenNumChunksOfflineStore) GetBatchFeatures(tables []provider.ResourceID) (provider.BatchFeatureIterator, error) {
	return nil, nil
}

func (b BrokenNumChunksOfflineStore) Close() error {
	return nil
}

func (b BrokenNumChunksOfflineStore) ResourceLocation(id provider.ResourceID, resource any) (pl.Location, error) {
	return nil, nil
}

type BrokenGetTableOnlineStore struct {
	provider.BaseProvider
}

func (store *BrokenGetTableOnlineStore) AsOnlineStore() (provider.OnlineStore, error) {
	return store, nil
}

func (b BrokenGetTableOnlineStore) GetTable(feature, variant string) (provider.OnlineStoreTable, error) {
	return nil, errors.New("failed to get table")
}

func (b BrokenGetTableOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (provider.OnlineStoreTable, error) {
	return nil, nil
}

func (b BrokenGetTableOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (b BrokenGetTableOnlineStore) Close() error {
	return nil
}

func TestMaterializeRunnerFactoryErrorCoverage(t *testing.T) {
	err := provider.RegisterFactory("MOCK_OFFLINE_BROKEN_NUMCHUNKS", brokenNumChunksOfflineFactory)
	if err != nil {
		t.Fatalf("Could not register broken offline provider factory: %v", err)
	}
	provider.RegisterFactory("MOCK_ONLINE_BROKEN_GET_TABLE", brokenGetTableOnlineFactory)
	if err != nil {
		t.Fatalf("Could not register broken offline table factory: %v", err)
	}
	serializeMaterializeConfig := func(m MaterializedChunkRunnerConfig) Config {
		config, err := m.Serialize()
		if err != nil {
			t.Fatalf("error serializing materialized chunk runner config: %v", err)
		}
		return config
	}
	errorConfigs := []ErrorChunkRunnerFactoryConfigs{
		{
			Name:        "cannot deserialize config",
			ErrorConfig: []byte{},
		},
		{
			Name: "cannot configure online provider",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType: "Invalid_Online_type",
				// Have to skip cache to avoid tests messing with eachother.
				SkipCache: true,
			}),
		},
		{
			Name: "cannot configure offline provider",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:   pt.LocalOnline,
				OnlineConfig: []byte{},
				OfflineType:  "Invalid_Offline_type",
				// Have to skip cache to avoid tests messing with eachother.
				SkipCache: true,
			}),
		},
		{
			Name: "cannot convert online provider to online store",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:    pt.MemoryOffline,
				OnlineConfig:  []byte{},
				OfflineType:   pt.MemoryOffline,
				OfflineConfig: []byte{},
				// Have to skip cache to avoid tests messing with eachother.
				SkipCache: true,
			}),
		},
		{
			Name: "cannot convert offline provider to offline store",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:    pt.LocalOnline,
				OnlineConfig:  []byte{},
				OfflineType:   pt.LocalOnline,
				OfflineConfig: []byte{},
				// Have to skip cache to avoid tests messing with eachother.
				SkipCache: true,
			}),
		},
		{
			Name: "cannot get materialization",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:     pt.LocalOnline,
				OnlineConfig:   []byte{},
				OfflineType:    pt.MemoryOffline,
				OfflineConfig:  []byte{},
				MaterializedID: "",
				// Have to skip cache to avoid tests messing with eachother.
				SkipCache: false,
			}),
		},
		{
			Name: "cannot get table",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:     "MOCK_ONLINE_BROKEN_GET_TABLE",
				OnlineConfig:   []byte{},
				OfflineType:    "MOCK_OFFLINE",
				OfflineConfig:  []byte{},
				MaterializedID: "",
				// Have to skip cache to avoid tests messing with eachother.
				SkipCache: true,
			}),
		},
	}
	err = RegisterFactory("TEST_COPY_TO_ONLINE", MaterializedChunkRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register chunk runner factory: %v", err)
	}
	for _, config := range errorConfigs {
		t.Run(config.Name, func(t *testing.T) {
			if err := testErrorConfigsFactory(config.ErrorConfig); err == nil {
				t.Fatalf("Test Job Failed to catch error: %s", config.Name)
			}
		})
	}
	delete(factoryMap, "TEST_COPY_TO_ONLINE")
}

func TestJobs(t *testing.T) {
	emptyList := CopyTestData{
		Rows: []interface{}{},
	}
	basicNumList := CopyTestData{
		Rows: []interface{}{1, 2, 3, 4, 5},
	}
	stringNumList := CopyTestData{
		Rows: []interface{}{"one", "two", "three", "four", "five"},
	}
	multipleTypesList := CopyTestData{
		Rows: []interface{}{1, "two", 3.0, 'f', false},
	}

	numListofLists := CopyTestData{
		Rows: []interface{}{[]int{1, 2, 3}, []int{2, 3, 4}, []int{3, 4, 5}},
	}

	differentTypeLists := CopyTestData{
		Rows: []interface{}{[]int{1, 2, 3}, []string{"two", "three", "four"}, []float64{3.0, 4.0, 5.0}},
	}
	testJobs := []JobTestParams{
		{
			TestName:     "Basic copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "Partial copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "Chunk size overflow test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "Single copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "Final index copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     2,
		},
		{
			TestName:     "Last overlap chunk test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     1,
		},
		{
			TestName:     "Zero chunk size copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "String list copy test",
			Materialized: CreateMockFeatureRows(stringNumList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "Different types copy test",
			Materialized: CreateMockFeatureRows(multipleTypesList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "List features test",
			Materialized: CreateMockFeatureRows(numListofLists.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "List features different types",
			Materialized: CreateMockFeatureRows(differentTypeLists.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "No rows test",
			Materialized: CreateMockFeatureRows(emptyList.Rows),
			ChunkIdx:     0,
		},
		{
			TestName:     "No rows/zero chunk size test",
			Materialized: CreateMockFeatureRows(emptyList.Rows),
			ChunkIdx:     0,
		},
	}
	for _, param := range testJobs {
		t.Run(param.TestName, func(t *testing.T) {
			if err := testParams(param); err != nil {
				t.Fatalf("Test Job Failed: %s, %v\n", param.TestName, err)
			}
		})
	}
}

type MockOnlineStore struct {
	provider.BaseProvider
}

type MockOfflineStore struct {
	provider.BaseProvider
}

func (m MockOnlineStore) AsOnlineStore() (provider.OnlineStore, error) {
	return m, nil
}

func (m MockOfflineStore) AsOfflineStore() (provider.OfflineStore, error) {
	return m, nil
}

func (m MockOfflineStore) CreatePrimaryTable(id provider.ResourceID, schema provider.TableSchema) (dataset.Dataset, error) {
	return nil, nil
}

func (m MockOfflineStore) GetPrimaryTable(id provider.ResourceID, source metadata.SourceVariant) (dataset.Dataset, error) {
	return nil, nil
}

func (m MockOfflineStore) RegisterResourceFromSourceTable(id provider.ResourceID, schema provider.ResourceSchema, opts ...provider.ResourceOption) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineStore) RegisterPrimaryFromSourceTable(id provider.ResourceID, tableLocation pl.Location) (dataset.Dataset, error) {
	return nil, nil
}
func (m MockOfflineStore) SupportsTransformationOption(opt provider.TransformationOptionType) (bool, error) {
	return false, nil
}
func (m MockOfflineStore) CreateTransformation(config provider.TransformationConfig, opt ...provider.TransformationOption) error {
	return nil
}

func (m MockOfflineStore) UpdateTransformation(config provider.TransformationConfig, opts ...provider.TransformationOption) error {
	return nil
}

func (m MockOfflineStore) GetTransformationTable(id provider.ResourceID) (dataset.Dataset, error) {
	return nil, nil
}

func (m MockOfflineStore) UpdateMaterialization(id provider.ResourceID, opts provider.MaterializationOptions) (dataset.Materialization, error) {
	return dataset.Materialization{}, nil
}

func (m MockOfflineStore) UpdateTrainingSet(provider.TrainingSetDef) error {
	return nil
}

func (m MockOfflineStore) Close() error {
	return nil
}

func (m MockOfflineStore) ResourceLocation(id provider.ResourceID, resource any) (pl.Location, error) {
	return nil, nil
}

func (m MockOfflineStore) CreateTrainTestSplit(def provider.TrainTestSplitDef) (func() error, error) {
	return nil, fmt.Errorf("not Implemented")
}

func (m MockOfflineStore) GetTrainTestSplit(def provider.TrainTestSplitDef) (provider.TrainingSetIterator, provider.TrainingSetIterator, error) {
	return nil, nil, fmt.Errorf("not Implemented")

}

type MockOnlineStoreTable struct{}

func NewMockOnlineStore() *MockOnlineStore {
	return &MockOnlineStore{
		BaseProvider: provider.BaseProvider{
			ProviderType:   "MOCK_ONLINE",
			ProviderConfig: []byte{},
		},
	}
}

func (m MockOnlineStore) GetTable(feature, variant string) (provider.OnlineStoreTable, error) {
	return &MockOnlineStoreTable{}, nil
}

func (m MockOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (provider.OnlineStoreTable, error) {
	return &MockOnlineStoreTable{}, nil
}

func (m MockOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (m MockOnlineStore) Close() error {
	return nil
}

func (m MockOnlineStoreTable) Set(entity string, value interface{}) error {
	return nil
}

func (m MockOnlineStoreTable) Get(entity string) (interface{}, error) {
	return nil, nil
}

func (m MockOnlineStoreTable) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func NewMockOfflineStore() *MockOfflineStore {
	return &MockOfflineStore{
		BaseProvider: provider.BaseProvider{
			ProviderType:   "MOCK_OFFLINE",
			ProviderConfig: []byte{},
		},
	}
}

func (m MockOfflineStore) CreateResourceTable(id provider.ResourceID, schema provider.TableSchema) (provider.OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockOfflineStore) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockOfflineStore) CreateMaterialization(id provider.ResourceID, opts provider.MaterializationOptions) (dataset.Materialization, error) {
	return provider.NewLegacyMaterializationAdapterWithEmptySchema(MockMaterialization{}), nil
}

func (store MockOfflineStore) SupportsMaterializationOption(opt provider.MaterializationOptionType) (bool, error) {
	return false, nil
}
func (m MockOfflineStore) GetMaterialization(id provider.MaterializationID) (dataset.Materialization, error) {
	return provider.NewLegacyMaterializationAdapterWithEmptySchema(MockMaterialization{}), nil
}

func (m MockOfflineStore) DeleteMaterialization(id provider.MaterializationID) error {
	return nil
}

func (m MockOfflineStore) CreateTrainingSet(provider.TrainingSetDef) error {
	return nil
}

func (m MockOfflineStore) GetTrainingSet(id provider.ResourceID) (provider.TrainingSetIterator, error) {
	return nil, nil
}

func (m MockOfflineStore) GetBatchFeatures(tables []provider.ResourceID) (provider.BatchFeatureIterator, error) {
	return nil, nil
}

type MockOfflineTable struct{}

func (m MockOfflineTable) Write(provider.ResourceRecord) error {
	return nil
}

func (m MockOfflineTable) WriteBatch([]provider.ResourceRecord) error {
	return nil
}

func (m MockOfflineTable) Location() pl.Location {
	return nil
}

type MockMaterialization struct{}

func (m MockMaterialization) ID() provider.MaterializationID {
	return ""
}

func (m MockMaterialization) NumRows() (int64, error) {
	return 0, nil
}

func (m MockMaterialization) IterateSegment(begin, end int64) (provider.FeatureIterator, error) {
	return MockIterator{}, nil
}

func (m MockMaterialization) NumChunks() (int, error) {
	return 0, nil
}

func (m MockMaterialization) IterateChunk(idx int) (provider.FeatureIterator, error) {
	return MockIterator{}, nil
}

func (m MockMaterialization) Location() pl.Location {
	return nil
}

type MockIterator struct{}

func (m MockIterator) Next() bool {
	return false
}

func (m MockIterator) Value() provider.ResourceRecord {
	return provider.ResourceRecord{}
}

func (m MockIterator) Err() error {
	return nil
}

func (m MockIterator) Close() error {
	return nil
}

func mockOnlineStoreFactory(pc.SerializedConfig) (provider.Provider, error) {
	return NewMockOnlineStore(), nil
}

func mockOfflineStoreFactory(pc.SerializedConfig) (provider.Provider, error) {
	return NewMockOfflineStore(), nil
}

func init() {
	if err := provider.RegisterFactory("MOCK_ONLINE", mockOnlineStoreFactory); err != nil {
		panic(err)
	}
	if err := provider.RegisterFactory("MOCK_OFFLINE", mockOfflineStoreFactory); err != nil {
		panic(err)
	}
}
func TestChunkRunnerFactory(t *testing.T) {

	offline := NewMockOfflineStore()
	online := NewMockOnlineStore()
	resourceID := provider.ResourceID{
		Name: "test_name", Variant: "test_variant", Type: provider.Feature,
	}
	if _, err := online.CreateTable(resourceID.Name, resourceID.Variant, types.String); err != nil {
		t.Fatalf("Failed to create online resource table: %v", err)
	}
	if _, err := offline.CreateResourceTable(resourceID, provider.TableSchema{}); err != nil {
		t.Fatalf("Failed to create offline resource table: %v", err)
	}
	materialization, err := offline.CreateMaterialization(resourceID, provider.MaterializationOptions{Output: fs.Parquet})
	if err != nil {
		t.Fatalf("Failed to create materialization: %v", err)
	}
	chunkRunnerConfig := MaterializedChunkRunnerConfig{
		OnlineType:     "MOCK_ONLINE",
		OfflineType:    "MOCK_OFFLINE",
		OnlineConfig:   []byte{},
		OfflineConfig:  []byte{},
		MaterializedID: provider.MaterializationID(materialization.ID()),
		ResourceID:     resourceID,
		// Have to skip cache to avoid tests messing with eachother.
		SkipCache: true,
	}
	delete(factoryMap, "TEST_COPY_TO_ONLINE")
	if err := RegisterFactory("TEST_COPY_TO_ONLINE", MaterializedChunkRunnerFactory); err != nil {
		t.Fatalf("Failed to register factory: %v", err)
	}
	serializedConfig, err := chunkRunnerConfig.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize chunk runner config: %v", err)
	}
	runner, err := Create("TEST_COPY_TO_ONLINE", serializedConfig)
	if err != nil {
		t.Fatalf("Failed to create materialized chunk runner: %v", err)
	}
	indexRunner, ok := runner.(IndexRunner)
	if !ok {
		t.Fatalf("Cannot convert runner to index runner")
	}
	if err := indexRunner.SetIndex(0); err != nil {
		t.Fatalf("Failed to set index: %v", err)
	}
	watcher, err := indexRunner.Run()
	if err != nil {
		t.Fatalf("runner failed to run: %v", err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("runner failed while running: %v", err)
	}
}

func TestRunnerConfigDeserializeFails(t *testing.T) {
	failConfig := []byte("this should fail when attempted to be deserialized")
	config := &MaterializedChunkRunnerConfig{}
	if err := config.Deserialize(failConfig); err == nil {
		t.Fatalf("Failed to report error deserializing config")
	}
}
