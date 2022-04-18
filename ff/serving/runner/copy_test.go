package runner

import (
	"errors"
	"fmt"
	provider "github.com/featureform/serving/provider"
	"github.com/google/uuid"
	"reflect"
	"sync"
	"testing"
)

type MockMaterializedFeatures struct {
	id   provider.MaterializationID
	Rows []provider.ResourceRecord
}

func (m *MockMaterializedFeatures) ID() provider.MaterializationID {
	return m.id
}

func (m *MockMaterializedFeatures) NumRows() (int64, error) {
	return int64(len(m.Rows)), nil
}

func (m *MockMaterializedFeatures) IterateSegment(begin int64, end int64) (provider.FeatureIterator, error) {
	return &MockFeatureIterator{
		CurrentIndex: -1,
		Slice:        m.Rows[begin:end],
	}, nil
}

type MaterializedFeaturesNumRowsBroken struct {
	id provider.MaterializationID
}

func (m *MaterializedFeaturesNumRowsBroken) ID() provider.MaterializationID {
	return m.id
}

func (m *MaterializedFeaturesNumRowsBroken) NumRows() (int64, error) {
	return 0, fmt.Errorf("cannot fetch number of rows")
}

func (m *MaterializedFeaturesNumRowsBroken) IterateSegment(begin int64, end int64) (provider.FeatureIterator, error) {
	return nil, nil
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

type MockOnlineTable struct {
	DataTable map[string]interface{}
}

func (m *MockOnlineTable) Set(entity string, value interface{}) error {
	m.DataTable[entity] = value
	return nil
}

func (m *MockOnlineTable) Get(entity string) (interface{}, error) {
	value, exists := m.DataTable[entity]
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
	Materialized MockMaterializedFeatures
	ChunkSize    int64
	ChunkIdx     int64
}

type ErrorJobTestParams struct {
	ErrorName    string
	Materialized provider.Materialization
	Table        provider.OnlineStoreTable
	ChunkSize    int64
	ChunkIdx     int64
}

func testParams(params JobTestParams) error {
	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}
	featureRows := params.Materialized.Rows
	job := &MaterializedChunkRunner{
		Materialized: &params.Materialized,
		Table:        table,
		ChunkSize:    params.ChunkSize,
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
	rowStart := params.ChunkIdx * params.ChunkSize
	rowEnd := rowStart + params.ChunkSize
	if rowEnd > int64(len(featureRows)) {
		rowEnd = int64(len(featureRows))
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
	job := &MaterializedChunkRunner{
		Materialized: params.Materialized,
		Table:        params.Table,
		ChunkSize:    params.ChunkSize,
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

func CreateMockFeatureRows(data []interface{}) MockMaterializedFeatures {
	featureRows := make([]provider.ResourceRecord, len(data))
	for i, row := range data {
		featureRows[i] = provider.ResourceRecord{Entity: fmt.Sprintf("entity_%d", i), Value: row}
	}
	return MockMaterializedFeatures{id: provider.MaterializationID(uuid.NewString()), Rows: featureRows}
}

func TestErrorCoverage(t *testing.T) {
	minimalMockFeatureRows := CreateMockFeatureRows([]interface{}{1})
	errorJobs := []ErrorJobTestParams{
		{
			ErrorName:    "iterator run error",
			Materialized: &MaterializedFeaturesIterateRunBroken{provider.MaterializationID(uuid.NewString())},
			Table:        &BrokenOnlineTable{},
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		{
			ErrorName:    "table set error",
			Materialized: &minimalMockFeatureRows,
			Table:        &BrokenOnlineTable{},
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		{
			ErrorName:    "create iterator error",
			Materialized: &MaterializedFeaturesIterateBroken{provider.MaterializationID(uuid.NewString())},
			Table:        &BrokenOnlineTable{},
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		{
			ErrorName:    "get num rows error",
			Materialized: &MaterializedFeaturesNumRowsBroken{provider.MaterializationID(uuid.NewString())},
			Table:        &BrokenOnlineTable{},
			ChunkSize:    1,
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
	_, err := Create("COPY", config)
	return err
}

func brokenNumRowsOfflineFactory(provider.SerializedConfig) (provider.Provider, error) {
	return &BrokenNumRowsOfflineStore{}, nil
}

func brokenGetTableOnlineFactory(provider.SerializedConfig) (provider.Provider, error) {
	return &BrokenGetTableOnlineStore{}, nil
}

type BrokenNumRowsOfflineStore struct {
	provider.BaseProvider
}

func (store *BrokenNumRowsOfflineStore) AsOfflineStore() (provider.OfflineStore, error) {
	return store, nil
}

func (b BrokenNumRowsOfflineStore) CreateResourceTable(id provider.ResourceID, schema provider.SerializedTableSchema) (provider.OfflineTable, error) {
	return nil, nil
}
func (b BrokenNumRowsOfflineStore) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}
func (b BrokenNumRowsOfflineStore) CreateMaterialization(id provider.ResourceID) (provider.Materialization, error) {
	return nil, nil
}
func (b BrokenNumRowsOfflineStore) GetMaterialization(id provider.MaterializationID) (provider.Materialization, error) {
	return &MaterializedFeaturesNumRowsBroken{""}, nil
}
func (b BrokenNumRowsOfflineStore) DeleteMaterialization(id provider.MaterializationID) error {
	return nil
}
func (b BrokenNumRowsOfflineStore) CreateTrainingSet(provider.TrainingSetDef) error {
	return nil
}
func (b BrokenNumRowsOfflineStore) GetTrainingSet(id provider.ResourceID) (provider.TrainingSetIterator, error) {
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
func (b BrokenGetTableOnlineStore) CreateTable(feature, variant string) (provider.OnlineStoreTable, error) {
	return nil, nil
}

func TestMaterializeRunnerFactoryErrorCoverage(t *testing.T) {
	err := provider.RegisterFactory("MOCK_OFFLINE_BROKEN_NUMROWS", brokenNumRowsOfflineFactory)
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
			}),
		},
		{
			Name: "cannot configure offline provider",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:   provider.LocalOnline,
				OnlineConfig: []byte{},
				OfflineType:  "Invalid_Offline_type",
			}),
		},
		{
			Name: "cannot convert online provider to online store",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:    provider.MemoryOffline,
				OnlineConfig:  []byte{},
				OfflineType:   provider.MemoryOffline,
				OfflineConfig: []byte{},
			}),
		},
		{
			Name: "cannot convert offline provider to offline store",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:    provider.LocalOnline,
				OnlineConfig:  []byte{},
				OfflineType:   provider.LocalOnline,
				OfflineConfig: []byte{},
			}),
		},
		{
			Name: "cannot get materialization",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:     provider.LocalOnline,
				OnlineConfig:   []byte{},
				OfflineType:    provider.MemoryOffline,
				OfflineConfig:  []byte{},
				MaterializedID: "",
			}),
		},
		{
			Name: "cannot get num rows",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:     "MOCK_ONLINE",
				OnlineConfig:   []byte{},
				OfflineType:    "MOCK_OFFLINE_BROKEN_NUMROWS",
				OfflineConfig:  []byte{},
				MaterializedID: "",
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
			}),
		},
		{
			Name: "chunk runner starts after end of rows",
			ErrorConfig: serializeMaterializeConfig(MaterializedChunkRunnerConfig{
				OnlineType:     "MOCK_ONLINE",
				OnlineConfig:   []byte{},
				OfflineType:    "MOCK_OFFLINE",
				OfflineConfig:  []byte{},
				MaterializedID: "",
				ChunkSize:      1,
				ChunkIdx:       1,
			}),
		},
	}
	err = RegisterFactory("COPY", MaterializedChunkRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register chunk runner factory: %v", err)
	}
	for _, config := range errorConfigs {
		if err := testErrorConfigsFactory(config.ErrorConfig); err == nil {
			t.Fatalf("Test Job Failed to catch error: %s", config.Name)
		}
	}
	delete(factoryMap, "COPY")
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
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		{
			TestName:     "Partial copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    2,
			ChunkIdx:     0,
		},
		{
			TestName:     "Chunk size overflow test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    6,
			ChunkIdx:     0,
		},
		{
			TestName:     "Single copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		{
			TestName:     "Final index copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    1,
			ChunkIdx:     4,
		},
		{
			TestName:     "Last overlap chunk test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    2,
			ChunkIdx:     2,
		},
		{
			TestName:     "Zero chunk size copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    0,
			ChunkIdx:     0,
		},
		{
			TestName:     "String list copy test",
			Materialized: CreateMockFeatureRows(stringNumList.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		{
			TestName:     "Different types copy test",
			Materialized: CreateMockFeatureRows(multipleTypesList.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		{
			TestName:     "List features test",
			Materialized: CreateMockFeatureRows(numListofLists.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		{
			TestName:     "List features different types",
			Materialized: CreateMockFeatureRows(differentTypeLists.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		{
			TestName:     "No rows test",
			Materialized: CreateMockFeatureRows(emptyList.Rows),
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		{
			TestName:     "No rows/zero chunk size test",
			Materialized: CreateMockFeatureRows(emptyList.Rows),
			ChunkSize:    0,
			ChunkIdx:     0,
		},
	}
	for _, param := range testJobs {
		if err := testParams(param); err != nil {
			t.Fatalf("Test Job Failed: %s, %v\n", param.TestName, err)
		}
	}
}

func TestJobIncompleteStatus(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	materialized := MaterializedFeaturesNumRowsBroken{}
	table := &BrokenOnlineTable{}
	job := &MaterializedChunkRunner{
		Materialized: &materialized,
		Table:        table,
		ChunkSize:    0,
		ChunkIdx:     0,
	}
	completionWatcher, err := job.Run()
	if err != nil {
		t.Fatalf("Job failed to run")
	}
	if complete := completionWatcher.Complete(); complete {
		t.Fatalf("Job reports completed while not complete")
	}
	completionWatcher.String()
	mu.Unlock()
	if err = completionWatcher.Wait(); err != nil {
		t.Fatalf("Job failed to cancel at 0 chunk size")
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

func (m MockOnlineStore) CreateTable(feature, variant string) (provider.OnlineStoreTable, error) {
	return &MockOnlineStoreTable{}, nil
}

func (m MockOnlineStoreTable) Set(entity string, value interface{}) error {
	return nil
}

func (m MockOnlineStoreTable) Get(entity string) (interface{}, error) {
	return nil, nil
}

func NewMockOfflineStore() *MockOfflineStore {
	return &MockOfflineStore{
		BaseProvider: provider.BaseProvider{
			ProviderType:   "MOCK_OFFLINE",
			ProviderConfig: []byte{},
		},
	}
}

func (m MockOfflineStore) CreateResourceTable(id provider.ResourceID, schema provider.SerializedTableSchema) (provider.OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockOfflineStore) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockOfflineStore) CreateMaterialization(id provider.ResourceID) (provider.Materialization, error) {
	return MockMaterialization{}, nil
}

func (m MockOfflineStore) GetMaterialization(id provider.MaterializationID) (provider.Materialization, error) {
	return MockMaterialization{}, nil
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

type MockOfflineTable struct{}

func (m MockOfflineTable) Write(provider.ResourceRecord) error {
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

func mockOnlineStoreFactory(provider.SerializedConfig) (provider.Provider, error) {
	return NewMockOnlineStore(), nil
}

func mockOfflineStoreFactory(provider.SerializedConfig) (provider.Provider, error) {
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
		"test_name", "test_variant", provider.Feature,
	}
	if _, err := online.CreateTable(resourceID.Name, resourceID.Variant); err != nil {
		t.Fatalf("Failed to create online resource table: %v", err)
	}
	if _, err := offline.CreateResourceTable(resourceID, provider.SerializedTableSchema{}); err != nil {
		t.Fatalf("Failed to create offline resource table: %v", err)
	}
	materialization, err := offline.CreateMaterialization(resourceID)
	if err != nil {
		t.Fatalf("Failed to create materialization: %v", err)
	}
	chunkRunnerConfig := MaterializedChunkRunnerConfig{
		OnlineType:     "MOCK_ONLINE",
		OfflineType:    "MOCK_OFFLINE",
		OnlineConfig:   []byte{},
		OfflineConfig:  []byte{},
		MaterializedID: materialization.ID(),
		ResourceID:     resourceID,
		ChunkSize:      0,
	}
	if err != nil {
		t.Fatalf("Failed to create new chunk runner config: %v", err)
	}
	delete(factoryMap, "COPY")
	if err := RegisterFactory("COPY", MaterializedChunkRunnerFactory); err != nil {
		t.Fatalf("Failed to register factory: %v", err)
	}
	serializedConfig, err := chunkRunnerConfig.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize chunk runner config: %v", err)
	}
	runner, err := Create("COPY", serializedConfig)
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
