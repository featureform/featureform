package provider

import (
	"fmt"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

type UnitTestProvider struct {
	ProviderType   pt.Type
	ProviderConfig pc.SerializedConfig
}

func (u UnitTestProvider) AsOnlineStore() (OnlineStore, error) {
	return MockUnitTestStore{}, nil
}

func (u UnitTestProvider) AsOfflineStore() (OfflineStore, error) {
	return MockUnitTestOfflineStore{}, nil
}

func (u UnitTestProvider) Type() pt.Type {
	return u.ProviderType
}

func (u UnitTestProvider) Config() pc.SerializedConfig {
	return u.ProviderConfig
}

func (u UnitTestProvider) Check() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

type UnitTestStore interface {
	GetTable(feature, variant string) (UnitTestTable, error)
	CreateTable(feature, variant string, valueType ValueType) (UnitTestTable, error)
	DeleteTable(feature, variant string) error
	Close() error
	Provider
}

type UnitTestTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
}

func unitTestStoreFactory(pc.SerializedConfig) (Provider, error) {
	return NewUnitTestStore(), nil
}

type MockUnitTestStore struct {
	UnitTestProvider
}

type MockUnitTestOfflineStore struct {
	UnitTestProvider
}

type MockUnitTestTable struct {
}

func NewUnitTestStore() *MockUnitTestStore {
	return &MockUnitTestStore{
		UnitTestProvider: UnitTestProvider{
			ProviderType:   "UNIT_TEST",
			ProviderConfig: []byte{},
		},
	}
}

/*
ONLINE STORE
*/

func (m MockUnitTestStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	return &MockUnitTestTable{}, nil
}

func (m MockUnitTestStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	return &MockUnitTestTable{}, nil
}

func (m MockUnitTestStore) DeleteTable(feature, variant string) error {
	return nil
}

func (m MockUnitTestStore) Close() error {
	return nil
}

func (m MockUnitTestStore) Check() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func (m MockUnitTestTable) Get(entity string) (interface{}, error) {
	return nil, nil
}

func (m MockUnitTestTable) Set(entity string, value interface{}) error {
	return nil
}

/*
OFFLINE UNIT STORE
*/

func (M MockUnitTestOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, nil
}

type MockPrimaryTable struct {
}

func (MockPrimaryTable) GetName() string {
	return ""
}

type UnitTestIterator struct {
	currentValue GenericRecord
	nextCount    int
}

func (u *UnitTestIterator) Next() bool {
	if u.nextCount < len(u.Columns()) {
		u.nextCount++
		return true
	} else {
		return false
	}
}

func (u *UnitTestIterator) Values() GenericRecord {
	return u.currentValue
}

func (UnitTestIterator) Columns() []string {
	return []string{"column1, column2"}
}

func (UnitTestIterator) Err() error {
	return nil
}

func (UnitTestIterator) Close() error {
	return nil
}

func (MockPrimaryTable) IterateSegment(int64) (GenericTableIterator, error) {
	records := make(GenericRecord, 2)
	records[0] = "row value"
	records[1] = "row value"
	return &UnitTestIterator{
		currentValue: records,
		nextCount:    0,
	}, nil
}

func (MockPrimaryTable) NumRows() (int64, error) {
	return 1, nil
}

func (MockPrimaryTable) Write(GenericRecord) error {
	return nil
}

func (MockPrimaryTable) WriteBatch([]GenericRecord) error {
	return nil
}

func (M MockUnitTestOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return MockPrimaryTable{}, nil
}

func (M MockUnitTestOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return nil, nil
}

func (M MockUnitTestOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return nil, nil
}

func (M MockUnitTestOfflineStore) CreateTransformation(config TransformationConfig) error {
	return nil
}

func (M MockUnitTestOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return nil
}

func (M MockUnitTestOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, nil
}

func (M MockUnitTestOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
}

func (M MockUnitTestOfflineStore) UpdateTrainingSet(TrainingSetDef) error {
	return nil
}

func (M MockUnitTestOfflineStore) Close() error {
	return nil
}

func (M MockUnitTestOfflineStore) Check() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

type MockMaterialization struct{}

func (m MockMaterialization) ID() MaterializationID {
	return ""
}

func (m MockMaterialization) NumRows() (int64, error) {
	return 0, nil
}

type MockIterator struct{}

func (m MockIterator) Next() bool {
	return false
}

func (m MockIterator) Value() ResourceRecord {
	return ResourceRecord{}
}

func (m MockIterator) Err() error {
	return nil
}

func (m MockIterator) Close() error {
	return nil
}

func (m MockMaterialization) IterateSegment(begin, end int64) (FeatureIterator, error) {
	return MockIterator{}, nil
}

func (m MockUnitTestOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	return MockMaterialization{}, nil
}

type MockOfflineTable struct{}

func (m MockOfflineTable) Write(ResourceRecord) error {
	return nil
}

func (m MockOfflineTable) WriteBatch([]ResourceRecord) error {
	return nil
}

func (m MockUnitTestOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockUnitTestOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockUnitTestOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return MockMaterialization{}, nil
}

func (m MockUnitTestOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return nil
}

func (m MockUnitTestOfflineStore) CreateTrainingSet(TrainingSetDef) error {
	return nil
}

func (m MockUnitTestOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return nil, nil
}
