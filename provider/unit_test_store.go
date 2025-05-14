// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"fmt"

	"github.com/featureform/fferr"
	fftype "github.com/featureform/fftypes"
	"github.com/featureform/provider/dataset"

	"github.com/featureform/filestore"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

// ====================================
// UnitTestProvider
// ====================================

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

func (u UnitTestProvider) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func (u UnitTestProvider) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

// ====================================
// Interface definitions
// ====================================

type UnitTestStore interface {
	GetTable(feature, variant string) (UnitTestTable, error)
	CreateTable(feature, variant string, valueType types.ValueType) (UnitTestTable, error)
	DeleteTable(feature, variant string) error
	Close() error
	Provider
}

type UnitTestTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
}

// Factory function
func unitTestStoreFactory(pc.SerializedConfig) (Provider, error) {
	return NewUnitTestStore(), nil
}

// ====================================
// MockUnitTestStore (Online Store)
// ====================================

type MockUnitTestStore struct {
	UnitTestProvider
}

func NewUnitTestStore() *MockUnitTestStore {
	return &MockUnitTestStore{
		UnitTestProvider: UnitTestProvider{
			ProviderType:   "UNIT_TEST",
			ProviderConfig: []byte{},
		},
	}
}

func (m MockUnitTestStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	return &MockUnitTestTable{}, nil
}

func (m MockUnitTestStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	return &MockUnitTestTable{}, nil
}

func (m MockUnitTestStore) DeleteTable(feature, variant string) error {
	return nil
}

func (m MockUnitTestStore) Close() error {
	return nil
}

func (m MockUnitTestStore) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

// ====================================
// MockUnitTestTable
// ====================================

type MockUnitTestTable struct {
}

func (m MockUnitTestTable) Get(entity string) (interface{}, error) {
	return nil, nil
}

func (m MockUnitTestTable) Set(entity string, value interface{}) error {
	return nil
}

// ====================================
// MockUnitTestOfflineStore
// ====================================

type MockUnitTestOfflineStore struct {
	UnitTestProvider
}

func (m MockUnitTestOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (dataset.Dataset, error) {
	return nil, nil
}

func (m MockUnitTestOfflineStore) GetBatchFeatures(tables []ResourceID) (BatchFeatureIterator, error) {
	return nil, fmt.Errorf("batch features not implemented for this provider")
}

func (m MockUnitTestOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (dataset.Dataset, error) {
	return &MockDataset{}, nil
}

func (m MockUnitTestOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	return nil, nil
}

func (m MockUnitTestOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, stableLocation pl.Location) (dataset.Dataset, error) {
	return nil, nil
}

func (m MockUnitTestOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (m MockUnitTestOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	return nil
}

func (m MockUnitTestOfflineStore) UpdateTransformation(config TransformationConfig, opt ...TransformationOption) error {
	return nil
}

func (m MockUnitTestOfflineStore) GetTransformationTable(id ResourceID) (dataset.Dataset, error) {
	return &MockDataset{}, nil
}

func (m MockUnitTestOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (dataset.Materialization, error) {
	return dataset.Materialization{}, nil
}

func (m MockUnitTestOfflineStore) UpdateTrainingSet(TrainingSetDef) error {
	return nil
}

func (m MockUnitTestOfflineStore) Close() error {
	return nil
}

func (m MockUnitTestOfflineStore) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func (m MockUnitTestOfflineStore) ResourceLocation(id ResourceID, resource any) (pl.Location, error) {
	path := id.ToFilestorePath()
	filePath, err := filestore.NewEmptyFilepath(filestore.FileSystem)
	if err != nil {
		return nil, err
	}
	filePath.ParseFilePath(path)
	return pl.NewFileLocation(filePath), nil
}

func (m MockUnitTestOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (dataset.Materialization, error) {
	return NewLegacyMaterializationAdapterWithEmptySchema(MockMaterialization{}), nil
}

func (m MockUnitTestOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	return false, nil
}

func (m MockUnitTestOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockUnitTestOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return MockOfflineTable{}, nil
}

func (m MockUnitTestOfflineStore) GetMaterialization(id MaterializationID) (dataset.Materialization, error) {
	return NewLegacyMaterializationAdapterWithEmptySchema(MockMaterialization{}), nil
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

func (m MockUnitTestOfflineStore) CreateTrainTestSplit(TrainTestSplitDef) (func() error, error) {
	return nil, nil
}

func (m MockUnitTestOfflineStore) GetTrainTestSplit(TrainTestSplitDef) (TrainingSetIterator, TrainingSetIterator, error) {
	return nil, nil, nil
}

// ====================================
// MockPrimaryTable
// ====================================

type MockDataset struct {
	name string
}

// Location implements the dataset.Dataset interface
func (md *MockDataset) Location() pl.Location {
	loc, err := pl.NewFileLocationFromURI(fmt.Sprintf("memory://%s", md.name))
	if err != nil {
		return nil
	}
	return loc
}

// Iterator implements the dataset.Dataset interface
func (md *MockDataset) Iterator(ctx context.Context, limit int64) (dataset.Iterator, error) {
	return NewUnitTestIterator(), nil
}

// Schema implements the dataset.Dataset interface
func (md *MockDataset) Schema() fftype.Schema {
	return fftype.Schema{
		Fields: []fftype.ColumnSchema{
			{Name: "column1", Type: fftype.String},
			{Name: "column2", Type: fftype.Bool},
			{Name: "column3", Type: fftype.Int64},
		},
	}
}

// WriteBatch implements the dataset.Dataset interface
func (md *MockDataset) WriteBatch(ctx context.Context, records []fftype.Row) error {
	return nil
}

// Len implements the dataset.Dataset interface
func (md *MockDataset) Len() (int64, error) {
	return 3, nil
}

// ====================================
// UnitTestIterator
// ====================================

// MockIterator implements only the dataset.Iterator interface
type UnitTestIterator struct {
	pos        int
	currentRow fftype.Row
}

// NewUnitTestIterator creates a new mock iterator
func NewUnitTestIterator() *UnitTestIterator {
	return &UnitTestIterator{pos: -1}
}

// Next implements the dataset.Iterator interface
func (mi *UnitTestIterator) Next() bool {
	if mi.pos >= 2 {
		return false
	}

	mi.pos++

	// All rows contain the same data to match test expectations
	mi.currentRow = fftype.Row{
		{NativeType: fftype.NativeTypeLiteral("column1"), Value: "row string value", Type: fftype.String},
		{NativeType: fftype.NativeTypeLiteral("column2"), Value: true, Type: fftype.Bool},
		{NativeType: fftype.NativeTypeLiteral("column3"), Value: int64(10), Type: fftype.Int64},
	}

	return true
}

// Values implements the dataset.Iterator interface
func (mi *UnitTestIterator) Values() fftype.Row {
	return mi.currentRow
}

// Schema implements the dataset.Iterator interface
func (mi *UnitTestIterator) Schema() fftype.Schema {
	return fftype.Schema{
		Fields: []fftype.ColumnSchema{
			{Name: fftype.ColumnName("column1"), Type: fftype.String},
			{Name: fftype.ColumnName("column2"), Type: fftype.Bool},
			{Name: fftype.ColumnName("column3"), Type: fftype.Int64},
		},
	}
}

// Close implements the dataset.Iterator interface
func (mi *UnitTestIterator) Close() error {
	return nil
}

func (mi *UnitTestIterator) Err() error {
	return nil
}

// ====================================
// MockMaterialization
// ====================================

type MockMaterialization struct{}

func (m MockMaterialization) ID() MaterializationID {
	return ""
}

func (m MockMaterialization) NumRows() (int64, error) {
	return 0, nil
}

func (m MockMaterialization) IterateSegment(begin, end int64) (FeatureIterator, error) {
	return MockIterator{}, nil
}

func (m MockMaterialization) NumChunks() (int, error) {
	return 0, nil
}

func (m MockMaterialization) IterateChunk(idx int) (FeatureIterator, error) {
	return MockIterator{}, nil
}

func (m MockMaterialization) Location() pl.Location {
	return nil
}

// ====================================
// MockIterator
// ====================================

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

// ====================================
// MockOfflineTable
// ====================================

type MockOfflineTable struct{}

func (m MockOfflineTable) Write(ResourceRecord) error {
	return nil
}

func (m MockOfflineTable) WriteBatch([]ResourceRecord) error {
	return nil
}

func (m MockOfflineTable) Location() pl.Location {
	return nil
}
