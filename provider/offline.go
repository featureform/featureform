// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/featureform/metadata"
	"github.com/google/uuid"
)

const (
	MemoryOffline    Type = "MEMORY_OFFLINE"
	PostgresOffline       = "POSTGRES_OFFLINE"
	SnowflakeOffline      = "SNOWFLAKE_OFFLINE"
	RedshiftOffline       = "REDSHIFT_OFFLINE"
)

type ValueType string

const (
	NilType   ValueType = ""
	Int                 = "int"
	Int32               = "int32"
	Int64               = "int64"
	Float32             = "float32"
	Float64             = "float64"
	String              = "string"
	Bool                = "bool"
	Timestamp           = "time.Time"
)

type OfflineResourceType int

const (
	NoType OfflineResourceType = iota
	Label
	Feature
	TrainingSet
	Primary
	Transformation
)

var ProviderToMetadataResourceType = map[OfflineResourceType]metadata.ResourceType{
	Feature:        metadata.FEATURE_VARIANT,
	TrainingSet:    metadata.TRAINING_SET_VARIANT,
	Primary:        metadata.SOURCE_VARIANT,
	Transformation: metadata.SOURCE_VARIANT,
}

type FeatureLabelColumnType string

const (
	Entity FeatureLabelColumnType = "entity"
	Value                         = "value"
	TS                            = "ts"
)

type ResourceID struct {
	Name, Variant string
	Type          OfflineResourceType
}

func (id *ResourceID) check(expectedType OfflineResourceType, otherTypes ...OfflineResourceType) error {
	if id.Name == "" {
		return errors.New("ResourceID must have Name set")
	}
	// If there is one expected type, we will default to it.
	if id.Type == NoType && len(otherTypes) == 0 {
		id.Type = expectedType
		return nil
	}
	possibleTypes := append(otherTypes, expectedType)
	for _, t := range possibleTypes {
		if id.Type == t {
			return nil
		}
	}
	return fmt.Errorf("Unexpected ResourceID Type")
}

type TrainingSetDef struct {
	ID       ResourceID
	Label    ResourceID
	Features []ResourceID
}

func (def *TrainingSetDef) check() error {
	if err := def.ID.check(TrainingSet); err != nil {
		return err
	}
	if err := def.Label.check(Label); err != nil {
		return err
	}
	if len(def.Features) == 0 {
		return errors.New("training set must have atleast one feature")
	}
	for i := range def.Features {
		// We use features[i] to make sure that the Type value is updated to
		// Feature if it's unset.
		if err := def.Features[i].check(Feature); err != nil {
			return err
		}
	}
	return nil
}

type ColumnMapping struct {
	sourceColumn   string
	resourceColumn FeatureLabelColumnType
}

type TransformationConfig struct {
	TargetTableID ResourceID
	Query         string
	ColumnMapping []ColumnMapping
}

type OfflineStore interface {
	RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error)
	RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error)
	CreateTransformation(config TransformationConfig) error
	GetTransformationTable(id ResourceID) (TransformationTable, error)
	UpdateTransformation(config TransformationConfig) error
	CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error)
	GetPrimaryTable(id ResourceID) (PrimaryTable, error)
	CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error)
	GetResourceTable(id ResourceID) (OfflineTable, error)
	CreateMaterialization(id ResourceID) (Materialization, error)
	GetMaterialization(id MaterializationID) (Materialization, error)
	UpdateMaterialization(id ResourceID) (Materialization, error)
	DeleteMaterialization(id MaterializationID) error
	CreateTrainingSet(TrainingSetDef) error
	UpdateTrainingSet(TrainingSetDef) error
	GetTrainingSet(id ResourceID) (TrainingSetIterator, error)
	Provider
}

type MaterializationID string

type TrainingSetIterator interface {
	Next() bool
	Features() []interface{}
	Label() interface{}
	Err() error
}

type GenericTableIterator interface {
	Next() bool
	Values() GenericRecord
	Columns() []string
	Err() error
}

type Materialization interface {
	ID() MaterializationID
	NumRows() (int64, error)
	IterateSegment(begin, end int64) (FeatureIterator, error)
}

type FeatureIterator interface {
	Next() bool
	Value() ResourceRecord
	Err() error
}

// Used to implement sort.Interface
type ResourceRecords []ResourceRecord

func (recs ResourceRecords) Swap(i, j int) {
	recs[i], recs[j] = recs[j], recs[i]
}

func (recs ResourceRecords) Less(i, j int) bool {
	return recs[j].TS.After(recs[i].TS)
}

func (recs ResourceRecords) Len() int {
	return len(recs)
}

type ResourceRecord struct {
	Entity string
	Value  interface{}
	// Defaults to 00:00 on 01-01-0001, technically if a user sets a time
	// in a BC year for some reason, our default time would not be the
	// earliest time in the feature store.
	TS time.Time
}

type GenericRecord []interface{}

func (rec ResourceRecord) check() error {
	if rec.Entity == "" {
		return errors.New("ResourceRecord must have Entity set.")
	}
	return nil
}

type OfflineTable interface {
	Write(ResourceRecord) error
}

type PrimaryTable interface {
	Write(GenericRecord) error
	GetName() string
	IterateSegment(n int64) (GenericTableIterator, error)
	NumRows() (int64, error)
}

type TransformationTable interface {
	PrimaryTable
}

type ResourceSchema struct {
	Entity      string
	Value       string
	TS          string
	SourceTable string
}

type TableSchema struct {
	Columns []TableColumn
}

type TableColumn struct {
	Name string
	ValueType
}

type memoryOfflineStore struct {
	tables           map[ResourceID]*memoryOfflineTable
	materializations map[MaterializationID]*memoryMaterialization
	trainingSets     map[ResourceID]trainingRows
	BaseProvider
}

func memoryOfflineStoreFactory(serializedConfig SerializedConfig) (Provider, error) {
	return NewMemoryOfflineStore(), nil
}

func NewMemoryOfflineStore() *memoryOfflineStore {
	return &memoryOfflineStore{
		tables:           make(map[ResourceID]*memoryOfflineTable),
		materializations: make(map[MaterializationID]*memoryMaterialization),
		trainingSets:     make(map[ResourceID]trainingRows),
		BaseProvider: BaseProvider{
			ProviderType:   MemoryOffline,
			ProviderConfig: []byte{},
		},
	}
}

func (store *memoryOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *memoryOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return nil, fmt.Errorf("Snowflake RegisterResourceFromSourceTable not implemented")
}

func (store *memoryOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return nil, fmt.Errorf("Snowflake RegisterPrimaryFromSourceTable not implemented")
}

func (store *memoryOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, errors.New("primary table unsupported for this provider")
}

func (store *memoryOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return nil, errors.New("primary table unsupported for this provider")
}

func (store *memoryOfflineStore) CreateTransformation(config TransformationConfig) error {
	return errors.New("CreateTransformation unsupported for this provider")
}

func (store *memoryOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return errors.New("UpdateTransformation unsupported for this provider")
}

func (store *memoryOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, errors.New("GetTransformationTable unsupported for this provider")
}

func (store *memoryOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	if _, has := store.tables[id]; has {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	table := newMemoryOfflineTable()
	store.tables[id] = table
	return table, nil
}

func (store *memoryOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getMemoryResourceTable(id)
}

func (store *memoryOfflineStore) getMemoryResourceTable(id ResourceID) (*memoryOfflineTable, error) {
	table, has := store.tables[id]
	if !has {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return table, nil
}

// Used to implement sort.Interface for sorting.
type materializedRecords []ResourceRecord

func (recs materializedRecords) Len() int {
	return len(recs)
}

func (recs materializedRecords) Less(i, j int) bool {
	return recs[i].Entity < recs[j].Entity
}

func (recs materializedRecords) Swap(i, j int) {
	recs[i], recs[j] = recs[j], recs[i]
}

func (store *memoryOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	if id.Type != Feature {
		return nil, errors.New("only features can be materialized")
	}
	table, err := store.getMemoryResourceTable(id)
	if err != nil {
		return nil, err
	}
	matData := make(materializedRecords, 0, len(table.entityMap))
	for _, records := range table.entityMap {
		matRec := latestRecord(records)
		matData = append(matData, matRec)
	}
	sort.Sort(matData)
	matId := MaterializationID(uuid.NewString())
	mat := &memoryMaterialization{
		id:   matId,
		data: matData,
	}
	store.materializations[matId] = mat
	return mat, nil
}

type MaterializationNotFound struct {
	id MaterializationID
}

func (err *MaterializationNotFound) Error() string {
	return fmt.Sprintf("Materialization %s not found", err.id)
}

func (store *memoryOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	mat, has := store.materializations[id]
	if !has {
		return nil, &MaterializationNotFound{id}
	}
	return mat, nil
}

func (store *memoryOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return store.CreateMaterialization(id)
}

func (store *memoryOfflineStore) DeleteMaterialization(id MaterializationID) error {
	if _, has := store.materializations[id]; !has {
		return &MaterializationNotFound{id}
	}
	delete(store.materializations, id)
	return nil
}

func latestRecord(recs []ResourceRecord) ResourceRecord {
	latest := recs[0]
	for _, rec := range recs {
		if latest.TS.Before(rec.TS) {
			latest = rec
		}
	}
	return latest
}

func (store *memoryOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getMemoryResourceTable(def.Label)
	if err != nil {
		return err
	}
	features := make([]*memoryOfflineTable, len(def.Features))
	for i, id := range def.Features {
		feature, err := store.getMemoryResourceTable(id)
		if err != nil {
			return err
		}
		features[i] = feature
	}
	labelRecs := label.records()
	trainingData := make([]trainingRow, len(labelRecs))
	for i, rec := range labelRecs {
		featureVals := make([]interface{}, len(features))
		for i, feature := range features {
			featureVals[i] = feature.getLastValueBefore(rec.Entity, rec.TS)
		}
		labelVal := rec.Value
		trainingData[i] = trainingRow{
			Features: featureVals,
			Label:    labelVal,
		}
	}
	store.trainingSets[def.ID] = trainingData
	return nil
}

func (store *memoryOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return store.CreateTrainingSet(def)
}

func (store *memoryOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	data, has := store.trainingSets[id]
	if !has {
		return nil, &TrainingSetNotFound{id}
	}
	return data.Iterator(), nil
}

type TrainingSetNotFound struct {
	ID ResourceID
}

func (err *TrainingSetNotFound) Error() string {
	return fmt.Sprintf("TrainingSet with ID %v not found", err.ID)
}

type trainingRows []trainingRow

func (rows trainingRows) Iterator() TrainingSetIterator {
	return newMemoryTrainingSetIterator(rows)
}

type trainingRow struct {
	Features []interface{}
	Label    interface{}
}

type memoryTrainingRowsIterator struct {
	data trainingRows
	idx  int
}

func newMemoryTrainingSetIterator(data trainingRows) TrainingSetIterator {
	return &memoryTrainingRowsIterator{
		data: data,
		idx:  -1,
	}
}

func (it *memoryTrainingRowsIterator) Next() bool {
	lastIdx := len(it.data) - 1
	if it.idx == lastIdx {
		return false
	}
	it.idx++
	return true
}

func (it *memoryTrainingRowsIterator) Err() error {
	return nil
}

func (it *memoryTrainingRowsIterator) Features() []interface{} {
	return it.data[it.idx].Features
}

func (it *memoryTrainingRowsIterator) Label() interface{} {
	return it.data[it.idx].Label
}

type memoryOfflineTable struct {
	entityMap map[string][]ResourceRecord
}

func newMemoryOfflineTable() *memoryOfflineTable {
	return &memoryOfflineTable{
		entityMap: make(map[string][]ResourceRecord),
	}
}

func (table *memoryOfflineTable) records() []ResourceRecord {
	allRecs := make([]ResourceRecord, 0)
	for _, recs := range table.entityMap {
		allRecs = append(allRecs, recs...)
	}
	return allRecs
}

func (table *memoryOfflineTable) getLastValueBefore(entity string, ts time.Time) interface{} {
	recs, has := table.entityMap[entity]
	if !has {
		return nil
	}
	sortedRecs := ResourceRecords(recs)
	sort.Sort(sortedRecs)
	lastIdx := len(sortedRecs) - 1
	for i, rec := range sortedRecs {
		if rec.TS.After(ts) {
			// Entity was not yet set at timestamp, don't return a record.
			if i == 0 {
				return nil
			}
			// Use the record before this, since it would have been before TS.
			return sortedRecs[i-1].Value
		} else if i == lastIdx {
			// Every record happened before the TS, use the last record.
			return rec.Value
		}
	}
	// This line should never be able to be reached.
	panic("Unable to getLastValue before timestamp")
}

func (table *memoryOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	if err := rec.check(); err != nil {
		return err
	}
	if recs, has := table.entityMap[rec.Entity]; has {
		// Replace any record with the same timestamp/entity pair.
		for i, existingRec := range recs {
			if existingRec.TS == rec.TS {
				recs[i] = rec
				return nil
			}
		}
		table.entityMap[rec.Entity] = append(recs, rec)
	} else {
		table.entityMap[rec.Entity] = []ResourceRecord{rec}
	}
	return nil
}

type memoryMaterialization struct {
	id   MaterializationID
	data []ResourceRecord
}

func (mat *memoryMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *memoryMaterialization) NumRows() (int64, error) {
	return int64(len(mat.data)), nil
}

func (mat *memoryMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	segment := mat.data[start:end]
	return newMemoryFeatureIterator(segment), nil
}

type memoryFeatureIterator struct {
	data []ResourceRecord
	idx  int64
}

func newMemoryFeatureIterator(recs []ResourceRecord) FeatureIterator {
	return &memoryFeatureIterator{
		data: recs,
		idx:  -1,
	}
}

func (iter *memoryFeatureIterator) Next() bool {
	if isLastIdx := iter.idx == int64(len(iter.data)-1); isLastIdx {
		return false
	}
	iter.idx++
	return true
}

func (iter *memoryFeatureIterator) Value() ResourceRecord {
	return iter.data[iter.idx]
}

func (iter *memoryFeatureIterator) Err() error {
	return nil
}

// checkTimestamp checks the timestamp of a record.
// If the record has the default initialization value of 0001-01-01 00:00:00 +0000 UTC, it is changed
// to the start of unix epoch time, since snowflake cannot handle values before 1582
func checkTimestamp(rec ResourceRecord) ResourceRecord {
	checkRecord := ResourceRecord{}
	if rec.TS == checkRecord.TS {
		rec.TS = time.UnixMilli(0).UTC()
	}
	return rec
}
