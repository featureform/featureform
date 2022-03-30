package provider

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

const (
	MemoryOffline Type = "MEMORY_OFFLINE"
)

type OfflineResourceType int

const (
	None OfflineResourceType = iota
	Label
	Feature
)

type ResourceID struct {
	Name, Variant string
	Type          OfflineResourceType
}

func (id ResourceID) Check() error {
	if id.Name == "" {
		return errors.New("ResourceID must have Name set")
	}
	if id.Type != Label && id.Type != Feature {
		return errors.New("Invalid ResourceID Type")
	}
	return nil
}

type OfflineStore interface {
	CreateResourceTable(id ResourceID) (OfflineTable, error)
	GetResourceTable(id ResourceID) (OfflineTable, error)
	CreateMaterialization(id ResourceID) (Materialization, error)
	GetMaterialization(id MaterializationID) (Materialization, error)
}

type MaterializationID string

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

type ResourceRecord struct {
	Entity string
	Value  interface{}
	// Defaults to 00:00 on 01-01-0001, technically if a user sets a time
	// in a BC year for some reason, our default time would not be the
	// earliest time in the feature store.
	TS time.Time
}

func (rec ResourceRecord) Check() error {
	if rec.Entity == "" {
		return errors.New("ResourceRecord must have Entity set.")
	}
	return nil
}

type OfflineTable interface {
	Write(ResourceRecord) error
}

type memoryOfflineStore struct {
	tables           map[ResourceID]*memoryOfflineTable
	materializations map[MaterializationID]*memoryMaterialization
	BaseProvider
}

func memoryOfflineStoreFactory(SerializedConfig) (Provider, error) {
	return NewMemoryOfflineStore(), nil
}

func NewMemoryOfflineStore() *memoryOfflineStore {
	return &memoryOfflineStore{
		tables:           make(map[ResourceID]*memoryOfflineTable),
		materializations: make(map[MaterializationID]*memoryMaterialization),
	}
}

func (store *memoryOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *memoryOfflineStore) CreateResourceTable(id ResourceID) (OfflineTable, error) {
	if err := id.Check(); err != nil {
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
		return nil, errors.New("Only features can be materialized")
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

func latestRecord(recs []ResourceRecord) ResourceRecord {
	latest := recs[0]
	for _, rec := range recs {
		if latest.TS.Before(rec.TS) {
			latest = rec
		}
	}
	return latest
}

type memoryOfflineTable struct {
	entityMap map[string][]ResourceRecord
}

func newMemoryOfflineTable() *memoryOfflineTable {
	return &memoryOfflineTable{
		entityMap: make(map[string][]ResourceRecord),
	}
}

func (table *memoryOfflineTable) Write(rec ResourceRecord) error {
	if err := rec.Check(); err != nil {
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
	isLastIdx := iter.idx == int64(len(iter.data)-1)
	if isLastIdx {
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
