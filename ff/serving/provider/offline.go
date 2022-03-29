package provider

import (
	"errors"
	"time"
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
}

type ResourceRecord struct {
	Entity string
	Value  interface{}
	TS     time.Time
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
	tables map[ResourceID]*memoryOfflineTable
	BaseProvider
}

func memoryOfflineStoreFactory(SerializedConfig) (Provider, error) {
	return NewMemoryOfflineStore(), nil
}

func NewMemoryOfflineStore() *memoryOfflineStore {
	return &memoryOfflineStore{
		tables: make(map[ResourceID]*memoryOfflineTable),
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
	table, has := store.tables[id]
	if !has {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return table, nil
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
		table.entityMap[rec.Entity] = append(recs, rec)
	} else {
		table.entityMap[rec.Entity] = []ResourceRecord{rec}
	}
	return nil
}
