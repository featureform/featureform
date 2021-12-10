package main

import (
	"sync"
)

type MetadataProvider interface {
	TrainingSetMetadata(name, version string) (MetadataEntry, error)
	SetTrainingSetMetadata(name, version string, entry MetadataEntry) error
}

type MetadataEntry struct {
	StorageId string
	Key       map[string]string
}

func (entry MetadataEntry) Valid() error {
	if entry.StorageId == "" {
		return &MetadataError{"StorageId must be set in metadata."}
	}
	return nil
}

type DatasetId struct {
	Name, Version string
}

func (id DatasetId) Valid() error {
	if id.Name == "" {
		return &MetadataError{"Dataset name must be provided."}
	}
	return nil
}

type LocalMemoryMetadata struct {
	data map[DatasetId]MetadataEntry
	mtx  *sync.RWMutex
}

func NewLocalMemoryMetadata() (MetadataProvider, error) {
	return &LocalMemoryMetadata{
		data: make(map[DatasetId]MetadataEntry),
		mtx:  &sync.RWMutex{},
	}, nil
}

type MetadataError struct {
	Msg string
}

func (err *MetadataError) Error() string {
	return err.Msg
}

func (provider *LocalMemoryMetadata) SetTrainingSetMetadata(name, version string, entry MetadataEntry) error {
	provider.mtx.Lock()
	defer provider.mtx.Unlock()
	if err := entry.Valid(); err != nil {
		return err
	}
	provider.data[DatasetId{name, version}] = entry
	return nil
}

func (provider *LocalMemoryMetadata) TrainingSetMetadata(name, version string) (MetadataEntry, error) {
	provider.mtx.Lock()
	defer provider.mtx.Unlock()
	entry, has := provider.data[DatasetId{name, version}]
	if !has {
		return entry, &MetadataError{"Dataset not found."}
	}
	return entry, nil
}
