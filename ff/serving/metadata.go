package main

import (
	"sync"

	"go.uber.org/zap"
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
	Logger *zap.SugaredLogger
	data   map[DatasetId]MetadataEntry
	mtx    *sync.RWMutex
}

func NewLocalMemoryMetadata(logger *zap.SugaredLogger) (MetadataProvider, error) {
	logger.Debug("Creating new local memory metadata")
	return &LocalMemoryMetadata{
		Logger: logger,
		data:   make(map[DatasetId]MetadataEntry),
		mtx:    &sync.RWMutex{},
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
	logger := provider.Logger.With("Name", name, "Version", version, "Entry", entry)
	logger.Debug("Setting training metadata")
	if err := entry.Valid(); err != nil {
		logger.Errorw("Invalid Metadata Entry", "Error", err)
		return err
	}
	provider.data[DatasetId{name, version}] = entry
	return nil
}

func (provider *LocalMemoryMetadata) TrainingSetMetadata(name, version string) (MetadataEntry, error) {
	provider.mtx.Lock()
	defer provider.mtx.Unlock()
	logger := provider.Logger.With("Name", name, "Version", version)
	logger.Debug("Retrieving Metadata")
	entry, has := provider.data[DatasetId{name, version}]
	if !has {
		logger.Error("Metadata not found")
		return entry, &MetadataError{"Dataset not found."}
	}
	return entry, nil
}
