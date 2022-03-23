package main

import (
	"sync"

	"go.uber.org/zap"
)

type MetadataProvider interface {
			TrainingSetMetadata(name, version string) (TrainingSetEntry, error)


	SetTrainingSetMetadata(name, version string, entry TrainingSetEntry) error
FeatureMetadata(name, version string) (FeatureEntry, error)
			SetFeatureMetadata(name, version string, entry FeatureEntry) error
}

type FeatureEntry struct {
	StorageId string
	Entity    string
	Key       map[string]string
}

type TrainingSetEntry struct {
	StorageId string
	Key       map[string]string
}

func (entry TrainingSetEntry) Valid() error {
	if entry.StorageId == "" {
		return &MetadataError{"StorageId must be set in metadata."}
	}
	return nil
}

type TrainingSetId struct {
	Name, Version string
}

func (id TrainingSetId) Valid() error {
	if id.Name == "" {
		return &MetadataError{"Dataset name must be provided."}
	}
	return nil
}

type FeatureId struct {
	Name, Version string
}

type LocalMemoryMetadata struct {
	Logger         *zap.SugaredLogger
	trainingSets   map[TrainingSetId]TrainingSetEntry
	trainingSetMtx *sync.RWMutex
	features       map[FeatureId]FeatureEntry
	featureMtx     *sync.RWMutex
}

func NewLocalMemoryMetadata(logger *zap.SugaredLogger) (MetadataProvider, error) {
	logger.Debug("Creating new local memory metadata")
	return &LocalMemoryMetadata{
		Logger:         logger,
		trainingSets:   make(map[TrainingSetId]TrainingSetEntry),
		trainingSetMtx: &sync.RWMutex{},
		features:       make(map[FeatureId]FeatureEntry),
		featureMtx:     &sync.RWMutex{},
	}, nil
}

type MetadataError struct {
	Msg string
}

func (err *MetadataError) Error() string {
	return err.Msg
}

func (provider *LocalMemoryMetadata) SetTrainingSetMetadata(name, version string, entry TrainingSetEntry) error {
	provider.trainingSetMtx.Lock()
	defer provider.trainingSetMtx.Unlock()
	logger := provider.Logger.With("Name", name, "Version", version, "Entry", entry)
	logger.Debug("Setting training metadata")
	if err := entry.Valid(); err != nil {
		logger.Errorw("Invalid Metadata Entry", "Error", err)
		return err
	}
	provider.trainingSets[TrainingSetId{name, version}] = entry
	return nil
}

func (provider *LocalMemoryMetadata) TrainingSetMetadata(name, version string) (TrainingSetEntry, error) {
	provider.trainingSetMtx.RLock()
	defer provider.trainingSetMtx.RUnlock()
	logger := provider.Logger.With("Name", name, "Version", version, "Type", "Training set")
	logger.Debug("Retrieving training set metadata")
	entry, has := provider.trainingSets[TrainingSetId{name, version}]
	if !has {
		logger.Error("Training set metadata not found")
		return entry, &MetadataError{"Training set not found."}
	}
	return entry, nil
}

func (provider *LocalMemoryMetadata) SetFeatureMetadata(name, version string, entry FeatureEntry) error {
	provider.featureMtx.Lock()
	defer provider.featureMtx.Unlock()
	logger := provider.Logger.With("Name", name, "Version", version, "Entry", entry)
	logger.Debug("Setting feature metadata")
	provider.features[FeatureId{name, version}] = entry
	return nil
}

func (provider *LocalMemoryMetadata) FeatureMetadata(name, version string) (FeatureEntry, error) {
	provider.featureMtx.RLock()
	defer provider.featureMtx.RUnlock()
	logger := provider.Logger.With("Name", name, "Version", version, "Type", "Feature")
	logger.Debug("Retrieving Metadata")
	entry, has := provider.features[FeatureId{name, version}]
	if !has {
		logger.Error("Feature not found")
		return entry, &MetadataError{"Feature not found."}
	}
	return entry, nil
}
