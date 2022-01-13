package main

import (
	"fmt"

	"github.com/featureform/serving/dataset"
)

type featureId struct {
	Name    string
	Version string
}

type MemoryOnlineProvider struct {
	tables map[featureId]featureTable
}

func NewMemoryOnlineProvider() *MemoryOnlineProvider {
	return &MemoryOnlineProvider{
		tables: make(map[featureId]featureTable),
	}
}

type featureTable map[string]interface{}

func (table featureTable) Get(entity string) (*dataset.Feature, error) {
	val, has := table[entity]
	if !has {
		return nil, fmt.Errorf("Entity not found: %s", entity)
	}
	return dataset.NewFeature(val)
}

func (provider *MemoryOnlineProvider) ToKey(name, version string) map[string]string {
	return map[string]string{
		"name":    name,
		"version": version,
	}
}

func (provider *MemoryOnlineProvider) SetFeature(name, version, entity string, value interface{}) {
	id := featureId{name, version}
	features, has := provider.tables[id]
	if !has {
		features = make(featureTable)
		provider.tables[id] = features
	}
	features[entity] = value
}

func (provider *MemoryOnlineProvider) GetFeatureLookup(key map[string]string) (dataset.Lookup, error) {
	id := featureId{key["name"], key["version"]}
	lookup, has := provider.tables[id]
	if !has {
		return nil, fmt.Errorf("Feature not found: %s %s", key["name"], key["version"])
	}
	return lookup, nil
}
