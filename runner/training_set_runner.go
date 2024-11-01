// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package runner

import (
	"encoding/json"
	"fmt"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/types"
)

type TrainingSetRunner struct {
	Offline  provider.OfflineStore
	Def      provider.TrainingSetDef
	IsUpdate bool
}

func (m TrainingSetRunner) Run() (types.CompletionWatcher, error) {
	done := make(chan interface{})
	trainingSetWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if !m.IsUpdate {
			if err := m.Offline.CreateTrainingSet(m.Def); err != nil {
				trainingSetWatcher.EndWatch(err)
				return
			}
		} else {
			if err := m.Offline.UpdateTrainingSet(m.Def); err != nil {
				trainingSetWatcher.EndWatch(err)
				return
			}
		}
		trainingSetWatcher.EndWatch(nil)
	}()
	return trainingSetWatcher, nil
}

type TrainingSetRunnerConfig struct {
	OfflineType   pt.Type
	OfflineConfig pc.SerializedConfig
	Def           provider.TrainingSetDef
	IsUpdate      bool
}

type TrainingSetRunnerConfigJSON struct {
	OfflineType   pt.Type                     `json:"OfflineType"`
	OfflineConfig pc.SerializedConfig         `json:"OfflineConfig"`
	Def           provider.TrainingSetDefJSON `json:"Def"`
	IsUpdate      bool                        `json:"IsUpdate"`
}

func (t TrainingSetRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{
		Name:    t.Def.ID.Name,
		Variant: t.Def.ID.Variant,
		Type:    provider.ProviderToMetadataResourceType[t.Def.ID.Type],
	}
}

func (t TrainingSetRunner) IsUpdateJob() bool {
	return t.IsUpdate
}

func (c *TrainingSetRunnerConfig) Serialize() ([]byte, error) {
	mapSourceMapping := func(sourceMapping provider.SourceMapping) (provider.SourceMappingJSON, error) {
		mapping := provider.SourceMappingJSON{
			Template:            sourceMapping.Template,
			Source:              sourceMapping.Source,
			ProviderType:        sourceMapping.ProviderType,
			ProviderConfig:      sourceMapping.ProviderConfig,
			TimestampColumnName: sourceMapping.TimestampColumnName,
		}

		if sourceMapping.Location != nil {
			locationData, locationDataErr := sourceMapping.Location.Serialize()
			if locationDataErr != nil {
				return provider.SourceMappingJSON{}, fmt.Errorf("failed to serialize Location: %v", locationDataErr)
			}

			mapping.Location = json.RawMessage(locationData)
		}

		return mapping, nil
	}

	labelSourceMappingJSON, err := mapSourceMapping(c.Def.LabelSourceMapping)
	if err != nil {
		return nil, err
	}

	featureSourceMappingsJSON := make([]provider.SourceMappingJSON, len(c.Def.FeatureSourceMappings))
	for i, mapping := range c.Def.FeatureSourceMappings {
		featureSourceMappingJSON, err := mapSourceMapping(mapping)
		if err != nil {
			return nil, err
		}
		featureSourceMappingsJSON[i] = featureSourceMappingJSON
	}

	data := TrainingSetRunnerConfigJSON{
		OfflineType:   c.OfflineType,
		OfflineConfig: c.OfflineConfig,
		Def: provider.TrainingSetDefJSON{
			ID:                      c.Def.ID,
			Label:                   c.Def.Label,
			LabelSourceMapping:      labelSourceMappingJSON,
			Features:                c.Def.Features,
			FeatureSourceMappings:   featureSourceMappingsJSON,
			LagFeatures:             c.Def.LagFeatures,
			ResourceSnowflakeConfig: c.Def.ResourceSnowflakeConfig,
		},
		IsUpdate: c.IsUpdate,
	}

	configBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize TrainingSetRunnerConfig: %v", err)
	}

	return configBytes, nil
}

func (c *TrainingSetRunnerConfig) Deserialize(config []byte) error {
	var intermediate TrainingSetRunnerConfigJSON
	err := json.Unmarshal(config, &intermediate)
	if err != nil {
		return fmt.Errorf("failed to deserialize TrainingSetRunnerConfig: %v", err)
	}

	mapSourceMapping := func(sourceMappingJSON provider.SourceMappingJSON) (provider.SourceMapping, error) {
		mapping := provider.SourceMapping{
			Template:            sourceMappingJSON.Template,
			Source:              sourceMappingJSON.Source,
			ProviderType:        sourceMappingJSON.ProviderType,
			ProviderConfig:      sourceMappingJSON.ProviderConfig,
			TimestampColumnName: sourceMappingJSON.TimestampColumnName,
		}

		if sourceMappingJSON.Location != nil {
			var location pl.Location

			var jsonLoc pl.JSONLocation
			err := json.Unmarshal(sourceMappingJSON.Location, &jsonLoc)
			if err != nil {
				return provider.SourceMapping{}, fmt.Errorf("failed to unmarshal Location: %v", err)
			}

			switch jsonLoc.LocationType {
			case string(pl.SQLLocationType):
				location = &pl.SQLLocation{}
			case string(pl.FileStoreLocationType):
				location = &pl.FileStoreLocation{}
			case string(pl.CatalogLocationType):
				location = &pl.CatalogLocation{}
			default:
				return provider.SourceMapping{}, fmt.Errorf("unknown location type: %s", jsonLoc.LocationType)
			}

			err = location.Deserialize(sourceMappingJSON.Location)
			if err != nil {
				return provider.SourceMapping{}, fmt.Errorf("failed to deserialize Location: %v", err)
			}
			mapping.Location = location
		}

		return mapping, nil
	}

	labelSourceMapping, err := mapSourceMapping(intermediate.Def.LabelSourceMapping)
	if err != nil {
		return err
	}

	featureSourceMappings := make([]provider.SourceMapping, len(intermediate.Def.FeatureSourceMappings))
	for i, mappingJSON := range intermediate.Def.FeatureSourceMappings {
		mapping, err := mapSourceMapping(mappingJSON)
		if err != nil {
			return err
		}
		featureSourceMappings[i] = mapping
	}

	c.OfflineType = intermediate.OfflineType
	c.OfflineConfig = intermediate.OfflineConfig
	c.Def = provider.TrainingSetDef{
		ID:                      intermediate.Def.ID,
		Label:                   intermediate.Def.Label,
		LabelSourceMapping:      labelSourceMapping,
		Features:                intermediate.Def.Features,
		FeatureSourceMappings:   featureSourceMappings,
		LagFeatures:             intermediate.Def.LagFeatures,
		ResourceSnowflakeConfig: intermediate.Def.ResourceSnowflakeConfig,
	}
	c.IsUpdate = intermediate.IsUpdate

	return nil
}

func TrainingSetRunnerFactory(config Config) (types.Runner, error) {
	runnerConfig := &TrainingSetRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, err
	}
	offlineProvider, err := provider.Get(runnerConfig.OfflineType, runnerConfig.OfflineConfig)
	if err != nil {
		return nil, err
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	return &TrainingSetRunner{
		Offline:  offlineStore,
		Def:      runnerConfig.Def,
		IsUpdate: runnerConfig.IsUpdate,
	}, nil
}
