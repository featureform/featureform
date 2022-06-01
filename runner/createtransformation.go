// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"
	metadata "github.com/featureform/serving/metadata"
	provider "github.com/featureform/serving/provider"
)

func (c *CreateTransformationRunner) Run() (CompletionWatcher, error) {
	done := make(chan interface{})
	transformationWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if !c.IsUpdate {
			if err := c.Offline.CreateTransformation(c.TransformationConfig); err != nil {
				transformationWatcher.EndWatch(err)
				return
			}
		} else {
			if err := c.Offline.UpdateTransformation(c.TransformationConfig); err != nil {
				transformationWatcher.EndWatch(err)
				return
			}
		}
		transformationWatcher.EndWatch(nil)
	}()
	return transformationWatcher, nil
}

type CreateTransformationConfig struct {
	OfflineType          provider.Type
	OfflineConfig        provider.SerializedConfig
	TransformationConfig provider.TransformationConfig
	IsUpdate             bool
}

type CreateTransformationRunner struct {
	Offline              provider.OfflineStore
	TransformationConfig provider.TransformationConfig
	IsUpdate             bool
}

func (c CreateTransformationRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{
		Name:    c.TransformationConfig.TargetTableID.Name,
		Variant: c.TransformationConfig.TargetTableID.Variant,
		Type:    provider.ProviderToMetadataResourceType[c.TransformationConfig.TargetTableID.Type],
	}
}

func (c CreateTransformationRunner) IsUpdateJob() bool {
	return c.IsUpdate
}

func (c *CreateTransformationConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *CreateTransformationConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return err
	}
	return nil
}

func CreateTransformationRunnerFactory(config Config) (Runner, error) {
	transformationConfig := &CreateTransformationConfig{}
	if err := transformationConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize create transformation config")
	}
	offlineProvider, err := provider.Get(transformationConfig.OfflineType, transformationConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure offline provider: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %v", err)
	}
	return &CreateTransformationRunner{
		Offline:              offlineStore,
		TransformationConfig: transformationConfig.TransformationConfig,
		IsUpdate:             transformationConfig.IsUpdate,
	}, nil

}
