// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/types"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

func (c *CreateTransformationRunner) Run() (types.CompletionWatcher, error) {
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
	OfflineType          pt.Type
	OfflineConfig        pc.SerializedConfig
	TransformationConfig provider.TransformationConfig
	IsUpdate             bool
}

func (c *CreateTransformationConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("could not marshal transformation config: %w", err)
	}
	return config, nil
}

func (c *CreateTransformationConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fmt.Errorf("could not unmarshal transformation config: %w", err)
	}
	return nil
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

func CreateTransformationRunnerFactory(config Config) (types.Runner, error) {
	transformationConfig := &CreateTransformationConfig{
		TransformationConfig: provider.TransformationConfig{
			Args: metadata.KubernetesArgs{},
		},
	}
	if err := transformationConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize create transformation config: %w", err)
	}
	offlineProvider, err := provider.Get(transformationConfig.OfflineType, transformationConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure offline provider: %w", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %w", err)
	}
	return &CreateTransformationRunner{
		Offline:              offlineStore,
		TransformationConfig: transformationConfig.TransformationConfig,
		IsUpdate:             transformationConfig.IsUpdate,
	}, nil

}
