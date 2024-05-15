// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"

	"github.com/featureform/fferr"
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
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (c *CreateTransformationConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fferr.NewInternalError(err)
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
		return nil, err
	}
	offlineProvider, err := provider.Get(transformationConfig.OfflineType, transformationConfig.OfflineConfig)
	if err != nil {
		return nil, err
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	return &CreateTransformationRunner{
		Offline:              offlineStore,
		TransformationConfig: transformationConfig.TransformationConfig,
		IsUpdate:             transformationConfig.IsUpdate,
	}, nil

}
