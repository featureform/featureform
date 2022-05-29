// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"
	provider "github.com/featureform/serving/provider"
)

func (m RegisterSourceRunner) Run() (CompletionWatcher, error) {
	done := make(chan interface{})
	transformationWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if !IsUpdate {
			if err := m.Offline.CreateTransformation(m.TransformationConfig); err != nil {
				transformationWatcher.EndWatch(err)
				return
			}
		} else {
			if err := m.Offline.UpdateTransformation(m.TransformationConfig); err != nil {
				transformationWatcher.EndWatch(err)
				return
			}
		}
		transformationWatcher.EndWatch(nil)
	}()
	return trainingSetWatcher, nil
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

func (c *CreateTransformationRunner) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *CreateTransformationRunner) Deserialize(config Config) error {
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
