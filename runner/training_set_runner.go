// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"
	metadata "github.com/featureform/metadata"
	provider "github.com/featureform/provider"
)

type TrainingSetRunner struct {
	Offline  provider.OfflineStore
	Def      provider.TrainingSetDef
	IsUpdate bool
}

func (m TrainingSetRunner) Run() (CompletionWatcher, error) {
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
	OfflineType   provider.Type
	OfflineConfig provider.SerializedConfig
	Def           provider.TrainingSetDef
	IsUpdate      bool
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

func (c *TrainingSetRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {

		panic(fmt.Errorf("serialize: %w", err))
	}
	return config, nil
}

func (c *TrainingSetRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fmt.Errorf("deserialize: %w", err)
	}
	return nil
}

func TrainingSetRunnerFactory(config Config) (Runner, error) {
	runnerConfig := &TrainingSetRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize materialize chunk runner config: %v", err)
	}
	offlineProvider, err := provider.Get(runnerConfig.OfflineType, runnerConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure offline provider: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %v", err)
	}
	return &TrainingSetRunner{
		Offline:  offlineStore,
		Def:      runnerConfig.Def,
		IsUpdate: runnerConfig.IsUpdate,
	}, nil
}
