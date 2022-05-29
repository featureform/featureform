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
	registerFileWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if err := m.Offline.RegisterPrimaryFromSourceTable(m.ResourceID, m.SourceTableName); err != nil {
			registerFileWatcher.EndWatch(err)
			return
		}
		registerFileWatcher.EndWatch(nil)
	}()
	return registerFileWatcher, nil
}

type RegisterSourceConfig struct {
	OfflineType     provider.Type
	OfflineConfig   provider.SerializedConfig
	ResourceID      provider.ResourceID
	SourceTableName string
}

type RegisterSourceRunner struct {
	Offline         provider.OfflineStore
	ResourceID      provider.ResourceID
	SourceTableName string
}

func (c *RegisterSourceRunner) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *RegisterSourceRunner) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return err
	}
	return nil
}

func RegisterSourceRunnerFactory(config Config) (Runner, error) {
	registerConfig := &RegisterSourceConfig{}
	if err := registerConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize register file config")
	}
	offlineProvider, err := provider.Get(registerConfig.OfflineType, registerConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure offline provider: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %v", err)
	}
	return &RegisterSourceRunner{
		Offline:         offlineStore,
		ResourceID:      registerConfig.ResourceID,
		SourceTableName: registerConfig.SourceTableName,
	}, nil

}
