// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"

	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/types"
	"go.uber.org/zap"
)

type S3ImportDynamoDBRunner struct {
	Online   provider.OnlineStore
	Offline  provider.OfflineStore
	ID       provider.ResourceID
	VType    provider.ValueType
	IsUpdate bool
	Logger   *zap.SugaredLogger
}

func (r S3ImportDynamoDBRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{
		Name:    r.ID.Name,
		Variant: r.ID.Variant,
		Type:    provider.ProviderToMetadataResourceType[r.ID.Type],
	}
}

func (r S3ImportDynamoDBRunner) IsUpdateJob() bool {
	return r.IsUpdate
}

func (r S3ImportDynamoDBRunner) Run() (types.CompletionWatcher, error) {
	return nil, nil
}

type S3ImportDynamoDBRunnerConfig struct {
	OnlineType    pt.Type
	OfflineType   pt.Type
	OnlineConfig  pc.SerializedConfig
	OfflineConfig pc.SerializedConfig
	ResourceID    provider.ResourceID
	VType         provider.ValueTypeJSONWrapper
	Cloud         JobCloud
	IsUpdate      bool
}

func (cfg *S3ImportDynamoDBRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (cfg *S3ImportDynamoDBRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, cfg)
	if err != nil {
		return err
	}
	return nil
}

func S3ImportDynamoDBRunnerFactory(config Config) (types.Runner, error) {
	runnerConfig := &S3ImportDynamoDBRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize materialize runner config: %v", err)
	}
	onlineProvider, err := provider.Get(runnerConfig.OnlineType, runnerConfig.OnlineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure %s provider: %v", runnerConfig.OnlineType, err)
	}
	offlineProvider, err := provider.Get(runnerConfig.OfflineType, runnerConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure %s provider: %v", runnerConfig.OfflineType, err)
	}
	onlineStore, err := onlineProvider.AsOnlineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to online store: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %v", err)
	}
	return &S3ImportDynamoDBRunner{
		Online:   onlineStore,
		Offline:  offlineStore,
		ID:       runnerConfig.ResourceID,
		VType:    runnerConfig.VType.ValueType,
		IsUpdate: runnerConfig.IsUpdate,
		Logger:   logging.NewLogger("s3importer"),
	}, nil
}
