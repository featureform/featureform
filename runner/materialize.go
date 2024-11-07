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
	"time"

	"go.uber.org/zap"

	cfg "github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/kubernetes"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	vt "github.com/featureform/provider/types"
	"github.com/featureform/types"
)

var WORKER_IMAGE string = helpers.GetEnv("WORKER_IMAGE", "featureformenterprise/worker:latest")

type JobCloud string

const (
	KubernetesMaterializeRunner JobCloud = "KUBERNETES"
	LocalMaterializeRunner      JobCloud = "LOCAL"
)

type MaterializeRunner struct {
	Online   provider.OnlineStore
	Offline  provider.OfflineStore
	ID       provider.ResourceID
	VType    vt.ValueType
	IsUpdate bool
	Cloud    JobCloud
	Logger   *zap.SugaredLogger
	Options  provider.MaterializationOptions
}

func (m MaterializeRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{
		Name:    m.ID.Name,
		Variant: m.ID.Variant,
		Type:    provider.ProviderToMetadataResourceType[m.ID.Type],
	}
}

func (m MaterializeRunner) IsUpdateJob() bool {
	return m.IsUpdate
}

type WatcherMultiplex struct {
	CompletionList []types.CompletionWatcher
}

func (w WatcherMultiplex) Complete() bool {
	complete := true
	for _, completion := range w.CompletionList {
		complete = complete && completion.Complete()
	}
	return complete
}
func (w WatcherMultiplex) String() string {
	complete := 0
	for _, completion := range w.CompletionList {
		if completion.Complete() {
			complete += 1
		}
	}
	return fmt.Sprintf("%v complete out of %v", complete, len(w.CompletionList))
}
func (w WatcherMultiplex) Wait() error {
	for _, completion := range w.CompletionList {
		if err := completion.Wait(); err != nil {
			return err
		}
	}
	return nil
}
func (w WatcherMultiplex) Err() error {
	for _, completion := range w.CompletionList {
		if err := completion.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m MaterializeRunner) Run() (types.CompletionWatcher, error) {
	m.Logger.Infow("Starting Materialization Runner", "name", m.ID.Name, "variant", m.ID.Variant)
	var materialization provider.Materialization
	var err error
	// offline
	if m.IsUpdate {
		m.Logger.Infow("Updating Materialization", "name", m.ID.Name, "variant", m.ID.Variant)
		materialization, err = m.Offline.UpdateMaterialization(m.ID, m.Options)
	} else {
		m.Logger.Infow("Creating Materialization", "name", m.ID.Name, "variant", m.ID.Variant)
		materialization, err = m.Offline.CreateMaterialization(m.ID, m.Options)
	}
	if err != nil {
		return nil, err
	}

	// online
	if m.Online == nil {
		return m.handleNoOnlineStore()
	}
	return m.MaterializeToOnline(materialization)
}

func (m MaterializeRunner) MaterializeToOnline(materialization provider.Materialization) (types.CompletionWatcher, error) {
	// Create the vector similarity index prior to writing any values to the
	// inference store. This is currently only required for RediSearch, but other
	// vector databases allow for manual index configuration even if they support
	// autogeneration of indexes.
	if vectorType, ok := m.VType.(vt.VectorType); ok && vectorType.IsEmbedding {
		m.Logger.Infow("Creating Index", "name", m.ID.Name, "variant", m.ID.Variant)
		vectorStore, ok := m.Online.(provider.VectorStore)
		if !ok {
			return nil, fferr.NewInternalErrorf("cannot create index on non-vector store: %s", m.Online.Type().String())
		}
		// TODO handle exists error
		_, err := vectorStore.CreateIndex(m.ID.Name, m.ID.Variant, vectorType)
		if err != nil {
			return nil, err
		}
	}
	m.Logger.Infow("Creating Table", "name", m.ID.Name, "variant", m.ID.Variant)
	_, err := m.Online.CreateTable(m.ID.Name, m.ID.Variant, m.VType)
	if err != nil {
		_, isExistsErr := err.(*fferr.DatasetAlreadyExistsError)
		if !isExistsErr {
			// Unknown error, pass through
			return nil, err
		} else if isExistsErr && !m.IsUpdate {
			// Table exists
			return nil, fferr.NewDatasetAlreadyExistsError(m.ID.Name, m.ID.Variant, fmt.Errorf("table already exists"))
		}
		// Otherwise it was an exists error, but was an update, so should be ignored.
	}

	m.Logger.Infow("Getting number of chunks", "name", m.ID.Name, "variant", m.ID.Variant)
	numChunks, err := materialization.NumChunks()
	if err != nil {
		return nil, err
	}
	m.Logger.Infow("Creating chunks", "name", m.ID.Name, "variant", m.ID.Variant, "count", numChunks)
	config := &MaterializedChunkRunnerConfig{
		OnlineType:     m.Online.Type(),
		OfflineType:    m.Offline.Type(),
		OnlineConfig:   m.Online.Config(),
		OfflineConfig:  m.Offline.Config(),
		MaterializedID: materialization.ID(),
		ResourceID:     m.ID,
		Logger:         m.Logger,
	}
	var cloudWatcher types.CompletionWatcher
	switch m.Cloud {
	case KubernetesMaterializeRunner:
		serializedConfig, err := config.Serialize()
		if err != nil {
			return nil, err
		}
		pandas_image := cfg.GetPandasRunnerImage()
		envVars := map[string]string{"NAME": string(COPY_TO_ONLINE), "CONFIG": string(serializedConfig), "PANDAS_RUNNER_IMAGE": pandas_image}
		kubernetesConfig := kubernetes.KubernetesRunnerConfig{
			JobPrefix: "materialize",
			EnvVars:   envVars,
			Image:     WORKER_IMAGE,
			NumTasks:  int32(numChunks),
			Resource:  metadata.ResourceID{Name: m.ID.Name, Variant: m.ID.Variant, Type: provider.ProviderToMetadataResourceType[m.ID.Type]},
		}
		kubernetesRunner, err := kubernetes.NewKubernetesRunner(kubernetesConfig)
		if err != nil {
			return nil, err
		}
		cloudWatcher, err = kubernetesRunner.Run()
		if err != nil {
			return nil, err
		}
	case LocalMaterializeRunner:
		m.Logger.Infow("Making Local Runner", "name", m.ID.Name, "variant", m.ID.Variant)
		completionList := make([]types.CompletionWatcher, int(numChunks))
		for i := 0; i < int(numChunks); i++ {
			m.Logger.Infow("Creating materialization chunk", "name", m.ID.Name, "variant", m.ID.Variant, "chunkIndex", i)
			config.ChunkIdx = i
			serializedChunkConfig, err := config.Serialize()
			if err != nil {
				return nil, err
			}
			localRunner, err := Create(COPY_TO_ONLINE, serializedChunkConfig)
			if err != nil {
				return nil, err
			}
			watcher, err := localRunner.Run()
			if err != nil {
				return nil, err
			}
			completionList[i] = watcher
		}
		cloudWatcher = WatcherMultiplex{completionList}
	default:
		return nil, fferr.NewInternalError(fmt.Errorf("no valid job cloud set"))
	}
	done := make(chan interface{})
	materializeWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if err := cloudWatcher.Wait(); err != nil {
			materializeWatcher.EndWatch(err)
			return
		}
		materializeWatcher.EndWatch(nil)
	}()
	return materializeWatcher, nil
}

func (m MaterializeRunner) handleNoOnlineStore() (types.CompletionWatcher, error) {
	m.Logger.Infow("No Online Store, skipping materialization", "name", m.ID.Name, "variant", m.ID.Variant)
	done := make(chan interface{})
	materializeWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		materializeWatcher.EndWatch(nil)
	}()
	return materializeWatcher, nil
}

type MaterializedRunnerConfig struct {
	OnlineType    pt.Type
	OfflineType   pt.Type
	OnlineConfig  pc.SerializedConfig
	OfflineConfig pc.SerializedConfig
	ResourceID    provider.ResourceID
	VType         vt.ValueTypeJSONWrapper
	Cloud         JobCloud
	IsUpdate      bool
	Options       provider.MaterializationOptions
}

type MaterializedRunnerConfigJSON struct {
	OnlineType    pt.Type                    `json:"OnlineType"`
	OfflineType   pt.Type                    `json:"OfflineType"`
	OnlineConfig  pc.SerializedConfig        `json:"OnlineConfig"`
	OfflineConfig pc.SerializedConfig        `json:"OfflineConfig"`
	ResourceID    provider.ResourceID        `json:"ResourceID"`
	VType         vt.ValueTypeJSONWrapper    `json:"VType"`
	Cloud         JobCloud                   `json:"Cloud"`
	IsUpdate      bool                       `json:"IsUpdate"`
	Options       MaterializationOptionsJSON `json:"Options"`
}

type MaterializationOptionsJSON struct {
	Output                  filestore.FileType                `json:"Output"`
	ShouldIncludeHeaders    bool                              `json:"ShouldIncludeHeaders"`
	MaxJobDuration          time.Duration                     `json:"MaxJobDuration"`
	JobName                 string                            `json:"JobName"`
	ResourceSnowflakeConfig *metadata.ResourceSnowflakeConfig `json:"ResourceSnowflakeConfig,omitempty"`
	Schema                  json.RawMessage                   `json:"Schema"`
}

func (m *MaterializedRunnerConfig) Serialize() (Config, error) {
	schemaBytes, err := m.Options.Schema.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize Schema in MaterializationOptions: %v", err)
	}

	data := MaterializedRunnerConfigJSON{
		OnlineType:    m.OnlineType,
		OfflineType:   m.OfflineType,
		OnlineConfig:  m.OnlineConfig,
		OfflineConfig: m.OfflineConfig,
		ResourceID:    m.ResourceID,
		VType:         m.VType,
		Cloud:         m.Cloud,
		IsUpdate:      m.IsUpdate,
		Options: MaterializationOptionsJSON{
			Output:                  m.Options.Output,
			ShouldIncludeHeaders:    m.Options.ShouldIncludeHeaders,
			MaxJobDuration:          m.Options.MaxJobDuration,
			JobName:                 m.Options.JobName,
			ResourceSnowflakeConfig: m.Options.ResourceSnowflakeConfig,
			Schema:                  json.RawMessage(schemaBytes),
		},
	}

	configBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize MaterializedRunnerConfig: %v", err)
	}

	return configBytes, nil
}

func (config *MaterializedRunnerConfig) Deserialize(data []byte) error {
	var intermediate MaterializedRunnerConfigJSON
	err := json.Unmarshal(data, &intermediate)
	if err != nil {
		return fmt.Errorf("failed to deserialize MaterializedRunnerConfig: %v", err)
	}

	config.OnlineType = intermediate.OnlineType
	config.OfflineType = intermediate.OfflineType
	config.OnlineConfig = intermediate.OnlineConfig
	config.OfflineConfig = intermediate.OfflineConfig
	config.ResourceID = intermediate.ResourceID
	config.VType = intermediate.VType
	config.Cloud = intermediate.Cloud
	config.IsUpdate = intermediate.IsUpdate

	options := provider.MaterializationOptions{}
	options.Output = intermediate.Options.Output
	options.ShouldIncludeHeaders = intermediate.Options.ShouldIncludeHeaders
	options.MaxJobDuration = intermediate.Options.MaxJobDuration
	options.JobName = intermediate.Options.JobName
	options.ResourceSnowflakeConfig = intermediate.Options.ResourceSnowflakeConfig

	var schema provider.ResourceSchema
	err = schema.Deserialize(intermediate.Options.Schema)
	if err != nil {
		return fmt.Errorf("failed to deserialize Schema in MaterializationOptions: %v", err)
	}
	options.Schema = schema

	config.Options = options

	return nil
}

func MaterializeRunnerFactory(config Config) (types.Runner, error) {
	runnerConfig := &MaterializedRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, err
	}
	var onlineStore provider.OnlineStore
	if runnerConfig.OnlineType != pt.NONE {
		onlineProvider, err := provider.Get(runnerConfig.OnlineType, runnerConfig.OnlineConfig)
		if err != nil {
			return nil, err
		}
		onlineStore, err = onlineProvider.AsOnlineStore()
		if err != nil {
			return nil, err
		}
	}
	offlineProvider, err := provider.Get(runnerConfig.OfflineType, runnerConfig.OfflineConfig)
	if err != nil {
		return nil, err
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	return &MaterializeRunner{
		Online:   onlineStore, // This can be nil if onlineProvider is nil
		Offline:  offlineStore,
		ID:       runnerConfig.ResourceID,
		VType:    runnerConfig.VType.ValueType,
		IsUpdate: runnerConfig.IsUpdate,
		Cloud:    runnerConfig.Cloud,
		Logger:   logging.NewLogger("materializer").SugaredLogger,
		Options:  runnerConfig.Options,
	}, nil
}
