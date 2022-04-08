package runner

import (
	"encoding/json"
	"fmt"
	provider "github.com/featureform/serving/provider"
)

type TrainingSetRunner struct {
	Offline provider.OfflineStore
	Def     provider.TrainingSetDef
}

func (m TrainingSetRunner) Run() (CompletionWatcher, error) {
	done := make(chan interface{})
	trainingSetWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if err := m.Offline.CreateTrainingSet(m.Def); err != nil {
			trainingSetWatcher.EndWatch(err)
			return
		}
		trainingSetWatcher.EndWatch(nil)
	}()
	return trainingSetWatcher, nil
}

type CreateTrainingSetRunnerConfig struct {
	OfflineType   provider.Type
	OfflineConfig provider.SerializedConfig
	Def           provider.TrainingSetDef
}

func (c *CreateTrainingSetRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *CreateTrainingSetRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return err
	}
	return nil
}

func CreateTrainingSetRunnerFactory(config Config) (Runner, error) {
	runnerConfig := &CreateTrainingSetRunnerConfig{}
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
		Offline: offlineStore,
		Def:     runnerConfig.Def,
	}, nil
}
