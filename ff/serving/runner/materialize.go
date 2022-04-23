package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
)

const MAXIMUM_CHUNK_ROWS int64 = 1024
const WORKER_IMAGE string = "featureform/worker"

type JobCloud string

const (
	KubernetesMaterializeRunner JobCloud = "KUBERNETES"
	LocalMaterializeRunner      JobCloud = "LOCAL"
)

type MaterializeRunner struct {
	Online  provider.OnlineStore
	Offline provider.OfflineStore
	ID      provider.ResourceID
	Cloud   JobCloud
}

type WatcherMultiplex struct {
	CompletionList []CompletionWatcher
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

func (m MaterializeRunner) Run() (CompletionWatcher, error) {
	materialization, err := m.Offline.CreateMaterialization(m.ID)
	if err != nil {
		return nil, err
	}
	chunkSize := MAXIMUM_CHUNK_ROWS
	var numChunks int64
	numRows, err := materialization.NumRows()
	if err != nil {
		return nil, err
	}
	if numRows <= MAXIMUM_CHUNK_ROWS {
		chunkSize = numRows
		numChunks = 1
	} else if chunkSize == 0 {
		numChunks = 0
	} else if numRows > chunkSize {
		numChunks = numRows / chunkSize
		if chunkSize*numChunks < numRows {
			numChunks += 1
		}
	}
	config := &MaterializedChunkRunnerConfig{
		OnlineType:     m.Online.Type(),
		OfflineType:    m.Offline.Type(),
		OnlineConfig:   m.Online.Config(),
		OfflineConfig:  m.Offline.Config(),
		MaterializedID: materialization.ID(),
		ResourceID:     m.ID,
		ChunkSize:      chunkSize,
	}
	serializedConfig, err := config.Serialize()
	if err != nil {
		return nil, err
	}
	var cloudWatcher CompletionWatcher
	switch m.Cloud {
	case KubernetesMaterializeRunner:
		envVars := map[string]string{"NAME": string(COPY_TO_ONLINE), "CONFIG": string(serializedConfig)}
		kubernetesConfig := KubernetesRunnerConfig{
			EnvVars:  envVars,
			Image:    WORKER_IMAGE,
			NumTasks: int32(numChunks),
		}
		kubernetesRunner, err := NewKubernetesRunner(kubernetesConfig)
		if err != nil {
			return nil, err
		}
		cloudWatcher, err = kubernetesRunner.Run()
		if err != nil {
			return nil, err
		}
	case LocalMaterializeRunner:
		completionList := make([]CompletionWatcher, int(numChunks))
		for i := 0; i < int(numChunks); i++ {
			localRunner, err := Create(string(COPY_TO_ONLINE), serializedConfig)
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
		return nil, fmt.Errorf("no valid job cloud set")
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
		if err := m.Offline.DeleteMaterialization(materialization.ID()); err != nil {
			materializeWatcher.EndWatch(err)
			return
		}
		materializeWatcher.EndWatch(nil)
	}()
	return materializeWatcher, nil
}
