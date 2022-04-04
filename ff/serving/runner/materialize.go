package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
	"math"
)

const MAXIMUM_CHUNK_ROWS int64 = 1024
const WORKER_IMAGE string = "featureform/worker"

type JobCloud string

const (
	Kubernetes JobCloud = "KUBERNETES"
)

type MaterializeRunner struct {
	Online  provider.OnlineStore
	Offline provider.OfflineStore
	ID      provider.ResourceID
	Cloud   JobCloud
}

func (m MaterializeRunner) Run() (CompletionWatcher, error) {
	materialization, err := m.Offline.CreateMaterialization(m.ID)
	if err != nil {
		return nil, err
	}
	chunkSize := MAXIMUM_CHUNK_ROWS
	numRows, err := materialization.NumRows()
	if err != nil {
		return nil, err
	}
	if numRows < MAXIMUM_CHUNK_ROWS {
		chunkSize = numRows
	}
	numChunks := int64(math.Ceil(float64(numRows) / float64(chunkSize)))
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
	case Kubernetes:
		envVars := map[string]string{"NAME": "COPY", "CONFIG": string(serializedConfig)}
		kubernetesConfig := KubernetesRunnerConfig{
			envVars:  envVars,
			image:    WORKER_IMAGE,
			numTasks: int32(numChunks),
		}
		kubernetesRunner, err := NewKubernetesRunner(kubernetesConfig)
		if err != nil {
			return nil, err
		}
		cloudWatcher, err = kubernetesRunner.Run()
		if err != nil {
			return nil, err
		}
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
