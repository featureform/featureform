package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
)

const MAXIMUM_CHUNK_ROWS = 1024

type MemoryMaterializeRunner struct {
	Online provider.OnlineStore
	Offline provider.OfflineStore
	ID provider.ResourceID
}

type MemoryMaterializeCompletionWatcher struct {
	Watchers []CopyCompletionWatcher
}

type KubernetesMaterializeRunner struct {
	Online provider.OnlineStore
	Offline provider.OfflineStore
	ID provider.ResourceID
}

func (m MemoryMaterializeRunner) Run() (CompletionWatcher, error) {
	materialization, err := m.Offline.CreateMaterialization(m.ID)
	if err != nil {
		return nil, err
	}
	onlineConfig, err := m.Online.Config().Serialized()
	if err != nil {
		return nil, err
	}
	offlineConfig, err := m.Offline.Config().Serialized()
	if err != nil {
		return nil, err
	}
	onlineType := m.Online.Type()
	offlineType := m.Offline.Type()
	chunkSize := MAXIMUM_CHUNK_ROWS
	if materialization.NumRows() < MAXIMUM_CHUNK_ROWS {
		chunkSize = materialization.NumRows()
	}
	numChunks := math.Ceil(materialization.NumRows() / chunkSize)
	watchers := make([]CopyCompletionWatcher, numChunks)
	for i := range numChunks {
		config := &MaterializedChunkRunnerConfig{
			OnlineType: onlineType,
			OfflineType: offlineType,
			OnlineConfig: onlineConfig,
			OfflineConfig: offlineConfig,
			MaterializedID: materialization.ID(),
			ResourceID: m.ID,
			ChunkSize: chunkSize,
		}
		serialConfig, err := config.Serialize()
		if err != nil {
			return err
		}
		copyRunner, err := Create("COPY", serialConfig)
		if err != nil {
			return err
		}
		copyRunner.SetIndex(i)
		watcher, err := copyRunner.Run()
		if err != nil {
			return err
		}
		watchers[i] = watcher
	}
	return &MemoryMaterializeCompletionWatcher{watchers}
}

func (m *MemoryMaterializeCompletionWatcher) Complete() bool {
	complete := true
	for watcher := range m.watchers {
		complete &= watcher.Complete()
	}
	return complete
}

func (m *MemoryMaterializeCompletionWatcher) String() string {
	numComplete := 0
	numErr := 0
	for watcher := range m.watchers {
		if watcher.Complete() {
			numComplete += 1
		}
		if watcher.Err() != nil {
			numErr += 1
		}
	}
	return fmt.Printf("%d out of %d copy runners complete. %d failed.", numComplete, len(m.watchers), numErr)
}

func (m *MemoryMaterializeCompletionWatcher) Wait() error {
	for watcher := range m.watchers {
		err := watcher.Wait()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryMaterializeCompletionWatcher) Err() error {
	for watcher := range m.watchers {
		if watcher.Err() != nil{
			return watcher.Err()
		}
	}
	return nil
}
