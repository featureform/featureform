package runner

import (
	"fmt"
	"sync"
	provider "github.com/featureform/serving/provider"
)

type MaterializedChunkRunnerConfig struct {
	OnlineType provider.Type
	OfflineType provider.Type
	OnlineConfig provider.SerializedConfig
	OfflineConfig provider.SerializedConfig
	MaterializedID provider.MaterializationID
	ResourceID provider.ResourceID
	ChunkSize int
	ChunkIdx int
}

func (m MaterializedChunkRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (m MaterializedChunkRunnerConfig) Deserialize(Config) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return err
	}
	return nil
}

func MaterializedChunkRunnerFactory(config Config) (Runner, error) {
	mConf := &MaterializedChunkRunnerConfig{}
	if err := materializedConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize materialize chunk runner config: %v", err)
	}
	onlineProvider, err := provider.Get(mConf.OnlineType, mConf.OnlineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure online provider: %v", err)
	}
	offlineProvider, err := provider.Get(mConf.OfflineType, mConf.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure offline provider: %v", err)
	}
	onlineStore, err := onlineProvider.AsOnlineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to online store: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("%T cannot be used as an OfflineStore", provider)
	}
	materialization, err := offlineStore.GetMaterialization(mConf.MaterializedID)
	if err != nil {
		return nil, fmt.Errorf("cannot get materialization: %v", err)
	}
	if mConf.ChunkSize*mConf.ChunkIdx > materialization.NumRows() {
		return nil, fmt.Errorf("chunk runner starts after end of materialization rows")
	}
	table, err := offlineStore.GetTable(mConf.ResourceID.Name, mConf.ResourceID.Variant)
	if err == &provider.TableNotFound{mConf.ResourceID.Name, mConf.ResourceID.Variant} {
		table, err := onlineStore.CreateTable(mConf.ResourceID.Name, mConf.ResourceID.Variant)
		if err != nil {
			return nil, fmt.Errorf("error creating online table: %v", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("error getting online table: %v", err)
	}

	return &MaterializedChunkRunner{
		Materialized: materialization,
		Table: table,
		ChunkSize: mConf.ChunkSize,
		ChunkIdx: mConf.ChunkIdx,
	}
}

func init() {
	RegisterFactory("COPY", MaterializedChunkRunnerFactory)
}

type Runner interface {
	Run() (CompletionWatcher, error)
}

type MaterializedChunkRunner struct {
	Materialized provider.Materialization
	Table        provider.OnlineStoreTable
	ChunkSize    int
	ChunkIdx     int
}

type CompletionWatcher interface {
	Complete() bool
	String() string
	Wait() error
	Err() error
}

type MaterializedFeatures interface {
	NumRows() (int, error)
	IterateSegment(begin int, end int) (FeatureIterator, error)
}

type OnlineTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
}

type FeatureIterator interface {
	Next() bool
	Err() error
	Entity() string
	Value() interface{}
}

type ResultSync struct {
	err  error
	done bool
	mu   sync.RWMutex
}

func (m *MaterializedChunkRunner) Run() (CompletionWatcher, error) {
	done := make(chan interface{})
	jobWatcher := &CopyCompletionWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if m.ChunkSize == 0 {
			jobWatcher.EndWatch(nil)
			return
		}
		numRows, err := m.Materialized.NumRows()
		if err != nil {
			jobWatcher.EndWatch(err)
			return
		}
		if numRows == 0 {
			jobWatcher.EndWatch(nil)
			return
		}

		rowStart := m.ChunkIdx * m.ChunkSize
		rowEnd := rowStart + m.ChunkSize
		if rowEnd > numRows {
			rowEnd = numRows
		}
		it, err := m.Materialized.IterateSegment(rowStart, rowEnd)
		if err != nil {
			jobWatcher.EndWatch(err)
			return
		}
		for it.Next() {
			value := it.Value()
			entity := it.Entity()
			err := m.Table.Set(entity, value)
			if err != nil {
				jobWatcher.EndWatch(err)
				return
			}
		}
		if err = it.Err(); err != nil {
			jobWatcher.EndWatch(err)
			return
		}
		jobWatcher.EndWatch(nil)
	}()
	return jobWatcher, nil
}

func (c *CopyCompletionWatcher) EndWatch(err error) {
	c.ResultSync.DoneWithError(err)
	close(c.DoneChannel)
}

func (r *ResultSync) Done() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.done
}

func (r *ResultSync) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.err
}

func (r *ResultSync) DoneWithError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
	r.done = true
}

type CopyCompletionWatcher struct {
	ResultSync  *ResultSync
	DoneChannel chan interface{}
}

func (m *CopyCompletionWatcher) Err() error {
	return m.ResultSync.Err()
}

func (m *CopyCompletionWatcher) Wait() error {
	<-m.DoneChannel
	return m.ResultSync.Err()
}

func (m *CopyCompletionWatcher) Complete() bool {
	return m.ResultSync.Done()
}

func (m *CopyCompletionWatcher) String() string {
	done := m.ResultSync.Done()
	err := m.ResultSync.Err()
	if err != nil {
		return fmt.Sprintf("Job failed with error: %v", err)
	}
	if !done {
		return "Job still running."
	}
	return "Job completed succesfully."
}
