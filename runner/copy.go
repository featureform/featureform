// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/types"
	"go.uber.org/zap"
)

// We buffer the channel responsible for passing records to the goroutines that will write
// to the inference store avoid blocking the writer loop below; after some initial testing,
// 1M records seems to be a good buffer size
const resourceRecordBufferSize = 1_000_000

// We create a pool of goroutines per materialization chunk runner to make Set requests to
// the inference store asynchronously; after some initial testing, 1000 workers appears to
// offer the best results
const workerPoolSize = 1000

type IndexRunner interface {
	types.Runner
	SetIndex(index int) error
}

type MaterializedChunkRunner struct {
	Materialized provider.Materialization
	Table        provider.OnlineStoreTable
	Store        provider.OnlineStore
	ChunkSize    int64
	ChunkIdx     int64
}

type ResultSync struct {
	err  error
	done bool
	mu   sync.RWMutex
}

func (m *MaterializedChunkRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (m *MaterializedChunkRunner) IsUpdateJob() bool {
	return false
}

func (m *MaterializedChunkRunner) Run() (types.CompletionWatcher, error) {
	done := make(chan interface{})
	jobWatcher := &SyncWatcher{
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
			jobWatcher.EndWatch(fmt.Errorf("failed to get number of rows: %w", err))
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
			jobWatcher.EndWatch(fmt.Errorf("failed to create iterator: %w", err))
			return
		}
		// The logic for the below code (i.e. the channel, goroutines, wait group and iteration loop)
		// is as follows:
		// 1. create a record channel and an error channel; the record channel will be written to by
		// the iterator loop; the error channel will be written to by one or more of the goroutines
		// if they happen to encounter an error while trying to set a value to the inference store
		// 2. create a wait group that will be used to wait for the goroutines to finish processing
		// all of the records in the channel before continuing execution of the Run method
		// 3. create a set/pool of goroutines that can process records from the channel asynchronously
		// to avoid unnecessary waiting/blocking; creating the workers prior to writing to the channel
		// means that as messages are sent to the channel, the workers will be ready to process them,
		// which means we don't have to wait for the iterator to be consumed before the work of persisting
		// records in the inference store begins
		// 4. create an iteration loop that will completely consume the iterator and write all records
		// to the channel
		ch := make(chan provider.ResourceRecord, resourceRecordBufferSize)
		// Using a buffered error channel to capture any errors that may occur while trying to
		// set values; buffering the error channel prevents the goroutines from blocking when
		// trying to write to the channel.
		errCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(workerPoolSize)
		// Create a set goroutines that can wait for the inference store to response asynchronously
		for idx := 0; idx < workerPoolSize; idx++ {
			go func() {
				defer wg.Done()
				for record := range ch {
					if err := m.Table.Set(record.Entity, record.Value); err != nil {
						select {
						case errCh <- fmt.Errorf("could not set value to table: %w", err):
						default:
						}
					}
				}
			}()
		}
		var chanErr error
		for it.Next() {
			select {
			case chanErr = <-errCh:
			case ch <- it.Value():
			default:
			}
			if chanErr != nil {
				break
			}
		}
		close(ch)
		wg.Wait()
		// Guarantees to show the first error written to the error channel
		// and then checks the error one last time. This check covers the
		// edge case in which the iterator has written all of the records
		// to the channel prior to any goroutine writing to the error channel
		// (e.g. an error occurs near the end of the iterator's loop, in which
		// case the loop breaks before every reading from the error channel).
		if chanErr == nil {
			select {
			case chanErr = <-errCh:
			default:
			}
		}
		close(errCh)
		if chanErr != nil {
			jobWatcher.EndWatch(fmt.Errorf("error encountered by inference store writer goroutine: %w", chanErr))
			return
		}
		if err = it.Err(); err != nil {
			jobWatcher.EndWatch(fmt.Errorf("iteration failed with error: %w", err))
			return
		}
		err = it.Close()
		if err != nil {
			jobWatcher.EndWatch(fmt.Errorf("failed to close iterator: %w", err))
			return
		}
		err = m.Store.Close()
		if err != nil {
			jobWatcher.EndWatch(fmt.Errorf("failed to close Online Store: %w", err))
			return
		}
		jobWatcher.EndWatch(nil)
	}()
	return jobWatcher, nil
}

func (m *MaterializedChunkRunner) SetIndex(index int) error {
	m.ChunkIdx = int64(index)
	return nil
}

func (c *SyncWatcher) EndWatch(err error) {
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

type SyncWatcher struct {
	ResultSync  *ResultSync
	DoneChannel chan interface{}
}

func (m *SyncWatcher) Err() error {
	return m.ResultSync.Err()
}

func (m *SyncWatcher) Wait() error {
	<-m.DoneChannel
	return m.ResultSync.Err()
}

func (m *SyncWatcher) Complete() bool {
	return m.ResultSync.Done()
}

func (m *SyncWatcher) String() string {
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

type MaterializedChunkRunnerConfig struct {
	OnlineType     pt.Type
	OfflineType    pt.Type
	OnlineConfig   pc.SerializedConfig
	OfflineConfig  pc.SerializedConfig
	MaterializedID provider.MaterializationID
	ResourceID     provider.ResourceID
	ChunkSize      int64
	ChunkIdx       int64
	IsUpdate       bool
	Logger         *zap.SugaredLogger
}

func (m *MaterializedChunkRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (m *MaterializedChunkRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return err
	}
	return nil
}

func MaterializedChunkRunnerFactory(config Config) (types.Runner, error) {
	runnerConfig := &MaterializedChunkRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize materialize chunk runner config: %v", err)
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
	materialization, err := offlineStore.GetMaterialization(runnerConfig.MaterializedID)
	if err != nil {
		return nil, fmt.Errorf("cannot get materialization: %v", err)
	}
	numRows, err := materialization.NumRows()
	if err != nil {
		return nil, fmt.Errorf("cannot get materialization num rows: %v", err)
	}
	if runnerConfig.ChunkSize*runnerConfig.ChunkIdx > numRows {
		return nil, fmt.Errorf("chunk runner starts after end of materialization rows")
	}
	table, err := onlineStore.GetTable(runnerConfig.ResourceID.Name, runnerConfig.ResourceID.Variant)
	if err != nil {
		return nil, fmt.Errorf("error getting online table: %v", err)
	}
	return &MaterializedChunkRunner{
		Materialized: materialization,
		Table:        table,
		Store:        onlineStore,
		ChunkSize:    runnerConfig.ChunkSize,
		ChunkIdx:     runnerConfig.ChunkIdx,
	}, nil
}
