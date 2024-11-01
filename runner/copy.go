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
	"sync"

	"github.com/featureform/logging"

	"github.com/featureform/fferr"
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
// the inference store asynchronously; after some initial testing, 500 workers appears to
// offer the best results
const workerPoolSize = 500

// This breaks tests currently and may have unintended consequences. More work to be done.
const providerCachingEnabled = false

type IndexRunner interface {
	types.Runner
	SetIndex(index int) error
}

type MaterializedChunkRunner struct {
	Materialized provider.Materialization
	Table        provider.OnlineStoreTable
	Store        provider.OnlineStore
	ChunkIdx     int
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
	logger := logging.NewLogger("Copy_to_Online")
	done := make(chan interface{})
	jobWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		it, err := m.Materialized.IterateChunk(m.ChunkIdx)
		if err != nil {
			jobWatcher.EndWatch(err)
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
		batchTable, supportsBatch := m.Table.(provider.BatchOnlineTable)
		var setterFn func()
		if supportsBatch {
			logger.Debugw("using batch table", "table", m.Table)
			maxBatch, err := batchTable.MaxBatchSize()
			if maxBatch <= 0 {
				logger.Errorf("max batch size must be greater than 0")
				jobWatcher.EndWatch(fferr.NewInternalErrorf("Max batch size must be greater than 0"))
				return
			}
			setterFn = func() {
				defer wg.Done()
				if err != nil {
					select {
					case errCh <- err:
						logger.Debugw("error getting max batch size", "error", err)
					default:
					}
					return
				}
				buffer := make([]provider.SetItem, 0, maxBatch)
				for record := range ch {
					buffer = append(buffer, provider.SetItem{record.Entity, record.Value})
					if len(buffer) == maxBatch {
						if err := batchTable.BatchSet(buffer); err != nil {
							logger.Errorf("error setting batch: %v", err)
							select {
							case errCh <- err:
							default:
							}
						}
						buffer = buffer[:0]
					}
				}
				// Clear the buffer
				if len(buffer) != 0 {
					if err := batchTable.BatchSet(buffer); err != nil {
						logger.Errorf("error setting batch: %v", err)
						select {
						case errCh <- err:
						default:
						}
					}
					buffer = buffer[:0]
				}
			}
		} else {
			setterFn = func() {
				defer wg.Done()
				for record := range ch {
					if err := m.Table.Set(record.Entity, record.Value); err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}
			}
		}
		// Create a set of goroutines that can wait for the inference store to respond asynchronously
		for idx := 0; idx < workerPoolSize; idx++ {
			go setterFn()
		}
		var chanErr error
		for it.Next() {
			select {
			case chanErr = <-errCh:
				logger.Errorf("error setting value: %v", chanErr)
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
			jobWatcher.EndWatch(chanErr)
			return
		}
		if err = it.Err(); err != nil {
			jobWatcher.EndWatch(err)
			return
		}
		err = it.Close()
		if err != nil {
			jobWatcher.EndWatch(err)
			return
		}
		err = m.Store.Close()
		if err != nil {
			jobWatcher.EndWatch(err)
			return
		}
		jobWatcher.EndWatch(nil)
	}()
	return jobWatcher, nil
}

func (m *MaterializedChunkRunner) SetIndex(index int) error {
	m.ChunkIdx = index
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
	return "Job completed successfully."
}

type MaterializedChunkRunnerConfig struct {
	OnlineType     pt.Type
	OfflineType    pt.Type
	OnlineConfig   pc.SerializedConfig
	OfflineConfig  pc.SerializedConfig
	MaterializedID provider.MaterializationID
	ResourceID     provider.ResourceID
	ChunkIdx       int
	IsUpdate       bool
	Logger         *zap.SugaredLogger
	SkipCache      bool
}

func (m *MaterializedChunkRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(m)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (m *MaterializedChunkRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

// We'll often build many MaterializedChunkRunners with the same provider configs, this is an optimization
// to avoid that. This assumes the providers are thread-safe however. We separate the caches because some
// tests will create empty configs for both an online and offline store and they'll map to the same thing.
var onlineProviderCache = &sync.Map{}
var offlineProviderCache = &sync.Map{}

func MaterializedChunkRunnerFactory(config Config) (types.Runner, error) {
	runnerConfig := &MaterializedChunkRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, err
	}
	var onlineStore provider.OnlineStore
	var offlineStore provider.OfflineStore
	onlineCfg := runnerConfig.OnlineConfig
	offlineCfg := runnerConfig.OfflineConfig
	onlineCfgStr := string(onlineCfg)
	offlineCfgStr := string(offlineCfg)

	cachedOnline, found := onlineProviderCache.Load(onlineCfgStr)
	if found && providerCachingEnabled {
		onlineStore = cachedOnline.(provider.OnlineStore)
	} else {
		onlineProvider, err := provider.Get(runnerConfig.OnlineType, onlineCfg)
		if err != nil {
			return nil, err
		}
		store, err := onlineProvider.AsOnlineStore()
		if err != nil {
			return nil, err
		}
		onlineStore = store
		if providerCachingEnabled {
			onlineProviderCache.Store(onlineCfgStr, onlineStore)
		}
	}

	cachedOffline, found := offlineProviderCache.Load(offlineCfgStr)
	if found && providerCachingEnabled {
		offlineStore = cachedOffline.(provider.OfflineStore)
	} else {
		offlineProvider, err := provider.Get(runnerConfig.OfflineType, offlineCfg)
		if err != nil {
			return nil, err
		}
		store, err := offlineProvider.AsOfflineStore()
		if err != nil {
			return nil, err
		}
		offlineStore = store
		if providerCachingEnabled {
			offlineProviderCache.Store(offlineCfgStr, offlineStore)
		}
	}

	materialization, err := offlineStore.GetMaterialization(runnerConfig.MaterializedID)
	if err != nil {
		return nil, err
	}
	table, err := onlineStore.GetTable(runnerConfig.ResourceID.Name, runnerConfig.ResourceID.Variant)
	if err != nil {
		return nil, err
	}
	return &MaterializedChunkRunner{
		Materialized: materialization,
		Table:        table,
		Store:        onlineStore,
		ChunkIdx:     runnerConfig.ChunkIdx,
	}, nil
}
