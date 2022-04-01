package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
	"sync"
)

type Runner interface {
	Run() (CompletionWatcher, error)
}

type IndexRunner interface {
	Runner
	SetIndex(index int) error
}

type MaterializedChunkRunner struct {
	Materialized provider.Materialization
	Table        provider.OnlineStoreTable
	ChunkSize    int64
	ChunkIdx     int64
}

type CompletionWatcher interface {
	Complete() bool
	String() string
	Wait() error
	Err() error
}

type OnlineTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
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
			value := it.Value().Value
			entity := it.Value().Entity
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

func (m *MaterializedChunkRunner) SetIndex(index int) error {
	m.ChunkIdx = int64(index)
	return nil
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
