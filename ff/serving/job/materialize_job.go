package job

import (
	"fmt"
	"sync"
)

type Runner interface {
	Run() (CompletionStatus, error)
}

type MaterializedChunkRunner struct {
	Materialized MaterializedFeatures
	Table        OnlineTable
	ChunkSize    int
	ChunkIdx     int
}

type CompletionStatus interface {
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

func (m *MaterializedChunkRunner) Run() (CompletionStatus, error) {
	done := make(chan interface{})
	resultSync := &ResultSync{}
	go func() {
		if m.ChunkSize == 0 {
			resultSync.DoneWithError(nil)
			close(done)
			return
		}
		numRows, err := m.Materialized.NumRows()
		if err != nil {
			resultSync.DoneWithError(err)
			close(done)
			return
		}
		if numRows == 0 {
			resultSync.DoneWithError(nil)
			close(done)
			return
		}
		var chunkEnd int
		if (m.ChunkIdx+1)*m.ChunkSize < numRows {
			chunkEnd = (m.ChunkIdx + 1) * m.ChunkSize
		} else {
			chunkEnd = numRows
		}
		it, err := m.Materialized.IterateSegment(m.ChunkIdx*m.ChunkSize, chunkEnd)
		if err != nil {
			resultSync.DoneWithError(err)
			close(done)
			return
		}
		for ok := true; ok; ok = it.Next() {
			value := it.Value()
			entity := it.Entity()
			err := m.Table.Set(entity, value)
			if err != nil {
				resultSync.DoneWithError(err)
				close(done)
				return
			}
		}
		if err = it.Err(); err != nil {
			resultSync.DoneWithError(err)
			close(done)
			return
		}
		resultSync.DoneWithError(nil)
		close(done)
	}()
	return &CopyCompletionWatcher{
		ResultSync:  resultSync,
		DoneChannel: done,
	}, nil
}

type ResultSync struct {
	err  error
	done bool
	mu   sync.RWMutex
}

func (r *ResultSync) Get() (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.done, r.err
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
	_, err := m.ResultSync.Get()
	return err
}

func (m *CopyCompletionWatcher) Wait() error {
	<-m.DoneChannel
	_, err := m.ResultSync.Get()
	return err
}

func (m *CopyCompletionWatcher) Complete() bool {
	done, _ := m.ResultSync.Get()
	return done
}

func (m *CopyCompletionWatcher) String() string {
	done, err := m.ResultSync.Get()
	if err != nil {
		return fmt.Sprintf("Job failed with error: %v", err)
	}
	if !done {
		return "Job still running."
	}
	return "Job completed succesfully."
}
