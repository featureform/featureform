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
	errorSync := &ErrorSync{done: false}
	go func() {
		if m.ChunkSize == 0 {
			errorSync.Set(nil)
			close(done)
			return
		}
		numRows, err := m.Materialized.NumRows()
		if err != nil {
			errorSync.Set(err)
			close(done)
			return
		}
		if numRows == 0 {
			errorSync.Set(nil)
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
			errorSync.Set(err)
			close(done)
			return
		}
		for ok := true; ok; ok = it.Next() {
			value := it.Value()
			entity := it.Entity()
			err := m.Table.Set(entity, value)
			if err != nil {
				errorSync.Set(err)
				close(done)
				return
			}
		}
		if err = it.Err(); err != nil {
			errorSync.Set(err)
			close(done)
			return
		}
		errorSync.Set(nil)
		close(done)
	}()
	return &MaterializeChunkJobCompletionStatus{
		ErrorSync: errorSync,
		Done:      done,
	}, nil
}

type ErrorSync struct {
	err  error
	done bool
	mu   sync.Mutex
}

func (e *ErrorSync) Get() (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.done, e.err
}

func (e *ErrorSync) Set(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.err = err
	e.done = true
}

type MaterializeChunkJobCompletionStatus struct {
	ErrorSync *ErrorSync
	Done      chan interface{}
}

func (m *MaterializeChunkJobCompletionStatus) Err() error {
	_, err := m.ErrorSync.Get()
	return err
}

func (m *MaterializeChunkJobCompletionStatus) Wait() error {
	<-m.Done
	_, err := m.ErrorSync.Get()
	return err
}

func (m *MaterializeChunkJobCompletionStatus) Complete() bool {
	done, _ := m.ErrorSync.Get()
	return done
}

func (m *MaterializeChunkJobCompletionStatus) String() string {
	done, err := m.ErrorSync.Get()
	if err != nil {
		return fmt.Sprintf("Job failed with error: %v", err)
	}
	if !done {
		return "Job still running."
	}
	return "Job completed succesfully."
}
