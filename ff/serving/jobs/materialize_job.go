package jobs

import (
	"fmt"
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
	PercentComplete() float32
	String() string
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

	done := make(chan bool, 1)
	errorChan := make(chan error, 1)
	go func() {
		if m.ChunkSize == 0 {
			done <- true
			return
		}
		it, err := m.Materialized.IterateSegment(m.ChunkIdx*m.ChunkSize, (m.ChunkIdx+1)*m.ChunkSize)
		if err != nil {
			errorChan <- err
			return
		}
		for ok := true; ok; ok = it.Next() {
			value := it.Value()
			entity := it.Entity()
			err := m.Table.Set(entity, value)
			if err != nil {
				errorChan <- err
				return
			}
		}
		if err = it.Err(); err != nil {
			errorChan <- err
			return
		}

		done <- true
		errorChan <- nil
	}()

	return &MaterializeChunkJobCompletionStatus{
		Completed:     false,
		CompletedChan: done,
		ErrorChan:     errorChan,
		Error:         nil,
	}, nil
}

type MaterializeChunkJobCompletionStatus struct {
	Completed     bool
	CompletedChan chan bool
	ErrorChan     chan error
	Error         error
}

func (m *MaterializeChunkJobCompletionStatus) Err() error {
	if m.Error != nil {
		return m.Error
	}
	select {
	case err := <-m.ErrorChan:
		close(m.ErrorChan)
		m.Error = err
		return err
	default:
		return nil
	}
}

func (m *MaterializeChunkJobCompletionStatus) PercentComplete() float32 {
	if m.Completed {
		return 1.0
	}
	select {
	case <-m.CompletedChan:
		close(m.CompletedChan)
		m.Completed = true
		return 1.0
	default:
		return 0.0
	}
}

func (m *MaterializeChunkJobCompletionStatus) String() string {
	if m.Err() != nil {
		return fmt.Sprintf("Job failed with error %v", m.Error)
	}
	if m.PercentComplete() != 1.0 {
		return "Job still running."
	}
	return "Job completed succesfully."
}
