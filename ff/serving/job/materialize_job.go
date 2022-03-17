package job

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
	errorChan := make(chan error, 1)
	go func() {
		if m.ChunkSize == 0 {
			errorChan <- nil
			return
		}
		numRows, err := m.Materialized.NumRows()
		if err != nil {
			errorChan <- err
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
		errorChan <- nil
	}()
	return &MaterializeChunkJobCompletionStatus{
		ErrorChan: errorChan,
		Error:     nil,
	}, nil
}

type MaterializeChunkJobCompletionStatus struct {
	Error     error
	ErrorChan chan error
}

func (m *MaterializeChunkJobCompletionStatus) Err() error {
	if m.Error != nil {
		return m.Error
	}
	select {
	case err := <-m.ErrorChan:
		close(m.ErrorChan)
		m.ErrorChan = nil
		m.Error = err
		return err
	default:
		return nil
	}
}

func (m *MaterializeChunkJobCompletionStatus) Wait() error {
	err := <-m.ErrorChan
	close(m.ErrorChan)
	m.ErrorChan = nil
	if err != nil {
		m.Error = err
	}
	return err
}

func (m *MaterializeChunkJobCompletionStatus) Complete() bool {
	if m.Err() != nil || m.ErrorChan != nil {
		return true
	}
	return false
}

func (m *MaterializeChunkJobCompletionStatus) String() string {
	if !m.Complete() {
		return "Job still running."
	}
	if m.Err() != nil {
		return fmt.Sprintf("Job failed with error %v", m.Error)
	}
	return "Job completed succesfully."
}
