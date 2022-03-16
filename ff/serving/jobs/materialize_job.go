package jobs

import (
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
	go func() {
		it, err := m.Materialized.IterateSegment(m.ChunkIdx, m.ChunkIdx+m.ChunkSize)
		if err != nil {
			panic(err)
		}
		for ok := true; ok; ok = it.Next() {
			value := it.Value()
			if err != nil {
				panic(err)
			}
			entity := it.Entity()
			m.Table.Set(entity, value)
		}

		done <- true
	}()

	return &MaterializeChunkJobCompletionStatus{
		Done: done,
		Complete: false,
	}, nil
}

type MaterializeChunkJobCompletionStatus struct {
	Done chan bool
	Complete bool
}

func (m *MaterializeChunkJobCompletionStatus) PercentComplete() float32 {
	if m.Complete{
		return 1.0
	}
	select {
	case <-m.Done:
		m.Complete = true
		close(m.Done)
		return 1.0
	default :
		return 0.0
	}
}

func (m *MaterializeChunkJobCompletionStatus) String() string {
	if m.PercentComplete() == 0 {
		return "Job not complete."
	}
	return "Job complete"
}
