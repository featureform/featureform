package jobs

import ()

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
	Failed() bool
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

	success := make(chan bool, 1)
	go func() {
		if(m.ChunkSize == 0){
			success <- true
			return
		}
		it, err := m.Materialized.IterateSegment(m.ChunkIdx, m.ChunkIdx+m.ChunkSize)
		if err != nil {
			success <- false
		}
		for ok := true; ok; ok = it.Next() {
			value := it.Value()
			entity := it.Entity()
			m.Table.Set(entity, value)
		}
		if err = it.Err(); err != nil {
			success <- false
		}

		success <- true
	}()

	return &MaterializeChunkJobCompletionStatus{
		Success:     success,
		Complete: false,
		JobFailed: false,
	}, nil
}

type MaterializeChunkJobCompletionStatus struct {
	Success     chan bool
	Complete bool
	JobFailed bool
}

func (m *MaterializeChunkJobCompletionStatus) PercentComplete() float32 {
	if m.Complete {
		return 1.0
	}
	select {
	case outcome := <-m.Success:
		m.Complete = true
		if !outcome {
			m.JobFailed = true
			return 1.0
		}
		close(m.Success)
		return 1.0
	default:
		return 0.0
	}
}

func (m *MaterializeChunkJobCompletionStatus) String() string {
	if m.PercentComplete() == 0 {
		return "Job not complete."
	}
	return "Job complete"
}

func (m *MaterializeChunkJobCompletionStatus) Failed() bool {
	return m.JobFailed
}
