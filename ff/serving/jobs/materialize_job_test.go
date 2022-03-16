package jobs


import (
	"fmt"
	"testing"
	"errors"
)

func (m *MaterializedChunkRunner) Run() (CompletionStatus, error) {
	return &MockMaterializedJobCompletionStatus{
		Done: true,
	}, nil
}


type MockMaterializedJobCompletionStatus struct {
	Done bool
}

func (m *MockMaterializedJobCompletionStatus) PercentComplete() float32 {
	if m.Done{
		return 1.0
	} else{
		return 0.0
	}
}

func (m *MockMaterializedJobCompletionStatus) String() string {
	if m.Done{
		return "Job is completed"
	} else{
		return "Job is not yet completed"
	}
}

type MockMaterializedFeatures struct {
	Rows []int
}

func (m *MockMaterializedFeatures) NumRows() (int, error) {
	return len(m.Rows), nil
}

func (m *MockMaterializedFeatures) IterateSegment(begin int, end int) (FeatureIterator, error) {
	return &MockFeatureIterator{
		CurrentIndex: 0,
		Slice: m.Rows[begin:end],
	}, nil
}

type MockOnlineTable struct {
	DataTable map[string]int
}

func (m *MockOnlineTable) Set(entity string, value interface{}) error {
	m.DataTable[entity] = value.(int)
	return nil
}

func (m *MockOnlineTable) Get(entity string) (interface{}, error) {
	value, exists := m.DataTable[entity]
	if !exists{
		return nil, errors.New("Value does not exist in online table")
	}
	return value, nil
}

type MockFeatureIterator struct {
	CurrentIndex int
	Slice []int
}

func (m *MockFeatureIterator) Next() bool {
	m.CurrentIndex++
	return m.CurrentIndex < len(m.Slice)

}

func (m *MockFeatureIterator) Err() error {
	return nil
}

func (m *MockFeatureIterator) Value() (interface{}, error) {
	value := m.Slice[m.CurrentIndex]
	return value, nil
}


func TestMockRunner(t *testing.T) {

	materialized := &MockMaterializedFeatures{
		Rows: []int{0,1,2,3,4,5,6,7,8,9},
	}

	fmt.Println(materialized.NumRows())

	table := &MockOnlineTable{
		DataTable: make(map[string]int),
	}

	chunkSize := 10
	chunkIdx := 0

	mockChunkJob := MaterializedChunkRunner{
		Materialized: materialized,
		Table: table,
		ChunkSize: chunkSize,
		ChunkIdx: chunkIdx,
	}

	it, err := mockChunkJob.Materialized.IterateSegment(mockChunkJob.ChunkIdx, mockChunkJob.ChunkIdx + mockChunkJob.ChunkSize)
	if err != nil {
		panic(err)
	}
	fmt.Println(it.Err())
	for it.Next() {
		value, err := it.Value()
		if err != nil {
			panic(err)
		}
		mockChunkJob.Table.Set("entity",value)
	}


	completionStatus, err := mockChunkJob.Run()
	if err != nil {
		return
	}

	fmt.Println(completionStatus.String())
	fmt.Println(completionStatus.PercentComplete())

	fmt.Println(table.Get("entity"))
}