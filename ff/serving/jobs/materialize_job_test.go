package jobs


import (
	"fmt"
	"testing"
	"errors"
	"time"
)

func (m *MaterializedChunkRunner) Run() (CompletionStatus, error) {
	var numComplete = new(int)
	*numComplete = 0
	completionStatus := &MockMaterializedJobCompletionStatus{
		RowsComplete: numComplete,
		TotalRows: m.ChunkSize,
		Done: false,
	}
	go func() {
		it, err := m.Materialized.IterateSegment(m.ChunkIdx, m.ChunkIdx + m.ChunkSize)
		if err != nil {
			panic(err)
		}
		for ok := true; ok; ok = it.Next() { 
			value, err := it.Value()
			if err != nil {
				panic(err)
			}
			m.Table.Set("entity",value)
			*numComplete += 1
			time.Sleep(time.Millisecond*10)
			
		}
		completionStatus.Done = true
	}()

	return completionStatus, nil
}


type MockMaterializedJobCompletionStatus struct {
	RowsComplete *int
	TotalRows int
	Done bool
}

func (m MockMaterializedJobCompletionStatus) PercentComplete() float32 {
	if m.Done {
		return 1.0
	}
	return float32(*(m.RowsComplete)/m.TotalRows)
}

func (m MockMaterializedJobCompletionStatus) String() string {
	if m.Done {
		return fmt.Sprintf("%d out of %d rows completed.",m.TotalRows,m.TotalRows)
	}
	return fmt.Sprintf("%d out of %d rows completed.",*(m.RowsComplete),m.TotalRows)
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

	mockChunkJob := &MaterializedChunkRunner{
		Materialized: materialized,
		Table: table,
		ChunkSize: chunkSize,
		ChunkIdx: chunkIdx,
	}
	


	completionStatus, err := mockChunkJob.Run()
	if err != nil {
		return
	}
	fmt.Println(completionStatus.PercentComplete())
	fmt.Println(completionStatus.String())
	fmt.Println(table.Get("entity"))
	
	time.Sleep(time.Millisecond*30)
	fmt.Println(completionStatus.String())
	fmt.Println(completionStatus.PercentComplete())
	time.Sleep(time.Second*1)
	fmt.Println(completionStatus.String())
	fmt.Println(completionStatus.PercentComplete())
}