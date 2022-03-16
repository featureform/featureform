package jobs

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type MockMaterializedFeatures struct {
	Rows [][]interface{}
}

func (m *MockMaterializedFeatures) NumRows() (int, error) {
	return len(m.Rows), nil
}

func (m *MockMaterializedFeatures) IterateSegment(begin int, end int) (FeatureIterator, error) {
	return &MockFeatureIterator{
		CurrentIndex: 0,
		Slice:        m.Rows[begin:end],
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
	if !exists {
		return nil, errors.New("Value does not exist in online table")
	}
	return value, nil
}

type MockFeatureIterator struct {
	CurrentIndex int
	Slice        [][]interface{}
}

func (m *MockFeatureIterator) Next() bool {
	m.CurrentIndex++
	return m.CurrentIndex < len(m.Slice)

}

func (m *MockFeatureIterator) Err() error {
	return nil
}

func (m *MockFeatureIterator) Entity() string {
	entity := m.Slice[m.CurrentIndex][0]
	return entity.(string)
}

func (m *MockFeatureIterator) Value() interface{} {
	value := m.Slice[m.CurrentIndex][1]
	return value
}

func TestMockRunner(t *testing.T) {

	materialized := &MockMaterializedFeatures{
		Rows: [][]interface{}{{"entity_1",1}, {"entity_2",2}, {"entity_3",3}, {"entity_4",4}, {"entity_5",5}},
	}

	table := &MockOnlineTable{
		DataTable: make(map[string]int),
	}

	chunkSize := 5
	chunkIdx := 0

	mockChunkJob := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    chunkSize,
		ChunkIdx:     chunkIdx,
	}

	completionStatus, err := mockChunkJob.Run()
	if err != nil {
		return
	}

	fmt.Println(completionStatus.String())
	time.Sleep(time.Second)
	fmt.Println(completionStatus.String())

	fmt.Println(materialized.NumRows())
	fmt.Print(table.Get("entity_1"))
	fmt.Print(table.Get("entity_6"))

}
