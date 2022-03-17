package jobs

import (
	"errors"
	"testing"

)

type MockMaterializedFeatures struct {
	Rows []FeatureRow
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
	DataTable map[string]interface{}
}

func (m *MockOnlineTable) Set(entity string, value interface{}) error {
	m.DataTable[entity] = value
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
	Slice        []FeatureRow
}

func (m *MockFeatureIterator) Next() bool {
	m.CurrentIndex++
	return m.CurrentIndex < len(m.Slice)

}

func (m *MockFeatureIterator) Err() error {
	return nil
}

func (m *MockFeatureIterator) Entity() string {
	return m.Slice[m.CurrentIndex].Entity
}

func (m *MockFeatureIterator) Value() interface{} { 
	return m.Slice[m.CurrentIndex].Row
}

type FeatureRow struct {
	Entity string
	Row interface{}
}

func TestRunnerOverlap(t *testing.T) {
	materialized := &MockMaterializedFeatures{
		Rows: []FeatureRow{
			FeatureRow{
				Entity: "entity_1",
				Row: 1,
			},
			FeatureRow{
				Entity: "entity_2",
				Row: 2,
			},
			FeatureRow{
				Entity: "entity_3",
				Row: 3,
			},
		},
	}

	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}

	mockChunkJob1 := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    1,
		ChunkIdx:     0,
	}

	mockChunkJob2 := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    2,
		ChunkIdx:     1,
	}

	completionStatus1, err := mockChunkJob1.Run()
	if err != nil {
		t.Fatalf("Chunk copy job 1 failed: %v", err)
	}
	completionStatus2, err := mockChunkJob2.Run()
	if err != nil {
		t.Fatalf("Chunk copy job 2 failed: %v", err)
	}

	for completionStatus1.PercentComplete() < 1.0 {}
	for completionStatus2.PercentComplete() < 1.0 {}

	if completionStatus1.Failed() {
		t.Fatalf("Chunk copy job 1 failed")
	}
	if completionStatus2.Failed() {
		t.Fatalf("Chunk copy job 2 failed")
	}

	for _, row := range materialized.Rows {

		tableValue, err := table.Get(row.Entity)
		if err != nil {
			t.Fatalf("Cannot fetch table value for entity %v: %v", row.Entity, err)
		}
		if tableValue != row.Row {
			t.Fatalf("%v becomes %v in table copy", row.Row, tableValue)
		}
	}

}

func TestRunnerChunks(t *testing.T) {
	materialized := &MockMaterializedFeatures{
		Rows: []FeatureRow{
			FeatureRow{
				Entity: "entity_1",
				Row: 1,
			},
			FeatureRow{
				Entity: "entity_2",
				Row: 2,
			},
			FeatureRow{
				Entity: "entity_3",
				Row: 3,
			},
		},
	}

	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}

	mockChunkJob1 := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    1,
		ChunkIdx:     0,
	}

	mockChunkJob2 := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    1,
		ChunkIdx:     1,
	}

	mockChunkJob3 := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    1,
		ChunkIdx:     2,
	}

	completionStatus1, err := mockChunkJob1.Run()
	if err != nil {
		t.Fatalf("Chunk copy job 1 failed: %v", err)
	}
	completionStatus2, err := mockChunkJob2.Run()
	if err != nil {
		t.Fatalf("Chunk copy job 2 failed: %v", err)
	}
	completionStatus3, err := mockChunkJob3.Run()
	if err != nil {
		t.Fatalf("Chunk copy job 3 failed: %v", err)
	}

	for completionStatus1.PercentComplete() < 1.0 {}
	for completionStatus2.PercentComplete() < 1.0 {}
	for completionStatus3.PercentComplete() < 1.0 {}

	if completionStatus1.Failed() {
		t.Fatalf("Chunk copy job 1 failed")
	}
	if completionStatus2.Failed() {
		t.Fatalf("Chunk copy job 2 failed")
	}
	if completionStatus3.Failed() {
		t.Fatalf("Chunk copy job 3 failed")
	}

	for _, row := range materialized.Rows {

		tableValue, err := table.Get(row.Entity)
		if err != nil {
			t.Fatalf("Cannot fetch table value for entity %v: %v", row.Entity, err)
		}
		if tableValue != row.Row {
			t.Fatalf("%v becomes %v in table copy", row.Row, tableValue)
		}
	}
}

func TestRunnerEmpty(t *testing.T) {

	tableEmpty := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}

	mockChunkJobEmpty := &MaterializedChunkRunner{
		Materialized: &MockMaterializedFeatures{},
		Table: tableEmpty,
		ChunkSize: 0,
		ChunkIdx: 0,
	}

	completionStatusEmpty, err := mockChunkJobEmpty.Run()
	if err != nil {
		t.Fatalf("Chunk copy job failed: %v", err)
	}

	for completionStatusEmpty.PercentComplete() < 1.0 {}

	if len(tableEmpty.DataTable) != 0 {
		t.Fatalf("Empty features somehow copied to table: %v", err)
	}

}
func TestRunner(t *testing.T) {

	materialized := &MockMaterializedFeatures{
		Rows: []FeatureRow{
			FeatureRow{
				Entity: "entity_1",
				Row: 1,
			},
			FeatureRow{
				Entity: "entity_2",
				Row: 2,
			},
			FeatureRow{
				Entity: "entity_3",
				Row: 3,
			},
		},
	}

	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}

	mockChunkJobFull := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    len(materialized.Rows),
		ChunkIdx:     0,
	}

	completionStatus, err := mockChunkJobFull.Run()
	if err != nil {
		t.Fatalf("Chunk copy job failed: %v", err)
	}

	for completionStatus.PercentComplete() < 1.0 {}

	if completionStatus.Failed() {
		t.Fatalf("Chunk copy job failed")
	}

	for _, row := range materialized.Rows {

		tableValue, err := table.Get(row.Entity)
		if err != nil {
			t.Fatalf("Cannot fetch table value for entity %v: %v", row.Entity, err)
		}
		if tableValue != row.Row {
			t.Fatalf("%v becomes %v in table copy", row.Row, tableValue)
		}
	}

}
