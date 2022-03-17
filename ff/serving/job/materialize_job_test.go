package job

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type MockMaterializedFeatures struct {
	Rows []FeatureRow
}

type JobTestParams struct {
	FeatureTableData []interface{}
	ChunkSize        int
	ChunkIdx         int
}

type TestError struct {
	Outcome string
	Err     error
}

func (m *TestError) Error() string {
	return fmt.Sprintf("%v: %s", m.Err, m.Outcome)
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
	Row    interface{}
}

func testParams(params *JobTestParams) error {

	featureRows := make([]FeatureRow, len(params.FeatureTableData))
	for i, row := range params.FeatureTableData {
		featureRows[i] = FeatureRow{Entity: fmt.Sprintf("entity_%d", i), Row: row}
	}

	materialized := &MockMaterializedFeatures{Rows: featureRows}

	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}

	job := &MaterializedChunkRunner{
		Materialized: materialized,
		Table:        table,
		ChunkSize:    params.ChunkSize,
		ChunkIdx:     params.ChunkIdx,
	}

	completionStatus, err := job.Run()
	if err != nil {
		return &TestError{Outcome: "Job failed to start.", Err: err}
	}

	err = completionStatus.Wait()
	if err != nil {
		return &TestError{Outcome: "Job failed while running.", Err: err}
	}

	var chunkEnd int
	if (params.ChunkIdx+1)*params.ChunkSize < len(featureRows) {
		chunkEnd = (params.ChunkIdx + 1) * params.ChunkSize
	} else {
		chunkEnd = len(featureRows)
	}
	for i := params.ChunkIdx * params.ChunkSize; i < chunkEnd; i++ {

		tableValue, err := table.Get(featureRows[i].Entity)
		if err != nil {
			return &TestError{Outcome: fmt.Sprintf("Cannot fetch table value for entity %v", featureRows[i].Entity), Err: err}
		}
		if !reflect.DeepEqual(tableValue, featureRows[i].Row) {
			return &TestError{Outcome: fmt.Sprintf("%v becomes %v in table copy", featureRows[i].Row, tableValue), Err: nil}
		}
	}

	return nil
}

func TestSingleRunJob(t *testing.T) {

	testJobs := []*JobTestParams{
		&JobTestParams{
			FeatureTableData: []interface{}{1, 2, 3, 4, 5},
			ChunkSize:        5,
			ChunkIdx:         0,
		},
		&JobTestParams{
			FeatureTableData: []interface{}{1, 2, 3, 4, 5},
			ChunkSize:        6,
			ChunkIdx:         0,
		},
		&JobTestParams{
			FeatureTableData: []interface{}{1, 2, 3, 4, 5},
			ChunkSize:        1,
			ChunkIdx:         0,
		},
		&JobTestParams{
			FeatureTableData: []interface{}{1, 2, 3, 4, 5},
			ChunkSize:        1,
			ChunkIdx:         4,
		},
		&JobTestParams{
			FeatureTableData: []interface{}{1, 2, 3, 4, 5},
			ChunkSize:        2,
			ChunkIdx:         2,
		},
		&JobTestParams{
			FeatureTableData: []interface{}{1, 2, 3, 4, 5},
			ChunkSize:        0,
			ChunkIdx:         0,
		},
	}

	for i, param := range testJobs {
		err := testParams(param)
		if err != nil {
			t.Fatalf("Test Job %d Failed: %v", i, err)
		}
	}

}
