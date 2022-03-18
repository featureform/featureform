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
	TestName     string
	Materialized MockMaterializedFeatures
	ChunkSize    int
	ChunkIdx     int
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
		CurrentIndex: -1,
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

func testParams(params JobTestParams) error {

	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}
	featureRows := params.Materialized.Rows
	job := &MaterializedChunkRunner{
		Materialized: &params.Materialized,
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

type CopyTestData struct {
	Rows []interface{}
}

func CreateMockFeatureRows(data []interface{}) MockMaterializedFeatures {
	featureRows := make([]FeatureRow, len(data))
	for i, row := range data {
		featureRows[i] = FeatureRow{Entity: fmt.Sprintf("entity_%d", i), Row: row}
	}
	return MockMaterializedFeatures{Rows: featureRows}
}

func TestJobs(t *testing.T) {
	emptyList := CopyTestData{
		Rows: []interface{}{},
	}
	basicNumList := CopyTestData{
		Rows: []interface{}{1, 2, 3, 4, 5},
	}

	stringNumList := CopyTestData{
		Rows: []interface{}{"one", "two", "three", "four", "five"},
	}
	multipleTypesList := CopyTestData{
		Rows: []interface{}{1, "two", 3.0, 'f', false},
	}

	numListofLists := CopyTestData{
		Rows: []interface{}{[]int{1, 2, 3}, []int{2, 3, 4}, []int{3, 4, 5}},
	}

	differentTypeLists := CopyTestData{
		Rows: []interface{}{[]int{1, 2, 3}, []string{"two", "three", "four"}, []float64{3.0, 4.0, 5.0}},
	}
	testJobs := []JobTestParams{
		JobTestParams{
			TestName:     "Basic copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "Partial copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    2,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "Chunk size overflow test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    6,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "Single copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "Final index copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    1,
			ChunkIdx:     4,
		},
		JobTestParams{
			TestName:     "Last overlap chunk test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    2,
			ChunkIdx:     2,
		},
		JobTestParams{
			TestName:     "Zero chunk size copy test",
			Materialized: CreateMockFeatureRows(basicNumList.Rows),
			ChunkSize:    0,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "String list copy test",
			Materialized: CreateMockFeatureRows(stringNumList.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "Different types copy test",
			Materialized: CreateMockFeatureRows(multipleTypesList.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "List features test",
			Materialized: CreateMockFeatureRows(numListofLists.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "List features different types",
			Materialized: CreateMockFeatureRows(differentTypeLists.Rows),
			ChunkSize:    5,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "No rows test",
			Materialized: CreateMockFeatureRows(emptyList.Rows),
			ChunkSize:    1,
			ChunkIdx:     0,
		},
		JobTestParams{
			TestName:     "No rows/zero chunk size test",
			Materialized: CreateMockFeatureRows(emptyList.Rows),
			ChunkSize:    0,
			ChunkIdx:     0,
		},
	}
	for _, param := range testJobs {
		if err := testParams(param); err != nil {
			t.Fatalf("Test Job Failed: %s, %v\n", param.TestName, err)
		}
	}
}
