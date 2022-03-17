package jobs

import (
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"math"
	"testing"
)

type MockMaterializedFeatures struct {
	Rows []FeatureRow
}

type JobTestParams struct {
	FeatureTableData []interface{}
	NumJobs          int
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
		Slice:        m.Rows[begin:int(math.Min(float64(end), float64(len(m.Rows))))],
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

func testFollowingParams(params *JobTestParams) error {

	featureRows := make([]FeatureRow, len(params.FeatureTableData))
	for i, row := range params.FeatureTableData {
		featureRows[i] = FeatureRow{Entity: fmt.Sprintf("entity_%d", i), Row: row}
	}

	materialized := &MockMaterializedFeatures{Rows: featureRows}

	table := &MockOnlineTable{
		DataTable: make(map[string]interface{}),
	}

	jobList := make([]*MaterializedChunkRunner, params.NumJobs)

	var chunkSize int
	if params.NumJobs == 0 {
		chunkSize = 0
	} else {
		chunkSize = int(math.Ceil(float64(len(params.FeatureTableData)) / float64(params.NumJobs)))
	}

	for i := 0; i < params.NumJobs; i++ {
		jobList[i] = &MaterializedChunkRunner{
			Materialized: materialized,
			Table:        table,
			ChunkSize:    chunkSize,
			ChunkIdx:     i,
		}
	}

	jobGroup := new(errgroup.Group)
	for i := range jobList {
		i := i
		jobGroup.Go(func() error {
			completionStatus, err := jobList[i].Run()
			if err != nil {
				return err
			}
			for completionStatus.PercentComplete() < 1.0 && completionStatus.Err() == nil {
			}
			if completionStatus.Err() != nil {
				return completionStatus.Err()
			}
			return nil
		})
	}

	if err := jobGroup.Wait(); err != nil {
		return &TestError{Outcome: "Job failed to run.", Err: err}
	}

	for _, row := range materialized.Rows {

		tableValue, err := table.Get(row.Entity)
		if err != nil {
			return &TestError{Outcome: fmt.Sprintf("Cannot fetch table value for entity %v", row.Entity), Err: err}
		}
		if tableValue != row.Row {
			return &TestError{Outcome: fmt.Sprintf("%v becomes %v in table copy", row.Row, tableValue), Err: nil}
		}
	}

	return nil
}

func TestSingleRunJob(t *testing.T) {

	testParams := &JobTestParams{
		FeatureTableData: []interface{}{1, 2, 3, 4, 5},
		NumJobs:          1,
	}

	err := testFollowingParams(testParams)

	if err != nil {
		t.Fatalf("Test Single Run Job Failed: %v", err)
	}
}

func TestEmpty(t *testing.T) {

	testParams := &JobTestParams{
		FeatureTableData: []interface{}{},
		NumJobs:          0,
	}

	err := testFollowingParams(testParams)

	if err != nil {
		t.Fatalf("Test Emtpy Failed: %v", err)
	}
}

func TestMultipleJobs(t *testing.T) {

	testParams := &JobTestParams{
		FeatureTableData: []interface{}{1, 2, 3, 4, 5},
		NumJobs:          5,
	}

	err := testFollowingParams(testParams)

	if err != nil {
		t.Fatalf("Test Multiple Jobs Failed: %v", err)
	}
}

func TestJobsDifferentRowAllotments(t *testing.T) {

	testParams := &JobTestParams{
		FeatureTableData: []interface{}{1, 2, 3, 4, 5},
		NumJobs:          2,
	}

	err := testFollowingParams(testParams)

	if err != nil {
		t.Fatalf("Test Multiple Jobs Failed: %v", err)
	}
}
