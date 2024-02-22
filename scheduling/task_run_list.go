package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskRunSimple struct {
	RunID       TaskRunID `json:"runID"`
	DateCreated time.Time `json:"dateCreated"`
}

type TaskRuns struct {
	TaskID TaskID          `json:"taskID"`
	Runs   []TaskRunSimple `json:"runs"`
}

func (tr *TaskRuns) Marshal() ([]byte, error) {
	b, err := json.Marshal(tr)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal TaskRun: %v", err)
	}
	return b, nil
}

func (tr *TaskRuns) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, tr)
	if err != nil {
		return fmt.Errorf("failed to unmarshal TaskRun: %v", err)
	}
	return nil
}
