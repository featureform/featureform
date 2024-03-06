package scheduling

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
)

type TaskRunSimple struct {
	RunID       TaskRunID `json:"runID"`
	DateCreated time.Time `json:"dateCreated"`
}

type TaskRuns struct {
	TaskID TaskID          `json:"taskID"`
	Runs   []TaskRunSimple `json:"runs"`
}

func (tr *TaskRuns) Marshal() ([]byte, fferr.GRPCError) {
	b, err := json.Marshal(tr)
	if err != nil {
		errMessage := fmt.Errorf("failed to marshal TaskRun: %v", err)
		return nil, fferr.NewInternalError(errMessage)
	}
	return b, nil
}

func (tr *TaskRuns) Unmarshal(data []byte) fferr.GRPCError {
	err := json.Unmarshal(data, tr)
	if err != nil {
		errMessage := fmt.Errorf("failed to deserialize NameVariant data: %w", err)
		return fferr.NewInternalError(errMessage)
	}
	return nil
}
