package scheduling

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
)

type TaskRunSimple struct {
	RunID       TaskRunID `json:"runID"`
	DateCreated time.Time `json:"dateCreated"`
}

func (trs *TaskRunSimple) Marshal() ([]byte, error) {
	b, err := json.Marshal(trs)
	if err != nil {
		errMessage := fmt.Errorf("failed to marshal TaskRunSimple: %v", err)
		return nil, fferr.NewInternalError(errMessage)
	}
	return b, nil
}

func (trs *TaskRunSimple) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		RunID       uint64    `json:"runID"`
		DateCreated time.Time `json:"dateCreated"`
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		errMessage := fmt.Errorf("failed to deserialize TaskRunSimple data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	id := ffsync.Uint64OrderedId(temp.RunID)
	trs.RunID = TaskRunID(&id)
	trs.DateCreated = temp.DateCreated

	return nil
}

type TaskRuns struct {
	TaskID TaskID          `json:"taskID"`
	Runs   []TaskRunSimple `json:"runs"`
}

func (tr *TaskRuns) Marshal() ([]byte, error) {
	b, err := json.Marshal(tr)
	if err != nil {
		errMessage := fmt.Errorf("failed to marshal TaskRun: %v", err)
		return nil, fferr.NewInternalError(errMessage)
	}
	return b, nil
}

func (tr *TaskRuns) Unmarshal(data []byte) error {
	type tempConfig struct {
		TaskID uint64          `json:"taskID"`
		Runs   []TaskRunSimple `json:"runs"`
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		errMessage := fmt.Errorf("failed to deserialize NameVariant data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	id := ffsync.Uint64OrderedId(temp.TaskID)
	tr.TaskID = TaskID(&id)

	tr.Runs = temp.Runs

	return nil
}

func (tr *TaskRuns) ContainsRun(runID TaskRunID) (bool, TaskRunSimple) {
	// TODO: need to convert this into binary search
	for _, run := range tr.Runs {
		if run.RunID.Value() == runID.Value() {
			return true, run
		}
	}
	return false, TaskRunSimple{}
}
