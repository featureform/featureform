package scheduling

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
)

type TaskRunID int32
type Status string

const (
	Success Status = "SUCCESS"
	Failed  Status = "FAILED"
	Pending Status = "PENDING"
	Running Status = "RUNNING"
)

type TriggerType string

const (
	OneOffTriggerType TriggerType = "OneOffTrigger"
	DummyTriggerType  TriggerType = "DummyTrigger"
)

type Trigger interface {
	Type() TriggerType
	Name() string
}

type OneOffTrigger struct {
	TriggerName string `json:"triggerName"`
}

func (t OneOffTrigger) Type() TriggerType {
	return OneOffTriggerType
}

func (t OneOffTrigger) Name() string {
	return t.TriggerName
}

type DummyTrigger struct {
	TriggerName string `json:"triggerName"`
	DummyField  bool   `json:"dummyField"`
}

func (t DummyTrigger) Type() TriggerType {
	return DummyTriggerType
}

func (t DummyTrigger) Name() string {
	return t.TriggerName
}

type TaskRunMetadata struct {
	ID          TaskRunID   `json:"runId"`
	TaskId      TaskID      `json:"taskId"`
	Name        string      `json:"name"`
	Trigger     Trigger     `json:"trigger"`
	TriggerType TriggerType `json:"triggerType"`
	Status      Status      `json:"status"`
	StartTime   time.Time   `json:"startTime"`
	EndTime     time.Time   `json:"endTime"`
	Logs        []string    `json:"logs"`
	Error       string      `json:"error"`
}

func (t *TaskRunMetadata) Marshal() ([]byte, fferr.GRPCError) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to marshal TaskRunMetadata: %w", err))
	}
	return bytes, nil
}
func (t *TaskRunMetadata) Unmarshal(data []byte) fferr.GRPCError {
	type tempConfig struct {
		ID          TaskRunID       `json:"runId"`
		TaskId      TaskID          `json:"taskId"`
		Name        string          `json:"name"`
		Trigger     json.RawMessage `json:"trigger"`
		TriggerType TriggerType     `json:"triggerType"`
		Status      Status          `json:"status"`
		StartTime   time.Time       `json:"startTime"`
		EndTime     time.Time       `json:"endTime"`
		Logs        []string        `json:"logs"`
		Error       string          `json:"error"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		errMessage := fmt.Errorf("failed to deserialize task run metadata: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	if temp.ID == 0 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing RunID"))
	}
	t.ID = temp.ID

	if temp.TaskId == 0 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing TaskID"))
	}
	t.TaskId = temp.TaskId

	if temp.Name == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing Name"))
	}
	t.Name = temp.Name

	t.Status = temp.Status

	if temp.StartTime.IsZero() {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing StartTime"))
	}
	t.StartTime = temp.StartTime

	t.TriggerType = temp.TriggerType

	t.EndTime = temp.EndTime
	t.Logs = temp.Logs
	t.Error = temp.Error

	triggerMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Trigger, &triggerMap); err != nil {
		errMessage := fmt.Errorf("failed to deserialize trigger data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	switch temp.TriggerType {
	case OneOffTriggerType:
		var oneOffTrigger OneOffTrigger
		if err := json.Unmarshal(temp.Trigger, &oneOffTrigger); err != nil {
			errMessage := fmt.Errorf("failed to deserialize One Off Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Trigger = oneOffTrigger
	case DummyTriggerType:
		var dummyTrigger DummyTrigger
		if err := json.Unmarshal(temp.Trigger, &dummyTrigger); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Dummy Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Trigger = dummyTrigger
	default:
		errMessage := fmt.Errorf("unknown trigger type: %s", temp.TriggerType)
		return fferr.NewInvalidArgumentError(errMessage)
	}
	return nil
}
