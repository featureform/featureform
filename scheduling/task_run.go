package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
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
	oneOffTrigger TriggerType = "OneOffTrigger"
	dummyTrigger  TriggerType = "DummyTrigger"
)

type Trigger interface {
	Type() TriggerType
	Name() string
}

type OneOffTrigger struct {
	TriggerName string `json:"triggerName"`
}

func (t OneOffTrigger) Type() TriggerType {
	return oneOffTrigger
}

func (t OneOffTrigger) Name() string {
	return t.TriggerName
}

type DummyTrigger struct {
	TriggerName string `json:"triggerName"`
	DummyField  bool   `json:"dummyField"`
}

func (t DummyTrigger) Type() TriggerType {
	return dummyTrigger
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

// Formatting
func (t *TaskRunMetadata) Marshal() ([]byte, error) {
	return json.Marshal(t)
}
func (t *TaskRunMetadata) Unmarshal(data []byte) error {
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
		return fmt.Errorf("failed to deserialize task run metadata: %w", err)
	}

	if temp.ID == 0 {
		return fmt.Errorf("task run metadata is missing ID")
	}
	t.ID = temp.ID

	if temp.TaskId == 0 {
		return fmt.Errorf("task run metadata is missing RunID")
	}
	t.TaskId = temp.TaskId

	if temp.Name == "" {
		return fmt.Errorf("task run metadata is missing name")
	}
	t.Name = temp.Name

	if temp.Status == "" || (temp.Status != Success && temp.Status != Failed && temp.Status != Pending) {
		return fmt.Errorf("unknown status: %s", temp.Status)
	}
	t.Status = temp.Status

	if temp.StartTime.IsZero() {
		return fmt.Errorf("task run metadata is missing Start Time")
	}
	t.StartTime = temp.StartTime

	t.TriggerType = temp.TriggerType

	t.EndTime = temp.EndTime
	t.Logs = temp.Logs
	t.Error = temp.Error

	triggerMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Trigger, &triggerMap); err != nil {
		return fmt.Errorf("failed to deserialize trigger data: %w", err)
	}

	switch temp.TriggerType {
	case oneOffTrigger:
		var oneOffTrigger OneOffTrigger
		if err := json.Unmarshal(temp.Trigger, &oneOffTrigger); err != nil {
			return fmt.Errorf("failed to deserialize One Off Trigger data: %w", err)
		}
		t.Trigger = oneOffTrigger
	case dummyTrigger:
		var dummyTrigger DummyTrigger
		if err := json.Unmarshal(temp.Trigger, &dummyTrigger); err != nil {
			return fmt.Errorf("failed to deserialize Dummy Trigger data: %w", err)
		}
		t.Trigger = dummyTrigger
	default:
		return fmt.Errorf("unknown trigger type: %s", temp.TriggerType)
	}
	return nil
}
