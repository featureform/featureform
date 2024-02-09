package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskRunId int32
type TaskId int32 // TODO: This datatype already exists in rasks.go, so when we merge, we should use the same?
type Status string

const (
	Success Status = "SUCCESS"
	Failed  Status = "FAILED"
	Pending Status = "PENDING"
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
	TriggerName string      `json:"triggerName"`
	TriggerType TriggerType `json:"triggerType"`
}

func (t OneOffTrigger) Type() TriggerType {
	return t.TriggerType
}

func (t OneOffTrigger) Name() string {
	return t.TriggerName
}

type DummyTrigger struct {
	TriggerName string      `json:"triggerName"`
	TriggerType TriggerType `json:"triggerType"`
	DummyField  bool        `json:"dummyField"`
}

func (t DummyTrigger) Type() TriggerType {
	return t.TriggerType
}

func (t DummyTrigger) Name() string {
	return t.TriggerName
}

type TaskRunMetadata struct {
	ID        TaskRunId `json:"id"`
	RunId     TaskId    `json:"runId"`
	Name      string    `json:"name"`
	Trigger   Trigger   `json:"trigger"`
	Status    Status    `json:"status"`
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	Logs      []string  `json:"logs"`
	Error     string    `json:"error"`
}

// Read Methods
// func (t *TaskRunMetadata) ID() TaskRunId
// func (t *TaskRunMetadata) RunID() TaskId
// func (t *TaskRunMetadata) Name() string
// func (t *TaskRunMetadata) Trigger() Trigger
// func (t *TaskRunMetadata) Status() Status // Enum
// func (t *TaskRunMetadata) StartTime() Time
// func (t *TaskRunMetadata) EndTime() Time
// func (t *TaskRunMetadata) Logs() []string
// func (t *TaskRunMetadata) Error() string

// Formatting
func (t *TaskRunMetadata) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}
func (t *TaskRunMetadata) FromJSON(data []byte) error {
	type tempConfig struct {
		ID        TaskRunId       `json:"id"`
		RunId     TaskId          `json:"runId"`
		Name      string          `json:"name"`
		Trigger   json.RawMessage `json:"trigger"`
		Status    Status          `json:"status"`
		StartTime time.Time       `json:"startTime"`
		EndTime   time.Time       `json:"endTime"`
		Logs      []string        `json:"logs"`
		Error     string          `json:"error"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to deserialize task run metadata: %w", err)
	}

	if temp.ID == 0 {
		return fmt.Errorf("task run metadata is missing ID")
	}
	t.ID = temp.ID

	if temp.RunId == 0 {
		return fmt.Errorf("task run metadata is missing RunID")
	}
	t.RunId = temp.RunId

	if temp.Name == "" {
		return fmt.Errorf("task run metadata is missing required fields")
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

	if temp.EndTime.IsZero() {
		return fmt.Errorf("task run metadata is missing End Time")
	}
	t.EndTime = temp.EndTime
	t.Logs = temp.Logs
	t.Error = temp.Error

	triggerMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Trigger, &triggerMap); err != nil {
		return fmt.Errorf("failed to deserialize trigger data: %w", err)
	}

	if _, ok := triggerMap["triggerType"]; !ok {
		return fmt.Errorf("target type is missing")
	}

	if triggerMap["triggerType"] == "OneOffTrigger" {
		var oneOffTrigger OneOffTrigger
		if err := json.Unmarshal(temp.Trigger, &oneOffTrigger); err != nil {
			return fmt.Errorf("failed to deserialize One Off Trigger data: %w", err)
		}
		t.Trigger = oneOffTrigger
	} else if triggerMap["triggerType"] == "DummyTrigger" {
		var dummyTrigger DummyTrigger
		if err := json.Unmarshal(temp.Trigger, &dummyTrigger); err != nil {
			return fmt.Errorf("failed to deserialize Dummy Trigger data: %w", err)
		}
		t.Trigger = dummyTrigger
	} else {
		return fmt.Errorf("unknown target type: %s", triggerMap["triggerType"])
	}
	return nil
}
