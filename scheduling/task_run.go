package scheduling

import (
	"encoding/json"
	"fmt"
	pb "github.com/featureform/metadata/proto"
	"strconv"
	"time"
)

type TaskRunID int32

func (tid *TaskRunID) FromString(id string) error {
	if id == "" {
		return fmt.Errorf("cannot convert an empty string")
	}
	intID, err := strconv.Atoi(id)
	if err != nil {
		return err
	}
	*tid = TaskRunID(intID)
	return nil
}

type Status int32

const (
	NO_STATUS Status = Status(pb.ResourceStatus_NO_STATUS)
	CREATED   Status = Status(pb.ResourceStatus_CREATED)
	PENDING   Status = Status(pb.ResourceStatus_PENDING)
	READY     Status = Status(pb.ResourceStatus_READY)
	FAILED    Status = Status(pb.ResourceStatus_FAILED)
	RUNNING   Status = Status(pb.ResourceStatus_RUNNING)
)

func (s Status) String() string {
	return pb.ResourceStatus_Status_name[int32(s)]
}

func (s Status) Serialized() pb.ResourceStatus_Status {
	return pb.ResourceStatus_Status(s)
}

type TriggerType string

const (
	OnApplyTriggerType TriggerType = "On Apply"
	DummyTriggerType   TriggerType = "DummyTrigger"
)

type Trigger interface {
	Type() TriggerType
	Name() string
}

type OnApplyTrigger struct {
	TriggerName string `json:"triggerName"`
}

func (t OnApplyTrigger) Type() TriggerType {
	return OnApplyTriggerType
}

func (t OnApplyTrigger) Name() string {
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
	case OnApplyTriggerType:
		var oneOffTrigger OnApplyTrigger
		if err := json.Unmarshal(temp.Trigger, &oneOffTrigger); err != nil {
			return fmt.Errorf("failed to deserialize One Off Trigger data: %w", err)
		}
		t.Trigger = oneOffTrigger
	case DummyTriggerType:
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
