package scheduling

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
)

const (
	taskRunKeyPrefix         = "/tasks/runs/task_id="
	taskRunMetadataKeyPrefix = "/tasks/runs/metadata"
)

type TaskRunKey struct {
	taskID TaskID
}

func (trk TaskRunKey) String() string {
	if trk.taskID == nil {
		return "/tasks/runs/task_id="
	}
	return fmt.Sprintf("%s%s", taskRunKeyPrefix, trk.taskID.String())
}

type TaskRunMetadataKey struct {
	taskID TaskID
	runID  TaskRunID
	date   time.Time
}

func (trmk TaskRunMetadataKey) String() string {
	// will return the taskRunMetadataKeyPrefix with date to the minute; ex. /tasks/runs/metadata/2021/01/01/15/04
	key := trmk.TruncateToMinute()

	// adds the task_id and run_id to the key if they're not null
	taskIdIsNotNil := trmk.taskID != nil
	runIdIsNotNil := trmk.runID != nil
	if taskIdIsNotNil && runIdIsNotNil {
		key += fmt.Sprintf("/task_id=%s/run_id=%s", trmk.taskID.String(), trmk.runID.String())
	}
	return key
}

func (trmk TaskRunMetadataKey) TruncateToDay() string {
	dayFormat := "2006/01/02"

	return trmk.pathWithDateFormat(dayFormat)
}

func (trmk TaskRunMetadataKey) TruncateToHour() string {
	hourFormat := "2006/01/02/15"

	return trmk.pathWithDateFormat(hourFormat)
}

func (trmk TaskRunMetadataKey) TruncateToMinute() string {
	minuteFormat := "2006/01/02/15/04"

	return trmk.pathWithDateFormat(minuteFormat)
}

func (trmk TaskRunMetadataKey) pathWithDateFormat(dateFormat string) string {
	key := taskRunMetadataKeyPrefix

	// adds the date to the key if it's not zero
	if !trmk.date.IsZero() {
		key += fmt.Sprintf("/%s", trmk.date.Format(dateFormat))
	}
	return key
}

type TaskRunID ffsync.OrderedId
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

func (t *TaskRunMetadata) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to marshal TaskRunMetadata: %w", err))
	}
	return bytes, nil
}
func (t *TaskRunMetadata) Unmarshal(data []byte) error {
	type tempConfig struct {
		ID          uint64          `json:"runId"`
		TaskId      uint64          `json:"taskId"`
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

	runId := ffsync.Uint64OrderedId(temp.ID)
	t.ID = TaskRunID(&runId)

	taskId := ffsync.Uint64OrderedId(temp.TaskId)
	t.TaskId = TaskID(&taskId)

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
