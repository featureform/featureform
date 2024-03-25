package scheduling

import (
	"encoding/json"
	"fmt"
	pb "github.com/featureform/metadata/proto"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
)

type TaskRunKey struct {
	taskID TaskID
}

func (trk TaskRunKey) String() string {
	if trk.taskID == nil {
		return "/tasks/runs/task_id="
	}
	return fmt.Sprintf("/tasks/runs/task_id=%s", trk.taskID.String())
}

type TaskRunMetadataKey struct {
	taskID TaskID
	runID  TaskRunID
	date   time.Time
}

func (trmk TaskRunMetadataKey) String() string {
	key := "/tasks/runs/metadata"

	// adds the date to the key if it's not zero
	if !trmk.date.IsZero() {
		key += fmt.Sprintf("/%s", trmk.date.Format("2006/01/02"))

		// adds the task_id and run_id to the key if they're not null
		taskIdIsNotNil := trmk.taskID != nil
		runIdIsNotNil := trmk.runID != nil
		if taskIdIsNotNil && runIdIsNotNil {
			key += fmt.Sprintf("/task_id=%s/run_id=%s", trmk.taskID.String(), trmk.runID.String())
		}
	}
	return key
}

type TaskRunID ffsync.OrderedId
type Status string

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
	ErrorProto  *pb.ErrorStatus
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
		ErrorProto  *pb.ErrorStatus
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
	case OnApplyTriggerType:
		var oneOffTrigger OnApplyTrigger
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
