package scheduling

import (
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/featureform/metadata/proto"
	sch "github.com/featureform/scheduling/proto"

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
		key += fmt.Sprintf("/%s", trmk.date.UTC().Format(dateFormat))
	}
	return key
}

type TaskRunID ffsync.OrderedId

func NewTaskRunIdFromString(id string) (TaskRunID, error) {
	var orderedID ffsync.Uint64OrderedId
	err := orderedID.FromString(id)
	if err != nil {
		return nil, err
	}
	return TaskRunID(&orderedID), nil
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

func (s Status) Proto() pb.ResourceStatus_Status {
	return pb.ResourceStatus_Status(s)
}

type TriggerType int32

const (
	OnApplyTriggerType  TriggerType = TriggerType(sch.TriggerType_ON_APPLY)
	ScheduleTriggerType TriggerType = TriggerType(sch.TriggerType_SCHEDULE)
)

func (tt TriggerType) String() string {
	return sch.TriggerType_name[int32(tt)]
}

func (tt TriggerType) Proto() sch.TriggerType {
	return sch.TriggerType(tt)
}

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

type ScheduleTrigger struct {
	TriggerName string `json:"triggerName"`
	Schedule    string `json:"schedule"`
}

func (t ScheduleTrigger) Type() TriggerType {
	return ScheduleTriggerType
}

func (t ScheduleTrigger) Name() string {
	return t.TriggerName
}

type TaskRunMetadata struct {
	ID           TaskRunID   `json:"runId"`
	TaskId       TaskID      `json:"taskId"`
	Name         string      `json:"name"`
	Trigger      Trigger     `json:"trigger"`
	TriggerType  TriggerType `json:"triggerType"`
	Target       TaskTarget  `json:"target"`
	TargetType   TargetType  `json:"targetType"`
	Dependencies []TaskRunID `json:"dependencies"`
	Status       Status      `json:"status"`
	StartTime    time.Time   `json:"startTime"`
	EndTime      time.Time   `json:"endTime"`
	Logs         []string    `json:"logs"`
	Error        string      `json:"error"`
	ErrorProto   *pb.ErrorStatus
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
		Target      json.RawMessage `json:"target"`
		TargetType  TargetType      `json:"targetType"`
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
	t.TargetType = temp.TargetType

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
	case ScheduleTriggerType:
		var scheduleTrigger ScheduleTrigger
		if err := json.Unmarshal(temp.Trigger, &scheduleTrigger); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Schedule Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Trigger = scheduleTrigger
	default:
		errMessage := fmt.Errorf("unknown trigger type: %s", temp.TriggerType)
		return fferr.NewInvalidArgumentError(errMessage)
	}

	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Target, &targetMap); err != nil {
		errMessage := fmt.Errorf("failed to deserialize target data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	switch temp.TargetType {
	case NameVariantTarget:
		var nvTarget NameVariant
		if err := json.Unmarshal(temp.Target, &nvTarget); err != nil {
			errMessage := fmt.Errorf("failed to deserialize NameVariant target data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = nvTarget
	case ProviderTarget:
		var providerTarget Provider
		if err := json.Unmarshal(temp.Target, &providerTarget); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Schedule Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = providerTarget
	default:
		errMessage := fmt.Errorf("unknown target type: %s", temp.Target)
		return fferr.NewInvalidArgumentError(errMessage)
	}

	return nil
}
