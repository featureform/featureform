package scheduling

import (
	"encoding/json"
	"fmt"

	"slices"

	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	pb "github.com/featureform/scheduling/proto"
)

type Key interface {
	String() string
}

type TaskMetadataKey struct {
	taskID TaskID
}

func (tmk TaskMetadataKey) String() string {
	if tmk.taskID == nil {
		return "/tasks/metadata/task_id="
	}
	return fmt.Sprintf("/tasks/metadata/task_id=%s", tmk.taskID.String())
}

type TaskID ffsync.OrderedId

type TaskType int32

const (
	ResourceCreation TaskType = TaskType(pb.TaskType_RESOURCE_CREATION)
	HealthCheck      TaskType = TaskType(pb.TaskType_HEALTH_CHECK)
	Monitoring       TaskType = TaskType(pb.TaskType_METRICS)
)

func (tt TaskType) String() string {
	return pb.TaskType_name[int32(tt)]
}

func (tt TaskType) Proto() pb.TaskType {
	return pb.TaskType(tt)
}

type TargetType int32

const (
	ProviderTarget    TargetType = TargetType(pb.TargetType_PROVIDER)
	NameVariantTarget TargetType = TargetType(pb.TargetType_NAME_VARIANT)
)

func (tt TargetType) String() string {
	return pb.TargetType_name[int32(tt)]
}

func (tt TargetType) Proto() pb.TargetType {
	return pb.TargetType(tt)
}

type Provider struct {
	Name string `json:"name"`
}

func (p Provider) Type() TargetType {
	return ProviderTarget
}

type NameVariant struct {
	Name         string `json:"name"`
	Variant      string `json:"variant"`
	ResourceType string `json:"type"`
}

func (nv NameVariant) Type() TargetType {
	return NameVariantTarget
}

type TaskTarget interface {
	Type() TargetType
}

type TaskMetadata struct {
	ID          TaskID     `json:"id"`
	Name        string     `json:"name"`
	TaskType    TaskType   `json:"type"`
	Target      TaskTarget `json:"target"`
	TargetType  TargetType `json:"targetType"`
	DateCreated time.Time  `json:"dateCreated"`
}

func (t *TaskMetadata) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		errMessage := fmt.Errorf("failed to serialize task metadata: %w", err)
		return nil, fferr.NewInternalError(errMessage)
	}
	return bytes, nil
}

func (t *TaskMetadata) Unmarshal(data []byte) error {
	type tempConfig struct {
		ID          uint64          `json:"id"`
		Name        string          `json:"name"`
		TaskType    TaskType        `json:"type"`
		Target      json.RawMessage `json:"target"`
		TargetType  TargetType      `json:"targetType"`
		DateCreated time.Time       `json:"dateCreated"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		errMessage := fmt.Errorf("failed to deserialize task metadata: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	id := ffsync.Uint64OrderedId(temp.ID)
	t.ID = TaskID(&id)

	if temp.Name == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task metadata is missing name"))
	}
	t.Name = temp.Name

	if temp.TaskType.String() == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task metadata is missing TaskType"))
	}

	validTypes := []TaskType{ResourceCreation, HealthCheck, Monitoring}
	if !slices.Contains(validTypes, temp.TaskType) {
		err := fferr.NewInvalidArgumentError(fmt.Errorf("task metadata has invalid TaskType"))
		err.AddDetail("TaskType", string(temp.TaskType))
		return err
	}
	t.TaskType = temp.TaskType

	if temp.DateCreated.IsZero() {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task metadata is missing DateCreated"))
	}
	t.DateCreated = temp.DateCreated

	t.TargetType = temp.TargetType

	// TODO: ask Riddhi if there was purpose for this
	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Target, &targetMap); err != nil {
		errMessage := fmt.Errorf("failed to deserialize target data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	switch temp.TargetType {
	case ProviderTarget:
		var provider Provider
		if err := json.Unmarshal(temp.Target, &provider); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Provider data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = provider
	case NameVariantTarget:
		var nameVariant NameVariant
		if err := json.Unmarshal(temp.Target, &nameVariant); err != nil {
			errMessage := fmt.Errorf("failed to deserialize NameVariant data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = nameVariant
	default:
		err := fferr.NewInvalidArgumentError(fmt.Errorf("unknown target type"))
		err.AddDetail("TargetType", string(temp.TargetType))
		return err
	}
	return nil
}
