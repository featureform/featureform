package scheduling

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
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

type TaskType string

const (
	ResourceCreation TaskType = "ResourceCreation"
	HealthCheck      TaskType = "HealthCheck"
	Monitoring       TaskType = "Monitoring"
)

type TargetType string

const (
	ProviderTarget    TargetType = "Provider"
	NameVariantTarget TargetType = "NameVariant"
)

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

	if temp.TaskType == "" {
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
