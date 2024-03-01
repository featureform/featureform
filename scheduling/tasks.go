package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskID int32 // need to determine how we want to create IDs
type TaskType string

const (
	ResourceCreation TaskType = "Resource Creation"
	HealthCheck      TaskType = "Health Check"
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
	return json.Marshal(t)
}

func (t *TaskMetadata) Unmarshal(data []byte) error {

	type tempConfig struct {
		ID          TaskID          `json:"id"`
		Name        string          `json:"name"`
		TaskType    TaskType        `json:"type"`
		Target      json.RawMessage `json:"target"`
		TargetType  TargetType      `json:"targetType"`
		DateCreated time.Time       `json:"dateCreated"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to deserialize task metadata: %w", err)
	}
	if temp.ID == 0 {
		return fmt.Errorf("task metadata is missing ID")
	}
	t.ID = temp.ID

	if temp.Name == "" {
		return fmt.Errorf("task metadata is missing Name")
	}
	t.Name = temp.Name

	if temp.TaskType == "" || (temp.TaskType != ResourceCreation && temp.TaskType != HealthCheck && temp.TaskType != Monitoring) {
		return fmt.Errorf("unknown task type: %s", temp.TaskType)
	}
	t.TaskType = temp.TaskType

	if temp.DateCreated.IsZero() {
		return fmt.Errorf("task metadata is missing Date value")
	}
	t.DateCreated = temp.DateCreated

	t.TargetType = temp.TargetType

	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Target, &targetMap); err != nil {
		return fmt.Errorf("failed to deserialize target data: %w", err)
	}

	switch temp.TargetType {
	case ProviderTarget:
		var provider Provider
		if err := json.Unmarshal(temp.Target, &provider); err != nil {
			return fmt.Errorf("failed to deserialize Provider data: %w", err)
		}
		t.Target = provider
	case NameVariantTarget:
		var nameVariant NameVariant
		if err := json.Unmarshal(temp.Target, &nameVariant); err != nil {
			return fmt.Errorf("failed to deserialize NameVariant data: %w", err)
		}
		t.Target = nameVariant
	default:
		return fmt.Errorf("unknown target type: %s", temp.TargetType)
	}
	return nil
}
