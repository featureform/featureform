package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskId int32 // need to determine how we want to create IDs
type RunId int32  // need to determine how we want to create IDs
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
	Name       string     `json:"name"`
	TargetType TargetType `json:"targetType"`
}

type NameVariant struct {
	Name       string     `json:"name"`
	TargetType TargetType `json:"targetType"`
}

func (p Provider) Type() TargetType {
	return p.TargetType
}

func (nv NameVariant) Type() TargetType {
	return nv.TargetType
}

type TaskTarget interface {
	Type() TargetType
}

type TaskMetadata struct {
	ID          TaskId     `json:"id"`
	Name        string     `json:"name"`
	TaskType    TaskType   `json:"type"`
	Target      TaskTarget `json:"target"`
	DateCreated time.Time  `json:"dateCreated"`
}

func (t *TaskMetadata) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TaskMetadata) Unmarshal(data []byte) error {

	type tempConfig struct {
		ID          TaskId          `json:"id"`
		Name        string          `json:"name"`
		TaskType    TaskType        `json:"type"`
		Target      json.RawMessage `json:"target"`
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

	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Target, &targetMap); err != nil {
		return fmt.Errorf("failed to deserialize target data: %w", err)
	}

	if _, ok := targetMap["targetType"]; !ok {
		return fmt.Errorf("target type is missing")
	}

	switch targetMap["targetType"] {
	case ProviderTarget:
		var provider Provider
		if err := json.Unmarshal(temp.Target, &provider); err != nil {
			return fmt.Errorf("failed to deserialize Provider data: %w", err)
		}
		t.Target = provider
	case NameVariantTarget:
		var namevariant NameVariant
		if err := json.Unmarshal(temp.Target, &namevariant); err != nil {
			return fmt.Errorf("failed to deserialize NameVariant data: %w", err)
		}
		t.Target = namevariant
	default:
		return fmt.Errorf("unknown target type: %s", targetMap["targetType"])
	}
	return nil

}
