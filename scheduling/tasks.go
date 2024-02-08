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
	ProviderTarget    TargetType = "provider"
	NameVariantTarget TargetType = "name_variant"
)

type Provider struct {
	Name       string     `json:"name"`
	TargetType TargetType `json:"target_type"`
}

type NameVariant struct {
	Name       string     `json:"name"`
	TargetType TargetType `json:"target_type"`
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
	ID       TaskId     `json:"id"`
	Name     string     `json:"name"`
	TaskType TaskType   `json:"type"`
	Target   TaskTarget `json:"target"`
	Date     time.Time  `json:"date"`
}

func (t *TaskMetadata) ToJSON() ([]byte, error) {
	marshal, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

func (t *TaskMetadata) FromJSON(data []byte) error {

	type tempConfig struct {
		ID       TaskId          `json:"id"`
		Name     string          `json:"name"`
		TaskType TaskType        `json:"type"`
		Target   json.RawMessage `json:"target"`
		Date     time.Time       `json:"date"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to deserialize task metadata due to: %w", err)
	}
	if temp.ID == 0 || temp.Name == "" || temp.TaskType == "" || len(temp.Target) == 0 || temp.Date.IsZero() {
		return fmt.Errorf("task metadata is missing required fields")
	}
	t.ID = temp.ID
	t.Name = temp.Name

	if temp.TaskType != ResourceCreation && temp.TaskType != HealthCheck && temp.TaskType != Monitoring {
		return fmt.Errorf("unknown task type: %s", temp.TaskType)
	}
	t.TaskType = temp.TaskType
	t.Date = temp.Date

	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Target, &targetMap); err != nil {
		return fmt.Errorf("failed to deserialize target data due to: %w", err)
	}

	if _, ok := targetMap["target_type"]; !ok {
		return fmt.Errorf("target type is missing")
	}

	if targetMap["target_type"] == "provider" {
		var provider Provider
		if err := json.Unmarshal(temp.Target, &provider); err != nil {
			return fmt.Errorf("failed to deserialize Provider data due to: %w", err)
		}
		t.Target = provider
	} else if targetMap["target_type"] == "name_variant" {
		var namevariant NameVariant
		if err := json.Unmarshal(temp.Target, &namevariant); err != nil {
			return fmt.Errorf("failed to deserialize NameVariant data due to: %w", err)
		}
		t.Target = namevariant
	} else {
		return fmt.Errorf("unknown target type: %s", targetMap["target_type"])
	}
	return nil

}
