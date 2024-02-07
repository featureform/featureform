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
	name       string     `json:"name"`
	targetType TargetType `json:"target_type"`
}

type NameVariant struct {
	name       string     `json:"name"`
	targetType TargetType `json:"target_type"`
}

func (p Provider) Type() TargetType {
	return p.targetType
}

func (nv NameVariant) Type() TargetType {
	return nv.targetType
}

type TaskTarget interface {
	Type() TargetType
}

type TaskMetadata struct {
	id       TaskId     `json:"id"`
	name     string     `json:"name"`
	taskType TaskType   `json:"type"`
	target   TaskTarget `json:"target"`
	date     time.Time  `json:"date"`
}

func (t *TaskMetadata) ID() TaskId {
	return t.id
}

func (t *TaskMetadata) Name() string {
	return t.name
}

func (t *TaskMetadata) Target() TaskTarget {
	return t.target
}

func (t *TaskMetadata) DateCreated() time.Time {
	return t.date
}

func (t *TaskMetadata) ToJSON() ([]byte, error) {
	type config TaskMetadata
	c := config(*t)
	marshal, err := json.Marshal(&c)
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

func (t *TaskMetadata) FromJSON(data []byte) error {

	type tempConfig struct {
		id       TaskId          `json:"id"`
		name     string          `json:"name"`
		taskType TaskType        `json:"type"`
		target   json.RawMessage `json:"target"`
		date     time.Time       `json:"date"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to deserialize task metadata due to: %w", err)
	}
	if temp.id == 0 || temp.name == "" || temp.taskType == "" || len(temp.target) == 0 || temp.date.IsZero() {
		return fmt.Errorf("task metadata is missing required fields")
	}
	t.id = temp.id
	t.name = temp.name

	if temp.taskType != ResourceCreation && temp.taskType != HealthCheck && temp.taskType != Monitoring {
		return fmt.Errorf("unknown task type: %s", temp.taskType)
	}
	t.taskType = temp.taskType
	t.date = temp.date

	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.target, &targetMap); err != nil {
		return fmt.Errorf("failed to deserialize target data due to: %w", err)
	}

	if _, ok := targetMap["target_type"]; !ok {
		return fmt.Errorf("target type is missing")
	}

	if targetMap["target_type"] == "provider" {
		var provider Provider
		if err := json.Unmarshal(temp.target, &provider); err != nil {
			return fmt.Errorf("failed to deserialize Provider data due to: %w", err)
		}
		t.target = provider
	} else if targetMap["target_type"] == "name_variant" {
		var namevariant NameVariant
		if err := json.Unmarshal(temp.target, &namevariant); err != nil {
			return fmt.Errorf("failed to deserialize NameVariant data due to: %w", err)
		}
		t.target = namevariant
	} else {
		return fmt.Errorf("unknown target type: %s", targetMap["target_type"])
	}
	return nil

}
