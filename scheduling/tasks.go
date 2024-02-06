package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskId int32 // need to determine how we want to create IDs
type RunId int32  // need to determine how we want to create IDs
type TaskType string
type TargetType string

const NameVariant TargetType = "NameVariant"
const Provider TargetType = "Provider"

const (
	ResourceCreation TaskType = "ResourceCreation"
	HealthCheck      TaskType = "HealthCheck"
	Monitoring       TaskType = "Monitoring"
)

type TaskTarget interface {
	Type() TargetType
}

type TaskMetadata struct {
	ID     TaskId
	Name   string
	Type   TaskType
	Target TaskTarget
	Date   time.Time
}

func (t *TaskMetadata) getID() TaskId {
	return t.ID
}

func (t *TaskMetadata) getName() string {
	return t.Name
}

func (t *TaskMetadata) getTarget() TaskTarget {
	return t.Target
}

func (t *TaskMetadata) DateCreated() time.Time {
	return t.Date
}

func (t *TaskMetadata) ToJSON() ([]byte, error) {
	switch vt.ValueType.(type) {
	case VectorType:
		return json.Marshal(map[string]VectorType{"ValueType": vt.ValueType.(VectorType)})
	case ScalarType:
		return json.Marshal(map[string]ScalarType{"ValueType": vt.ValueType.(ScalarType)})
	default:
		return nil, fmt.Errorf("could not marshal value type: %v", vt.ValueType)
	}
}

func (t *TaskMetadata) FromJSON(data []byte) error {
	v := map[string]VectorType{"ValueType": {}}
	if err := json.Unmarshal(data, &v); err == nil {
		vt.ValueType = v["ValueType"]
		return nil
	}

	s := map[string]ScalarType{"ValueType": ScalarType("")}
	if err := json.Unmarshal(data, &s); err == nil {
		vt.ValueType = s["ValueType"]
		return nil
	}

	return fmt.Errorf("could not unmarshal value type: %v", data)
}
