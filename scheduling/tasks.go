package scheduling

import (
	"time"
)

type TaskId int32 // need to determine how we want to create IDs
type RunId int32  // need to determine how we want to create IDs
type Time time.Time
type TaskType int
type TargetType string

const (
	ResourceCreation TaskType = iota
	HealthCheck
	Monitoring
)

type TaskTarget interface {
	Type() TargetType
}

type TaskMetadata struct {
	IDField          TaskId
	NameField        string
	TypeField        TaskType
	TargetField      TaskTarget
	DateCreatedField Time
}

func (t *TaskMetadata) ID() TaskId {
	return t.IDField
}

func (t *TaskMetadata) Name() string {
	return t.NameField
}

func (t *TaskMetadata) Target() TaskTarget {
	return t.TargetField
}

func (t *TaskMetadata) DateCreated() Time {
	return t.DateCreatedField
}

func (t *TaskMetadata) ToJSON() ([]byte, error) {
	return nil, nil
}

func (t *TaskMetadata) FromJSON(data []byte) error {
	return nil
}
