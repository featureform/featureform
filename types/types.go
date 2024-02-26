package types

import (
	cm "github.com/featureform/helpers/resource"
)

type Runner interface {
	Run() (CompletionWatcher, error)
	Resource() cm.ResourceID
	IsUpdateJob() bool
}

type IndexRunner interface {
	Runner
	SetIndex(index int) error
}

type CompletionWatcher interface {
	Complete() bool
	String() string
	Wait() error
	Err() error
}
