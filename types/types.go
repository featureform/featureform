package types

import (
	"github.com/featureform/metadata"
)

type Runner interface {
	Run() (CompletionWatcher, error)
	Resource() metadata.ResourceID
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
