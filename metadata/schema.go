package metadata

import (
	s "github.com/featureform/scheduling"
	"github.com/featureform/schema"
)

const latestExecutorTaskRunLockPath schema.Version = 1

type executorTaskRunLockPathSchema map[schema.Version]string

var executorTaskRunLockPath = executorTaskRunLockPathSchema{
	1: "/runlock/{{ .TaskRunID }}",
}

func ExecutorTaskRunLockPath(id s.TaskRunID) string {
	path := executorTaskRunLockPath[latestExecutorTaskRunLockPath]
	templ := schema.Templater(path, map[string]interface{}{
		"TaskRunID": id.Value(),
	})
	return templ
}

// Need to add storage field to this
type executorTaskRunLockPathUpgrader struct {
	executorTaskRunLockPathSchema
}

func (p *executorTaskRunLockPathUpgrader) Upgrade(start, end schema.Version) error {
	return nil
}

func (p *executorTaskRunLockPathUpgrader) Downgrade(start, end schema.Version) error {
	return nil
}
