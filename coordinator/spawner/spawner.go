package spawner

import (
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/types"
)

type JobSpawner interface {
	GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error)
}
