package spawner

import (
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/types"
)

type MemoryJobSpawner struct{}

func (k *MemoryJobSpawner) GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	jobRunner, err := runner.Create(jobName, config)
	if err != nil {
		return nil, err
	}
	return jobRunner, nil
}
