package tasks

import (
	"context"
	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
	"go.uber.org/zap/zaptest"
	"testing"
)

func initFeatureRunner(t *testing.T) {
	if err := runner.RegisterFactory(runner.MATERIALIZE, runner.MaterializeRunnerFactory); err != nil {
		t.Fatalf("failed to register 'Materialize' runner factory: %s", err.Error())
	}
}

func TestFeatureTaskRun(t *testing.T) {
	initFeatureRunner(t)
	logger := zaptest.NewLogger(t).Sugar()

	serv, addr := startServ(t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	sourceTaskRun := createPreqResources(t, client)
	t.Log("Source Run:", sourceTaskRun)

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, scheduling.READY, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name:    "featureName",
		Variant: "featureVariant",
		Owner:   "mockOwner",
		Source:  metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
		Location: metadata.ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			Source: "mockTable",
		},
		Entity: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 2 {
		t.Fatalf("Expected 2 run to be created, got: %d", len(runs))
	}

	var featureTaskRun scheduling.TaskRunMetadata
	for _, run := range runs {
		if sourceTaskRun.ID.String() != run.ID.String() {
			featureTaskRun = run
		}
	}

	task := FeatureTask{
		BaseTask{
			metadata: client,
			taskDef:  featureTaskRun,
			Spawner:  &spawner.MemoryJobSpawner{},
		},
		zaptest.NewLogger(t).Sugar(),
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}
