package tasks

import (
	"context"
	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/metadata"
	pt "github.com/featureform/provider/provider_type"
	"go.uber.org/zap/zaptest"
	"testing"
)

func TestSourceTaskRun(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	serv, addr := startServ(t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	createSourcePreqResources(t, client)

	err = client.CreateSourceVariant(context.Background(), metadata.SourceDef{
		Name:    "sourceName",
		Variant: "sourceVariant",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	task := SourceTask{
		BaseTask{
			metadata: client,
			taskDef:  runs[0],
			Spawner:  &spawner.MemoryJobSpawner{},
			logger:   zaptest.NewLogger(t).Sugar(),
		},
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createSourcePreqResources(t *testing.T, client *metadata.Client) {
	err := client.CreateUser(context.Background(), metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 0 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}
}
