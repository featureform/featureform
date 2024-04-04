package tasks

import (
	"context"
	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/ffsync"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
	ss "github.com/featureform/storage"
	"go.uber.org/zap/zaptest"
	"net"
	"testing"
)

func startServ(t *testing.T) (*metadata.MetadataServer, string) {
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		panic(err.Error())
	}
	mstorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		panic(err.Error())
	}
	storage := ss.MetadataStorage{
		Locker:  &locker,
		Storage: &mstorage,
	}

	manager, err := scheduling.NewMemoryTaskMetadataManager()
	if err != nil {
		panic(err.Error())
	}
	logger := zaptest.NewLogger(t)
	config := &metadata.Config{
		Logger:          logger.Sugar(),
		StorageProvider: storage,
		TaskManager:     manager,
	}
	serv, err := metadata.NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func TestLabelTaskRun(t *testing.T) {
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

	err = client.CreateLabelVariant(context.Background(), metadata.LabelDef{
		Name:     "labelName",
		Variant:  "labelVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Source:   metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
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

	var labelTaskRun scheduling.TaskRunMetadata
	for _, run := range runs {
		if sourceTaskRun.ID.String() != run.ID.String() {
			labelTaskRun = run
		}
	}

	task := LabelTask{
		BaseTask{
			metadata: client,
			taskDef:  labelTaskRun,
			Spawner:  &spawner.MemoryJobSpawner{},
			logger:   zaptest.NewLogger(t).Sugar(),
		},
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createPreqResources(t *testing.T, client *metadata.Client) scheduling.TaskRunMetadata {
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

	source, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"})
	if err != nil {
		t.Fatalf(err.Error())
	}

	sourceProvider, err := source.FetchProvider(client, context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}

	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		t.Fatalf(err.Error())
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		t.Fatalf(err.Error())
	}

	schema := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "col1", ValueType: provider.String},
			{Name: "col2", ValueType: provider.String},
		},
		SourceTable: "mockTable",
	}

	// Added this because we dont actually run the primary table registration before this test
	tableID := provider.ResourceID{Name: "sourceName", Variant: "sourceVariant", Type: provider.Primary}
	_, err = store.CreatePrimaryTable(tableID, schema)
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

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	return runs[0]
}
