package coordinator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func createTrainingSetWithProvider(client *metadata.Client, config provider.SerializedConfig) {
	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Simba Khadder",
		},
		metadata.ProviderDef{
			Name:             "test_provider",
			Description:      "",
			Type:             "",
			Software:         "",
			Team:             "",
			SerializedConfig: config,
		},
		metadata.EntityDef{
			Name:        "user",
			Description: "",
		},
		metadata.SourceDef{
			Name:        "Transactions",
			Variant:     "default",
			Description: "",
			Type:        "",
			Owner:       "",
			Provider:    "",
		},
		metadata.LabelDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "",
			Type:        "int",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Entity:      "user",
			Owner:       "",
			Provider:    "test_provider",
		},
		metadata.FeatureDef{
			Name:        "user_transaction_count",
			Variant:     "7d",
			Source:      metadata.NameVariant{"Transactions", "default"},
			Type:        "int",
			Entity:      "user",
			Owner:       "",
			Description: "",
			Provider:    "test_provider",
		},
		metadata.TrainingSetDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "",
			Owner:       "",
			Provider:    "test_provider",
			Label:       metadata.NameVariant{"is_fraud", "default"},
			Features:    []metadata.NameVariant{{"user_transaction_count", "7d"}},
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
}

func TestCoordinatorTrainingSet(t *testing.T) {
	if testing.Short() {
		return
	}
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	var postgresConfig = PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: os.Getenv("POSTGRES_DB"),
		Username: os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
	}
	serialPGConfig := postgresConfig.Serialize()
	my_provider, err := provider.Get(Provider.PostgresOffline, serialPGConfig)
	if err != nil {
		t.Fatalf("could not get provider: %v", err)
	}
	my_offline, err := my_provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not get provider as offline store: %v", err)
	}
	offline_feature := provider.ResourceID{Name: "user_transaction_count", Variant: "7d", Type: provider.OfflineResourceType.Feature}
	featureTable, err := my_offline.CreateResourceTable(offline_feature, provider.PostgresTableSchema{Int})
	if err != nil {
		t.Fatalf("could not create feature table: %v", err)
	}
	if err := featureTable.Write(provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()}); err != nil {
		t.Fatalf("could not write to feature table")
	}
	offline_label := provider.ResourceID{Name: "is_fraud", Variant: "default", Type: provider.OfflineResourceType.Label}
	labelTable, err := my_offline.CreateResourceTable(offline_label, provider.PostgresTableSchema{Int})
	if err != nil {
		t.Fatalf("could not create label table: %v", err)
	}
	if err := labelTable.Write(provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()}); err != nil {
		t.Fatalf("could not write to label table")
	}
	if err := createTrainingSetWithProvider(client, serialPGConfig); err != nil {
		return err
	}
	ctx := context.Background()
	tsID := metadata.ResourceID{Name: "is_fraud", Variant: "default", Type: metadata.ResourceType.TRAINING_SET_VARIANT}
	tsCreated := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: "is_fraud", Variant: "default"})
	tsCreated.GetStatus()
	assert.Equal(t, ts_created.GetStatus(), ResourceStatus.Created, "Training set should be set to created with no coordinator running")
	coord, err := NewCoordinator(client, logger, cli)
	if err != nil {
		t.Fatalf("Failed to set up coordinator")
	}
	s, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		return err
	}
	job_key := fmt.Sprintf("JOB_%s", tsID)
	go coord.syncHandleJob(job_key, s)
	ts_pending := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: "is_fraud", Variant: "default"})
	assert.Equal(t, ts_pending.GetStatus(), ResourceStatus.Pending, "Training set should be set to pending once coordinator spawns")
	for job, err := client.GetJob(job_key); err != nil; {
		time.Sleep(1 * time.Second)
	}
	ts_complete := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: "is_fraud", Variant: "default"})
	assert.Equal(t, ts_complete.GetStatus(), ResourceStatus.Ready, "Training set should be set to ready once job completes")
	tsIterator, err := my_offline.GetTrainingSet(tsID)
	if err != nil {
		t.Fatalf("Coordinator did not create training set")
	}
	retrievedFeatures := tsIterator.Features()
	retrievedLabel := tsIterator.Label()
	assert.Equal(t, retrievedFeatures, []interface{}{1})
	assert.Equal(t, retrievedLabel, 1)
}
