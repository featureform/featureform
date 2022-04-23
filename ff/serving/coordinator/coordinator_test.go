package coordinator

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/featureform/serving/metadata"
	provider "github.com/featureform/serving/provider"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func createTrainingSetWithProvider(client *metadata.Client, config provider.SerializedConfig, featureName string, labelName string, tsName string) error {
	providerName := uuid.New().String()
	userName := uuid.New().String()
	sourceName := uuid.New().String()
	entityName := uuid.New().String()
	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             providerName,
			Description:      "",
			Type:             "POSTGRES_OFFLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: config,
		},
		metadata.EntityDef{
			Name:        entityName,
			Description: "",
		},
		metadata.SourceDef{
			Name:        sourceName,
			Variant:     "",
			Description: "",
			Type:        "",
			Owner:       userName,
			Provider:    providerName,
		},
		metadata.LabelDef{
			Name:        labelName,
			Variant:     "",
			Description: "",
			Type:        "int",
			Source:      metadata.NameVariant{sourceName, ""},
			Entity:      entityName,
			Owner:       userName,
			Provider:    providerName,
		},
		metadata.FeatureDef{
			Name:        featureName,
			Variant:     "",
			Source:      metadata.NameVariant{sourceName, ""},
			Type:        "int",
			Entity:      entityName,
			Owner:       userName,
			Description: "",
			Provider:    providerName,
		},
		metadata.TrainingSetDef{
			Name:        tsName,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    providerName,
			Label:       metadata.NameVariant{labelName, ""},
			Features:    []metadata.NameVariant{{featureName, ""}},
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
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
	var postgresConfig = provider.PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: os.Getenv("POSTGRES_DB"),
		Username: os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
	}
	featureName := uuid.New().String()
	labelName := uuid.New().String()
	tsName := uuid.New().String()

	serialPGConfig := postgresConfig.Serialize()
	my_provider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		t.Fatalf("could not get provider: %v", err)
	}
	my_offline, err := my_provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not get provider as offline store: %v", err)
	}
	offline_feature := provider.ResourceID{Name: featureName, Variant: "", Type: provider.Feature}
	postGresIntSchema := provider.PostgresTableSchema{provider.Int}
	serializedPostgresSchema := postGresIntSchema.Serialize()
	featureTable, err := my_offline.CreateResourceTable(offline_feature, serializedPostgresSchema)
	if err != nil {
		t.Fatalf("could not create feature table: %v", err)
	}
	if err := featureTable.Write(provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()}); err != nil {
		t.Fatalf("could not write to feature table")
	}
	offline_label := provider.ResourceID{Name: labelName, Variant: "", Type: provider.Label}
	labelTable, err := my_offline.CreateResourceTable(offline_label, serializedPostgresSchema)
	if err != nil {
		t.Fatalf("could not create label table: %v", err)
	}
	if err := labelTable.Write(provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()}); err != nil {
		t.Fatalf("could not write to label table")
	}
	if err := createTrainingSetWithProvider(client, serialPGConfig, featureName, labelName, tsName); err != nil {
		t.Fatalf("could not create training set %v", err)
	}
	ctx := context.Background()
	tsID := metadata.ResourceID{Name: tsName, Variant: "", Type: metadata.TRAINING_SET_VARIANT}
	tsCreated, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		t.Fatalf("could not get training set")
	}
	assert.Equal(t, tsCreated.Status(), metadata.CREATED, "Training set should be set to created with no coordinator running")
	memJobSpawner := MemoryJobSpawner{}
	coord, err := NewCoordinator(client, logger, cli, &memJobSpawner)
	if err != nil {
		t.Fatalf("Failed to set up coordinator")
	}
	s, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		t.Fatalf("could not create new session")
	}
	go coord.executeJob(metadata.GetJobKey(tsID), s)
	for has, _ := coord.hasJob(tsID); has; has, _ = coord.hasJob(tsID) {
		time.Sleep(1 * time.Second)
	}
	ts_complete, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		t.Fatalf("could not get training set variant")
	}
	assert.Equal(t, metadata.READY, ts_complete.Status(), "Training set should be set to ready once job completes")
	providerTsID := provider.ResourceID{Name: tsID.Name, Variant: tsID.Variant, Type: provider.TrainingSet}
	tsIterator, err := my_offline.GetTrainingSet(providerTsID)
	if err != nil {
		t.Fatalf("Coordinator did not create training set")
	}
	tsIterator.Next()
	retrievedFeatures := tsIterator.Features()
	retrievedLabel := tsIterator.Label()
	assert.Equal(t, []interface{}{1}, retrievedFeatures, "Features should be copied into training set")
	assert.Equal(t, 1, retrievedLabel, "Label should be copied into training set")
}
