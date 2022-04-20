package coordinator

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func createTrainingSetWithProvider(client *metadata.Client, config provider.SerializedConfig)

	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Simba Khadder",
		},
		metadata.ProviderDef{
			Name:        "test_provider",
			Description: "",
			Type:        "",
			Software:    "",
			Team:        "",
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
			Type:        "",
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
			Owner:       "Simba Khadder",
			Description: "Number of transcations the user performed in the last 7 days.",
			Provider:    "demo-s3",
		},
		metadata.TrainingSetDef{
			Name:        "is_fraud",
			Variant:     "default",
			Description: "if a transaction is fraud",
			Owner:       "Simba Khadder",
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
	//populate actual training set data here
	if err := createTrainingSetWithProvider(client, serialPGConfig); err != nil {
		return err
	}
	ctx := context.Background()
	ts_id := metadata.ResourceID{Name: "is_fraud", Variant: "default", Type: metadata.ResourceType.TRAINING_SET_VARIANT}
	ts_created := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: "is_fraud", Variant: "default"})
	ts_created.GetStatus()
	assert.Equal(t, ts_created.GetStatus(), ResourceStatus.Created, "Training set should be set to created with no coordinator running")
	coord, err := NewCoordinator(client, logger, cli)
	if err != nil {
		t.Fatalf("Failed to set up coordinator")
	}
	s, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		return err
	}
	job_key := fmt.Sprintf("JOB_%s",ts_id)
	go coord.syncHandleJob(job_key, s)
	ts_pending := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: "is_fraud", Variant: "default"})
	assert.Equal(t, ts_pending.GetStatus(), ResourceStatus.Pending, "Training set should be set to pending once coordinator spawns")
	for job, err := client.GetJob(job_key); err != nil {
		time.Sleep(1 * time.Second)
	}
	ts_complete := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: "is_fraud", Variant: "default"})
	assert.Equal(t, ts_complete.GetStatus(), ResourceStatus.Ready, "Training set should be set to ready once job completes")
}

