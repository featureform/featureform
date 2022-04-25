package coordinator

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"os"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/featureform/serving/metadata"
	provider "github.com/featureform/serving/provider"
	runner "github.com/featureform/serving/runner"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var testOfflineTableValues = [...]provider.ResourceRecord{
	provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "d", Value: 4, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "e", Value: 5, TS: time.UnixMilli(0).UTC()},
}

func setupMetadataServer() error {
	logger := zap.NewExample().Sugar()
	addr := ":8080"
	storageProvider := metadata.EtcdStorageProvider{
		metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{"localhost", "2379"},
			},
		},
	}
	config := &metadata.Config{
		Logger:          logger,
		Address:         addr,
		StorageProvider: storageProvider,
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		return err
	}
	if err := server.Serve(); err != nil {
		return err
	}
	return nil
}

func TestCoordinatorCalls(t *testing.T) {
	//needs etcd and providers set up to run
	if testing.Short() {
		return
	}
	go setupMetadataServer()
	logger := zap.NewExample().Sugar()
	_, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		t.Fatalf("could not set up metadata client: %v", err)
	}
	if err := testCoordinatorMaterializeFeature(); err != nil {
		t.Fatalf("coordinator could not materialize feature: %v", err)
	}
	if err := testCoordinatorTrainingSet(); err != nil {
		t.Fatalf("coordinator could not create training set: %v", err)
	}
	//g.Go(testCoordinatorCreateTransformation)
}

// func createTransformationWithProvider(client *metadata.Client, config provider.SerializedConfig, tsName string) {

// }

func materializeFeatureWithProvider(client *metadata.Client, offlineConfig provider.SerializedConfig, onlineConfig provider.SerializedConfig, featureName string, sourceName string) error {
	offlineProviderName := uuid.New().String()
	onlineProviderName := uuid.New().String()
	userName := uuid.New().String()
	entityName := uuid.New().String()
	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             offlineProviderName,
			Description:      "",
			Type:             "POSTGRES_OFFLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: offlineConfig,
		},
		metadata.ProviderDef{
			Name:             onlineProviderName,
			Description:      "",
			Type:             "REDIS_ONLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: onlineConfig,
		},
		metadata.EntityDef{
			Name:        entityName,
			Description: "",
		},
		metadata.SourceDef{
			Name:        sourceName,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    offlineProviderName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "mockPrimary",
				},
			},
		},
		metadata.FeatureDef{
			Name:        featureName,
			Variant:     "",
			Source:      metadata.NameVariant{sourceName, ""},
			Type:        string(provider.Int),
			Entity:      entityName,
			Owner:       userName,
			Description: "",
			Provider:    onlineProviderName,
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

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
			Owner:       userName,
			Provider:    providerName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "mockPrimary",
				},
			},
		},
		metadata.LabelDef{
			Name:        labelName,
			Variant:     "",
			Description: "",
			Type:        string(provider.Int),
			Source:      metadata.NameVariant{sourceName, ""},
			Entity:      entityName,
			Owner:       userName,
			Provider:    providerName,
		},
		metadata.FeatureDef{
			Name:        featureName,
			Variant:     "",
			Source:      metadata.NameVariant{sourceName, ""},
			Type:        string(provider.Int),
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

// func testCoordinatorCreateTransformation() error {
//it's morphin time
// 	return nil
// }

func testCoordinatorTrainingSet() error {
	if err := runner.RegisterFactory(string(runner.CREATE_TRAINING_SET), runner.TrainingSetRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		return err
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
		return fmt.Errorf("could not get provider: %v", err)
	}
	my_offline, err := my_provider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	offline_feature := provider.ResourceID{Name: featureName, Variant: "", Type: provider.Feature}
	schemaInt := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "entity", ValueType: provider.String},
			{Name: "value", ValueType: provider.Int},
			{Name: "ts", ValueType: provider.Timestamp},
		},
	}
	featureTable, err := my_offline.CreateResourceTable(offline_feature, schemaInt)
	if err != nil {
		return fmt.Errorf("could not create feature table: %v", err)
	}
	for _, value := range testOfflineTableValues {
		if err := featureTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline feature table")
		}
	}
	offline_label := provider.ResourceID{Name: labelName, Variant: "", Type: provider.Label}
	labelTable, err := my_offline.CreateResourceTable(offline_label, schemaInt)
	if err != nil {
		return fmt.Errorf("could not create label table: %v", err)
	}
	for _, value := range testOfflineTableValues {
		if err := labelTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline label table")
		}
	}
	if err := createTrainingSetWithProvider(client, serialPGConfig, featureName, labelName, tsName); err != nil {
		return fmt.Errorf("could not create training set %v", err)
	}
	ctx := context.Background()
	tsID := metadata.ResourceID{Name: tsName, Variant: "", Type: metadata.TRAINING_SET_VARIANT}
	tsCreated, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	if tsCreated.Status() != metadata.CREATED {
		return fmt.Errorf("Training set not set to created with no coordinator running")
	}
	memJobSpawner := MemoryJobSpawner{}
	coord, err := NewCoordinator(client, logger, cli, &memJobSpawner)
	if err != nil {
		return fmt.Errorf("Failed to set up coordinator")
	}
	go func() {
		if err := coord.WatchForNewJobs(); err != nil {
			panic(err)
		}
	}()
	for has, _ := coord.hasJob(tsID); has; has, _ = coord.hasJob(tsID) {
		time.Sleep(1 * time.Second)
	}
	ts_complete, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set variant")
	}
	if metadata.READY != ts_complete.Status() {
		return fmt.Errorf("Training set not set to ready once job completes")
	}
	providerTsID := provider.ResourceID{Name: tsID.Name, Variant: tsID.Variant, Type: provider.TrainingSet}
	tsIterator, err := my_offline.GetTrainingSet(providerTsID)
	if err != nil {
		return fmt.Errorf("Coordinator did not create training set")
	}
	for i := 0; tsIterator.Next(); i++ {
		retrievedFeatures := tsIterator.Features()
		retrievedLabel := tsIterator.Label()
		if !reflect.DeepEqual(retrievedFeatures[0], testOfflineTableValues[i].Value) {
			return fmt.Errorf("Features not copied into training set")
		}
		if !reflect.DeepEqual(retrievedLabel, testOfflineTableValues[i].Value) {
			return fmt.Errorf("Label not copied into training set")
		}

	}
	return nil
}

func testCoordinatorMaterializeFeature() error {
	if err := runner.RegisterFactory(string(runner.COPY_TO_ONLINE), runner.MaterializedChunkRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		return err
	}
	defer cli.Close()
	var postgresConfig = provider.PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: os.Getenv("POSTGRES_DB"),
		Username: os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
	}
	serialPGConfig := postgresConfig.Serialize()
	offlineProvider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		return fmt.Errorf("could not get offline provider: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	redisPort := os.Getenv("REDIS_PORT")
	redisHost := "localhost"
	liveAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	redisConfig := &provider.RedisConfig{
		Addr: liveAddr,
	}
	serialRedisConfig := redisConfig.Serialized()
	p, err := provider.Get(provider.RedisOnline, serialRedisConfig)
	if err != nil {
		return fmt.Errorf("could not get online provider: %v", err)
	}
	onlineStore, err := p.AsOnlineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as online store")
	}
	schemaInt := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "entity", ValueType: provider.String},
			{Name: "value", ValueType: provider.Int},
			{Name: "ts", ValueType: provider.Timestamp},
		},
	}
	featureName := uuid.New().String()
	sourceName := uuid.New().String()
	offlineFeature := provider.ResourceID{Name: featureName, Variant: "", Type: provider.Feature}
	featureTable, err := offlineStore.CreateResourceTable(offlineFeature, schemaInt)
	if err != nil {
		return fmt.Errorf("could not create feature table: %v", err)
	}
	for _, value := range testOfflineTableValues {
		if err := featureTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline feature table")
		}
	}
	if err := materializeFeatureWithProvider(client, serialPGConfig, serialRedisConfig, featureName, sourceName); err != nil {
		return fmt.Errorf("could not create online feature in metadata: %v", err)
	}
	if err := client.SetStatus(context.Background(), metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}, metadata.READY); err != nil {
		return err
	}
	featureID := metadata.ResourceID{Name: featureName, Variant: "", Type: metadata.FEATURE_VARIANT}
	featureCreated, err := client.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: featureName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get feature: %v", err)
	}
	if featureCreated.Status() != metadata.CREATED {
		return fmt.Errorf("Feature not set to created with no coordinator running")
	}
	memJobSpawner := MemoryJobSpawner{}
	coord, err := NewCoordinator(client, logger, cli, &memJobSpawner)
	if err != nil {
		return fmt.Errorf("Failed to set up coordinator")
	}
	go func() {
		if err := coord.WatchForNewJobs(); err != nil {
			panic(err)
		}
	}()
	for has, _ := coord.hasJob(featureID); has; has, _ = coord.hasJob(featureID) {
		time.Sleep(1 * time.Second)
	}
	featureComplete, err := client.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: featureName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get feature variant")
	}
	if metadata.READY != featureComplete.Status() {
		return fmt.Errorf("Feature not set to ready once job completes")
	}
	resourceTable, err := onlineStore.GetTable(featureName, "")
	if err != nil {
		return err
	}
	for _, record := range testOfflineTableValues {
		value, err := resourceTable.Get(record.Entity)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(value, record.Value) {
			return fmt.Errorf("Feature value did not materialize")
		}
	}
	return nil
}
