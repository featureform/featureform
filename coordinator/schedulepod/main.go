package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"net"
	"os"
	//"reflect"
	db "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
	"strings"
	//"testing"
	"time"

	"github.com/featureform/coordinator"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/runner"
	//"github.com/jackc/pgx/v4/pgxpool"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

var metadata_addr string

func main() {
	serv, metadata_addr := startServ()
	defer serv.Stop()
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(metadata_addr, logger)
	if err != nil {
		logger.Errorf("could not set up metadata client: %v", err)
	}
	defer client.Close()
	eg := &errgroup.Group{}
	eg.Go(testScheduleTrainingSet)
	eg.Go(testScheduleTransformation)
	eg.Go(testScheduleFeatureMaterialization)
	if err := eg.Wait(); err != nil {
		logger.Errorf("Error", err)
	}
	fmt.Println("Completed successfully!")
}

func createSafeUUID() string {
	return strings.ReplaceAll(fmt.Sprintf("a%sa", uuid.New().String()), "-", "")
}

var testOfflineTableValues = [...]provider.ResourceRecord{
	provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "d", Value: 4, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "e", Value: 5, TS: time.UnixMilli(0).UTC()},
}

var postgresConfig = provider.PostgresConfig{
	Host:            "localhost",
	Port:            "5432",
	Database:        "postgresdb",
	Username:        "postgresadmin",
	Password:        "admin123",
	// Database: os.Getenv("POSTGRES_DB"),
	// Username: os.Getenv("POSTGRES_USER"),
	// Password: os.Getenv("POSTGRES_PASSWORD"),
}

// postgresPassword: "StrongPassword"
// username: "app1"
// password: "AppPassword"
// database: "app_db"

// var redisPort = os.Getenv("REDIS_PORT")
var redisPost = "6379"
var redisHost = "localhost"

var etcdHost = "localhost"
var etcdPort = "2379"

func startServ() (*metadata.MetadataServer, string) {
	logger := zap.NewExample().Sugar()
	storageProvider := metadata.EtcdStorageProvider{
		metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{etcdHost, etcdPort},
			},
		},
	}
	config := &metadata.Config{
		Logger:          logger,
		StorageProvider: storageProvider,
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

func createNewCoordinator(addr string) (*coordinator.Coordinator, error) {
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return nil, err
	}
	etcdConnect := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}})
	if err != nil {
		return nil, err
	}
	kubeJobSpawner := coordinator.KubernetesJobSpawner{}
	return coordinator.NewCoordinator(client, logger, cli, &kubeJobSpawner)
}

func testScheduleTrainingSet() error {
	if err := runner.RegisterFactory(string(runner.CREATE_TRAINING_SET), runner.TrainingSetRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	defer runner.UnregisterFactory(string(runner.CREATE_TRAINING_SET))
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(metadata_addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
	etcdConnect := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}})
	if err != nil {
		return err
	}
	defer cli.Close()
	featureName := createSafeUUID()
	labelName := createSafeUUID()
	tsName := createSafeUUID()
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
	originalTableName := createSafeUUID()
	if err := CreateOriginalPostgresTable(originalTableName); err != nil {
		return err
	}
	sourceName := createSafeUUID()
	if err := createTrainingSetWithProvider(client, serialPGConfig, sourceName, featureName, labelName, tsName, originalTableName, "*/1 * * * *"); err != nil {
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
	kubeJobSpawner := coordinator.KubernetesJobSpawner{}
	coord, err := coordinator.NewCoordinator(client, logger, cli, &kubeJobSpawner)
	if err != nil {
		return fmt.Errorf("Failed to set up coordinator")
	}
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return err
	}
	go func() {
		if err := coord.WatchForUpdateEvents(); err != nil {
			logger.Errorf("Error watching for new update events: %v", err)
		}
	}()
	if err := coord.ExecuteJob(metadata.GetJobKey(tsID)); err != nil {
		return err
	}
	time.Sleep(70 * time.Second)
	jobClient, err := runner.NewKubernetesJobClient(runner.GetCronJobName(tsID), runner.Namespace)
	if err != nil {
		return err
	}
	cronJob, err := jobClient.GetCronJob()
	if err != nil {
		return err
	}
	lastExecutionTime := cronJob.Status.LastSuccessfulTime
	if lastExecutionTime.IsZero() {
		return fmt.Errorf("job did not execute in time")
	}
	tsUpdated, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return err
	}
	tUpdateStatus := tsUpdated.UpdateStatus()
	tsLastUpdated := tUpdateStatus.LastUpdated
	if tsLastUpdated.AsTime().IsZero() {
		return fmt.Errorf("Scheduler did not update training set")
	}
	return nil
}

func testScheduleFeatureMaterialization() error {
	if err := runner.RegisterFactory(string(runner.COPY_TO_ONLINE), runner.MaterializedChunkRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	defer runner.UnregisterFactory(string(runner.COPY_TO_ONLINE))
	if err := runner.RegisterFactory(string(runner.MATERIALIZE), runner.MaterializeRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	defer runner.UnregisterFactory(string(runner.MATERIALIZE))
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(metadata_addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
	etcdConnect := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}})
	if err != nil {
		return err
	}
	defer cli.Close()
	serialPGConfig := postgresConfig.Serialize()
	offlineProvider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		return fmt.Errorf("could not get offline provider: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	liveAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	redisConfig := &provider.RedisConfig{
		Addr: liveAddr,
	}
	serialRedisConfig := redisConfig.Serialized()
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
	featureName := createSafeUUID()
	sourceName := createSafeUUID()
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
	originalTableName := createSafeUUID()
	if err := CreateOriginalPostgresTable(originalTableName); err != nil {
		return err
	}
	if err := materializeFeatureWithProvider(client, serialPGConfig, serialRedisConfig, featureName, sourceName, originalTableName, "*/1 * * * *"); err != nil {
		return fmt.Errorf("could not create online feature in metadata: %v", err)
	}
	if err := client.SetStatus(context.Background(), metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}, metadata.READY, ""); err != nil {
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
	kubeJobSpawner := coordinator.KubernetesJobSpawner{}
	coord, err := coordinator.NewCoordinator(client, logger, cli, &kubeJobSpawner)
	if err != nil {
		return fmt.Errorf("Failed to set up coordinator")
	}
	go func() {
		if err := coord.WatchForNewJobs(); err != nil {
			logger.Errorf("Error watching for new jobs: %v", err)
		}
	}()
	go func() {
		if err := coord.WatchForUpdateEvents(); err != nil {
			logger.Errorf("Error watching for new update events: %v", err)
		}
	}()
	time.Sleep(70 * time.Second)
	jobClient, err := runner.NewKubernetesJobClient(runner.GetCronJobName(featureID), runner.Namespace)
	if err != nil {
		return err
	}
	cronJob, err := jobClient.GetCronJob()
	if err != nil {
		return err
	}
	lastExecutionTime := cronJob.Status.LastSuccessfulTime
	if lastExecutionTime.IsZero() {
		return fmt.Errorf("job did not execute in time")
	}
	featureUpdated, err := client.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: featureID.Name, Variant: ""})
	if err != nil {
		return err
	}
	featureUpdateStatus := featureUpdated.UpdateStatus()
	featureLastUpdated := featureUpdateStatus.LastUpdated
	if featureLastUpdated.AsTime().IsZero() {
		return fmt.Errorf("Scheduler did not update feature")
	}
	return nil
}

func testScheduleTransformation() error {
	if err := runner.RegisterFactory(string(runner.CREATE_TRANSFORMATION), runner.CreateTransformationRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	defer runner.UnregisterFactory(string(runner.CREATE_TRANSFORMATION))
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(metadata_addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
	etcdConnect := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}})
	if err != nil {
		return err
	}
	defer cli.Close()
	tableName := createSafeUUID()
	serialPGConfig := postgresConfig.Serialize()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	if err := CreateOriginalPostgresTable(tableName); err != nil {
		return fmt.Errorf("Could not create non-featureform source table: %v", err)
	}
	sourceName := strings.Replace(createSafeUUID(), "-", "", -1)
	if err := createSourceWithProvider(client, serialPGConfig, sourceName, tableName); err != nil {
		return fmt.Errorf("could not register source in metadata: %v", err)
	}
	sourceCreated, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: sourceName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get source: %v", err)
	}
	if sourceCreated.Status() != metadata.CREATED {
		return fmt.Errorf("Source not set to created with no coordinator running")
	}
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	kubeJobSpawner := coordinator.KubernetesJobSpawner{}
	coord, err := coordinator.NewCoordinator(client, logger, cli, &kubeJobSpawner)
	if err != nil {
		return fmt.Errorf("Failed to set up coordinator")
	}
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return err
	}
	sourceComplete, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: sourceName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get source variant")
	}
	if metadata.READY != sourceComplete.Status() {
		return fmt.Errorf("source variant not set to ready once job completes")
	}
	transformationQuery := fmt.Sprintf("SELECT * FROM {{%s.}}", sourceName)
	transformationName := strings.Replace(createSafeUUID(), "-", "", -1)
	transformationID := metadata.ResourceID{Name: transformationName, Variant: "", Type: metadata.SOURCE_VARIANT}
	sourceNameVariants := []metadata.NameVariant{{Name: sourceName, Variant: ""}}
	if err := createTransformationWithProvider(client, serialPGConfig, transformationName, transformationQuery, sourceNameVariants, "*/1 * * * *"); err != nil {
		return err
	}
	transformationCreated, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get transformation: %v", err)
	}
	if transformationCreated.Status() != metadata.CREATED {
		return fmt.Errorf("Transformation not set to created with no coordinator running")
	}
	go func() {
		if err := coord.WatchForNewJobs(); err != nil {
			logger.Errorf("Error watching for new jobs: %v", err)
		}
	}()
	go func() {
		if err := coord.WatchForUpdateEvents(); err != nil {
			logger.Errorf("Error watching for new update events: %v", err)
		}
	}()
	time.Sleep(70 * time.Second)
	jobClient, err := runner.NewKubernetesJobClient(runner.GetCronJobName(transformationID), runner.Namespace)
	if err != nil {
		return err
	}
	cronJob, err := jobClient.GetCronJob()
	if err != nil {
		return err
	}
	lastExecutionTime := cronJob.Status.LastSuccessfulTime
	if lastExecutionTime.IsZero() {
		return fmt.Errorf("job did not execute in time")
	}
	transformationUpdated, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: transformationID.Name, Variant: ""})
	if err != nil {
		return err
	}
	transformationUpdateStatus := transformationUpdated.UpdateStatus()
	transformationLastUpdated := transformationUpdateStatus.LastUpdated
	if transformationLastUpdated.AsTime().IsZero() {
		return fmt.Errorf("Scheduler did not update transformation")
	}
	return nil
}

func CreateOriginalPostgresTable(tableName string) error {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresConfig.Username, postgresConfig.Password, postgresConfig.Host, postgresConfig.Port, postgresConfig.Database)
	ctx := context.Background()
	conn, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return err
	}
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value INT, ts TIMESTAMPTZ)", sanitize(tableName))
	if _, err := conn.Exec(context.Background(), createTableQuery); err != nil {
		return err
	}
	for _, record := range testOfflineTableValues {
		upsertQuery := fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES ($1, $2, $3)", sanitize(tableName))
		if _, err := conn.Exec(context.Background(), upsertQuery, record.Entity, record.Value, record.TS); err != nil {
			return err
		}
	}
	return nil
}

func createTrainingSetWithProvider(client *metadata.Client, config provider.SerializedConfig, sourceName string, featureName string, labelName string, tsName string, originalTableName string, schedule string) error {
	providerName := createSafeUUID()
	userName := createSafeUUID()
	entityName := createSafeUUID()
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
					Name: originalTableName,
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
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "value",
				TS:     "ts",
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
			Provider:    providerName,
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "value",
				TS:     "ts",
			},
		},
		metadata.TrainingSetDef{
			Name:        tsName,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    providerName,
			Label:       metadata.NameVariant{labelName, ""},
			Features:    []metadata.NameVariant{{featureName, ""}},
			Schedule:    schedule,
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

func materializeFeatureWithProvider(client *metadata.Client, offlineConfig provider.SerializedConfig, onlineConfig provider.SerializedConfig, featureName string, sourceName string, originalTableName string, schedule string) error {
	offlineProviderName := createSafeUUID()
	onlineProviderName := createSafeUUID()
	userName := createSafeUUID()
	entityName := createSafeUUID()
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
					Name: originalTableName,
				},
			},
			Schedule: "",
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
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "value",
				TS:     "ts",
			},
			Schedule: schedule,
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

func createSourceWithProvider(client *metadata.Client, config provider.SerializedConfig, sourceName string, tableName string) error {
	userName := createSafeUUID()
	providerName := createSafeUUID()
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
		metadata.SourceDef{
			Name:        sourceName,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    providerName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: tableName,
				},
			},
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

func createTransformationWithProvider(client *metadata.Client, config provider.SerializedConfig, sourceName string, transformationQuery string, sources []metadata.NameVariant, schedule string) error {
	userName := createSafeUUID()
	providerName := createSafeUUID()
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
		metadata.SourceDef{
			Name:        sourceName,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    providerName,
			Definition: metadata.TransformationSource{
				TransformationType: metadata.SQLTransformationType{
					Query:   transformationQuery,
					Sources: sources,
				},
			},
			Schedule: schedule,
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}
