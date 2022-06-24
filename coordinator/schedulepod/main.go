package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	db "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	//"reflect"
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

var metadata_addr = "featureform-metadata-server:8080"

func main() {
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

var testOfflineTableUpdateValues = [...]provider.ResourceRecord{
	provider.ResourceRecord{Entity: "a", Value: 6, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "b", Value: 7, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "c", Value: 8, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "d", Value: 9, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "e", Value: 10, TS: time.UnixMilli(1).UTC()},
}

var postgresConfig = provider.PostgresConfig{
	Host:     os.Getenv("POSTGRES_HOST"),
	Port:     "5432",
	Database: os.Getenv("POSTGRES_DB"),
	Username: os.Getenv("POSTGRES_USERNAME"),
	Password: os.Getenv("POSTGRES_PASSWORD"),
}

var redisPort = "6379"
var redisHost = os.Getenv("REDIS_HOST")

var etcdHost = os.Getenv("ETCD_HOST")
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
	fmt.Println(lis.Addr().String())
	return serv, lis.Addr().String()
}

func createNewCoordinator(addr string) (*coordinator.Coordinator, error) {
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return nil, err
	}
	etcdConnect := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}, Username: "root",
		Password: "secretpassword"})
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
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}, Username: "root",
		Password: "secretpassword"})
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
	fmt.Println("creating table in postgres")
	if err := CreateOriginalPostgresTable(originalTableName); err != nil {
		return err
	}
	sourceName := createSafeUUID()
	fmt.Println("create training set with provider")
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
	providerTsID := provider.ResourceID{Name: tsID.Name, Variant: tsID.Variant, Type: provider.TrainingSet}
	tsIterator, err := my_offline.GetTrainingSet(providerTsID)
	if err != nil {
		return fmt.Errorf("Coordinator did not create training set")
	}

	for i := 0; tsIterator.Next(); i++ {
		retrievedFeatures := tsIterator.Features()
		retrievedLabel := tsIterator.Label()
		fmt.Println("training set features", retrievedFeatures[0])
		fmt.Println("training set labels", retrievedLabel)
		// if !reflect.DeepEqual(retrievedFeatures[0], testOfflineTableValues[i].Value) {
		// 	return fmt.Errorf("Features not copied into training set")
		// }
		// if !reflect.DeepEqual(retrievedLabel, testOfflineTableValues[i].Value) {
		// 	return fmt.Errorf("Label not copied into training set")
		// }

	}
	tsFirstTimestamp, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	oldTimestamp := tsFirstTimestamp.LastUpdated()
	// if err := UpdateOriginalPostgresTable(originalTableName); err != nil {
	// 	return err
	// }
	for _, value := range testOfflineTableUpdateValues {
		if err := featureTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline feature table")
		}
	}
	for _, value := range testOfflineTableUpdateValues {
		if err := labelTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline label table")
		}
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
	providerTsID = provider.ResourceID{Name: tsID.Name, Variant: tsID.Variant, Type: provider.TrainingSet}
	tsIterator, err = my_offline.GetTrainingSet(providerTsID)
	if err != nil {
		return fmt.Errorf("Coordinator did not create training set")
	}
	for i := 0; tsIterator.Next(); i++ {
		retrievedFeatures := tsIterator.Features()
		retrievedLabel := tsIterator.Label()
		fmt.Println("training set features", retrievedFeatures[0])
		fmt.Println("training set label", retrievedLabel)
		// if !reflect.DeepEqual(retrievedFeatures[0], testOfflineTableUpdateValues[i].Value) {
		// 	return fmt.Errorf("Features not copied into training set")
		// }
		// if !reflect.DeepEqual(retrievedLabel, testOfflineTableUpdateValues[i].Value) {
		// 	return fmt.Errorf("Label not copied into training set")
		// }

	}
	tsSecondTimestamp, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	newTimestamp := tsSecondTimestamp.LastUpdated()
	if !oldTimestamp.Before(newTimestamp) {
		return fmt.Errorf("Resource update not signaled in metadata")
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
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}, Username: "root",
		Password: "secretpassword"})
	if err != nil {
		return err
	}
	defer cli.Close()
	serialPGConfig := postgresConfig.Serialize()
	if err != nil {
		return fmt.Errorf("could not get offline provider: %v", err)
	}
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
	onlineProvider, err := provider.Get(provider.RedisOnline, serialRedisConfig)
	if err != nil {
		return fmt.Errorf("could not get online provider")
	}
	onlineStore, err := onlineProvider.AsOnlineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as online store: %v", err)
	}
	featureName := createSafeUUID()
	sourceName := createSafeUUID()
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
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return err
	}
	if err := coord.ExecuteJob(metadata.GetJobKey(featureID)); err != nil {
		return err
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
		fmt.Println("online table value original", value)
		// if !reflect.DeepEqual(value, record.Value) {
		// 	return fmt.Errorf("Feature value did not materialize")
		// }
	}
	featureFirstTimestamp, err := client.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: featureName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	oldTimestamp := featureFirstTimestamp.LastUpdated()
	if err := UpdateOriginalPostgresTable(originalTableName); err != nil {
		return err
	}
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
	resourceTable, err = onlineStore.GetTable(featureName, "")
	if err != nil {
		return err
	}
	for _, record := range testOfflineTableValues {
		value, err := resourceTable.Get(record.Entity)
		if err != nil {
			return err
		}
		fmt.Println("online table value updated", value)
		// if !reflect.DeepEqual(value, record.Value) {
		// 	return fmt.Errorf("Feature value did not materialize")
		// }
	}
	featureSecondTimestamp, err := client.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: featureName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get online data")
	}
	newTimestamp := featureSecondTimestamp.LastUpdated()
	if !oldTimestamp.Before(newTimestamp) {
		return fmt.Errorf("Resource update not signaled in metadata")
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
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}, Username: "root",
		Password: "secretpassword"})
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
	if err := coord.ExecuteJob(metadata.GetJobKey(transformationID)); err != nil {
		return err
	}
	transformationFirstTimestamp, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	oldTimestamp := transformationFirstTimestamp.LastUpdated()
	if err := UpdateOriginalPostgresTable(tableName); err != nil {
		return err
	}
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
	transformationSecondTimestamp, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	newTimestamp := transformationSecondTimestamp.LastUpdated()
	if !oldTimestamp.Before(newTimestamp) {
		return fmt.Errorf("Resource update not signaled in metadata")
	}
	myProvider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		return fmt.Errorf("could not get provider: %v", err)
	}
	myOffline, err := myProvider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	providerJoinTransformationID := provider.ResourceID{Name: transformationName, Variant: "", Type: provider.Transformation}
	joinTransformationTable, err := myOffline.GetTransformationTable(providerJoinTransformationID)
	if err != nil {
		return err
	}
	transformationJoinName, err := provider.GetTransformationName(providerJoinTransformationID)
	if err != nil {
		return fmt.Errorf("invalid transformation table name: %v", err)
	}
	if joinTransformationTable.GetName() != transformationJoinName {
		return fmt.Errorf("Transformation table did not copy name")
	}
	joinTransformationIterator, err := joinTransformationTable.IterateSegment(int64(len(testOfflineTableValues) + len(testOfflineTableUpdateValues)))
	if err != nil {
		return err
	}
	i := 0
	for ; joinTransformationIterator.Next(); i++ {
		if joinTransformationIterator.Err() != nil {
			return err
		}
		joinTransformationTableRow := joinTransformationIterator.Values()
		// values := reflect.ValueOf(testOfflineTableValues[i])
		// newValues := reflect.ValueOf(testOfflineTableUpdateValues[i])
		fmt.Println("updated table value", joinTransformationTableRow)

	}

	providerSourceID := provider.ResourceID{Name: sourceName, Variant: "", Type: provider.Primary}
	primaryTable, err := myOffline.GetPrimaryTable(providerSourceID)
	primaryTableIterator, err := primaryTable.IterateSegment(int64(len(testOfflineTableValues) + len(testOfflineTableUpdateValues)))
	if err != nil {
		return err
	}
	i = 0
	for ; primaryTableIterator.Next(); i++ {
		if primaryTableIterator.Err() != nil {
			return err
		}
		primaryTableRow := primaryTableIterator.Values()
		// values := reflect.ValueOf(testOfflineTableValues[i])
		// newValues := reflect.ValueOf(testOfflineTableUpdateValues[i])
		fmt.Println("Updated primary table value")
		fmt.Println(primaryTableRow)
		// if primaryTableRow[j] != values.Field(j).Interface() {
		// 	return fmt.Errorf("Primary table value does not match original value")
		// }
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

func UpdateOriginalPostgresTable(tableName string) error {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresConfig.Username, postgresConfig.Password, postgresConfig.Host, postgresConfig.Port, postgresConfig.Database)
	ctx := context.Background()
	conn, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return err
	}
	for _, record := range testOfflineTableUpdateValues {
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
