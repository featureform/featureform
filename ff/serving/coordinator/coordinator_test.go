package coordinator

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
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

var postgresConfig = provider.PostgresConfig{
	Host:     "localhost",
	Port:     "5432",
	Database: os.Getenv("POSTGRES_DB"),
	Username: os.Getenv("POSTGRES_USER"),
	Password: os.Getenv("POSTGRES_PASSWORD"),
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
	if err := testRegisterPrimaryTableFromSource(); err != nil {
		t.Fatalf("coordinator could not register primary table from source: %v", err)
	}
	if err := testRegisterTransformationFromSource(); err != nil {
		t.Fatalf("coordinator could not register transformation from source and transformation: %v", err)
	}
}

func materializeFeatureWithProvider(client *metadata.Client, offlineConfig provider.SerializedConfig, onlineConfig provider.SerializedConfig, featureName string, sourceName string, originalTableName string) error {
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
					Name: originalTableName,
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
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "value",
				TS:     "ts",
			},
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

func createSourceWithProvider(client *metadata.Client, config provider.SerializedConfig, sourceName string, tableName string) error {
	userName := uuid.New().String()
	providerName := uuid.New().String()
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

func createTransformationWithProvider(client *metadata.Client, config provider.SerializedConfig, sourceName string, transformationQuery string, sources []metadata.NameVariant) error {
	userName := uuid.New().String()
	providerName := uuid.New().String()
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
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

func createTrainingSetWithProvider(client *metadata.Client, config provider.SerializedConfig, featureName string, labelName string, tsName string, originalTableName string) error {
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
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		return err
	}
	return nil
}

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
	originalTableName := uuid.New().String()
	if err := CreateOriginalPostgresTable(originalTableName); err != nil {
		return err
	}
	if err := createTrainingSetWithProvider(client, serialPGConfig, featureName, labelName, tsName, originalTableName); err != nil {
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
	originalTableName := uuid.New().String()
	if err := CreateOriginalPostgresTable(originalTableName); err != nil {
		return err
	}
	if err := materializeFeatureWithProvider(client, serialPGConfig, serialRedisConfig, featureName, sourceName, originalTableName); err != nil {
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

//I control the data

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

func testRegisterPrimaryTableFromSource() error {
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
	tableName := uuid.New().String()
	serialPGConfig := postgresConfig.Serialize()
	myProvider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		return fmt.Errorf("could not get provider: %v", err)
	}
	myOffline, err := myProvider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	if err := CreateOriginalPostgresTable(tableName); err != nil {
		return fmt.Errorf("Could not create non-featureform source table: %v", err)
	}
	//use the postgres/whatever to make a blank agnostic table "sammy's table",
	sourceName := uuid.New().String()
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
	//now we set up the coordinator and actually do shit
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
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
	for has, _ := coord.hasJob(sourceID); has; has, _ = coord.hasJob(sourceID) {
		time.Sleep(1 * time.Second)
	}
	sourceComplete, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: sourceName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get source variant")
	}
	if metadata.READY != sourceComplete.Status() {
		return fmt.Errorf("source variant not set to ready once job completes")
	}
	providerSourceID := provider.ResourceID{Name: sourceName, Variant: "", Type: provider.Primary}
	primaryTable, err := myOffline.GetPrimaryTable(providerSourceID)
	if err != nil {
		return fmt.Errorf("Coordinator did not create primary table")
	}
	if primaryTable.GetName() != provider.GetPrimaryTableName(providerSourceID) {
		return fmt.Errorf("Primary table did not copy name")
	}
	numRows, err := primaryTable.NumRows()
	if err != nil {
		return fmt.Errorf("Could not get num rows from primary table")
	}
	if int(numRows) != len(testOfflineTableValues) {
		return fmt.Errorf("primary table did not copy correct number of rows")
	}
	primaryTableIterator, err := primaryTable.IterateSegment(int64(len(testOfflineTableValues)))
	if err != nil {
		return err
	}
	i := 0
	for ; primaryTableIterator.Next(); i++ {
		if primaryTableIterator.Err() != nil {
			return err
		}
		primaryTableRow := primaryTableIterator.Values()
		values := reflect.ValueOf(testOfflineTableValues[i])
		for j := 0; j < values.NumField(); j++ {
			if primaryTableRow[j] != values.Field(j).Interface() {
				return fmt.Errorf("Primary table value does not match original value")
			}
		}
	}
	if i != len(testOfflineTableValues) {
		return fmt.Errorf("primary table did not copy all rows")
	}
	return nil
}

func testRegisterTransformationFromSource() error {
	/////////// this code is copied from the above, make into its own function
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
	tableName := uuid.New().String()
	serialPGConfig := postgresConfig.Serialize()
	myProvider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		return fmt.Errorf("could not get provider: %v", err)
	}
	myOffline, err := myProvider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("could not get provider as offline store: %v", err)
	}
	if err := CreateOriginalPostgresTable(tableName); err != nil {
		return fmt.Errorf("Could not create non-featureform source table: %v", err)
	}
	sourceName := strings.Replace(uuid.New().String(), "-", "", -1)
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
	memJobSpawner := MemoryJobSpawner{}
	coord, err := NewCoordinator(client, logger, cli, &memJobSpawner)
	if err != nil {
		return fmt.Errorf("Failed to set up coordinator")
	}
	if err := coord.executeJob(metadata.GetJobKey(sourceID)); err != nil {
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
	transformationName := strings.Replace(uuid.New().String(), "-", "", -1)
	transformationID := metadata.ResourceID{Name: transformationName, Variant: "", Type: metadata.SOURCE_VARIANT}
	sourceNameVariants := []metadata.NameVariant{{Name: sourceName, Variant: ""}}
	if err := createTransformationWithProvider(client, serialPGConfig, transformationName, transformationQuery, sourceNameVariants); err != nil {
		return err
	}
	transformationCreated, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get transformation: %v", err)
	}
	if transformationCreated.Status() != metadata.CREATED {
		return fmt.Errorf("Transformation not set to created with no coordinator running")
	}
	if err := coord.executeJob(metadata.GetJobKey(transformationID)); err != nil {
		return err
	}
	transformationComplete, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get source variant")
	}
	if metadata.READY != transformationComplete.Status() {
		return fmt.Errorf("transformation variant not set to ready once job completes")
	}
	providerTransformationID := provider.ResourceID{Name: transformationName, Variant: "", Type: provider.Transformation}
	transformationTable, err := myOffline.GetTransformationTable(providerTransformationID)
	if err != nil {
		return err
	}
	if transformationTable.GetName() != provider.GetTransformationName(providerTransformationID) {
		return fmt.Errorf("Transformation table did not copy name")
	}
	numRows, err := transformationTable.NumRows()
	if err != nil {
		return fmt.Errorf("Could not get num rows from transformation table")
	}
	if int(numRows) != len(testOfflineTableValues) {
		return fmt.Errorf("transformation table did not copy correct number of rows")
	}
	transformationIterator, err := transformationTable.IterateSegment(int64(len(testOfflineTableValues)))
	if err != nil {
		return err
	}
	i := 0
	for ; transformationIterator.Next(); i++ {
		if transformationIterator.Err() != nil {
			return err
		}
		transformationTableRow := transformationIterator.Values()
		values := reflect.ValueOf(testOfflineTableValues[i])
		for j := 0; j < values.NumField(); j++ {
			if transformationTableRow[j] != values.Field(j).Interface() {
				return fmt.Errorf("Transformation table value does not match original value")
			}
		}
	}
	if i != len(testOfflineTableValues) {
		return fmt.Errorf("transformation table did not copy all rows")
	}
	//now make a new transformation with two sources, one the original source, and the other the transformation
	joinTransformationQuery := fmt.Sprintf("SELECT {{%s.}}.entity, {{%s.}}.value, {{%s.}}.ts FROM {{%s.}} INNER JOIN {{%s.}} ON {{%s.}}.entity = {{%s.}}.entity", sourceName, sourceName, sourceName, sourceName, transformationName, sourceName, transformationName)
	joinTransformationName := strings.Replace(uuid.New().String(), "-", "", -1)
	joinTransformationID := metadata.ResourceID{Name: joinTransformationName, Variant: "", Type: metadata.SOURCE_VARIANT}
	joinSourceNameVariants := []metadata.NameVariant{{Name: sourceName, Variant: ""}, {Name: transformationName, Variant: ""}}
	if err := createTransformationWithProvider(client, serialPGConfig, joinTransformationName, joinTransformationQuery, joinSourceNameVariants); err != nil {
		return err
	}
	joinTransformationCreated, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: joinTransformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get transformation: %v", err)
	}
	if joinTransformationCreated.Status() != metadata.CREATED {
		return fmt.Errorf("Transformation not set to created with no coordinator running")
	}

	if err := coord.executeJob(metadata.GetJobKey(joinTransformationID)); err != nil {
		return err
	}
	joinTransformationComplete, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: joinTransformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get source variant")
	}
	if metadata.READY != joinTransformationComplete.Status() {
		return fmt.Errorf("transformation variant not set to ready once job completes")
	}
	providerJoinTransformationID := provider.ResourceID{Name: transformationName, Variant: "", Type: provider.Transformation}
	joinTransformationTable, err := myOffline.GetTransformationTable(providerJoinTransformationID)
	if err != nil {
		return err
	}
	if joinTransformationTable.GetName() != provider.GetTransformationName(providerJoinTransformationID) {
		return fmt.Errorf("Transformation table did not copy name")
	}
	numRows, err = joinTransformationTable.NumRows()
	if err != nil {
		return fmt.Errorf("Could not get num rows from transformation table")
	}
	if int(numRows) != len(testOfflineTableValues) {
		return fmt.Errorf("transformation table did not copy correct number of rows")
	}
	joinTransformationIterator, err := joinTransformationTable.IterateSegment(int64(len(testOfflineTableValues)))
	if err != nil {
		return err
	}
	i = 0
	for ; joinTransformationIterator.Next(); i++ {
		if joinTransformationIterator.Err() != nil {
			return err
		}
		joinTransformationTableRow := joinTransformationIterator.Values()
		values := reflect.ValueOf(testOfflineTableValues[i])
		for j := 0; j < values.NumField(); j++ {
			if joinTransformationTableRow[j] != values.Field(j).Interface() {
				return fmt.Errorf("Transformation table value does not match original value")
			}
		}
	}
	if i != len(testOfflineTableValues) {
		return fmt.Errorf("transformation table did not copy all rows")
	}

	return nil
}

// func TestRegisterTransformationFromSource(t *testing.T) {
// 	tableName := uuid.New().String()
// 	if err := CreateOriginalPostgresTable(tableName); err != nil {
// 		t.Fatalf("Could not create non-featureform source table")
// 	}
// 	//basically the same but we add a sql query or sometin
// 	//we make two sources, one a source, one a transformation form that source
// 	//then we make a transformation with two sources, one the original source and one the transformation
// }

//RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error)
//here we do the The Rock thing
//this is how we reconcile the new definition
//current materialize and training set job assumes resource tables exist in offline store... but do they?
//whether they do should be defined in metadata, though it works so far at least

//also create primary table..... if there is no primarySQLtable name (so it's just a blank table)
