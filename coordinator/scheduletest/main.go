package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/featureform/coordinator"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/runner"

	"github.com/google/uuid"
	db "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func createSafeUUID() string {
	return strings.ReplaceAll(fmt.Sprintf("a%sa", uuid.New().String()), "-", "")
}

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

var ctx = context.Background()

var testOfflineTableValues = []provider.ResourceRecord{
	provider.ResourceRecord{Entity: "a", Value: 1, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "b", Value: 2, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "c", Value: 3, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "d", Value: 4, TS: time.UnixMilli(0).UTC()},
	provider.ResourceRecord{Entity: "e", Value: 5, TS: time.UnixMilli(0).UTC()},
}

var testOfflineTableUpdateValues = []provider.ResourceRecord{
	provider.ResourceRecord{Entity: "a", Value: 6, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "b", Value: 7, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "c", Value: 8, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "d", Value: 9, TS: time.UnixMilli(1).UTC()},
	provider.ResourceRecord{Entity: "e", Value: 10, TS: time.UnixMilli(1).UTC()},
}

var finalUpdatedTableValues = append(testOfflineTableValues, testOfflineTableUpdateValues...)

var postgresConfig = provider.PostgresConfig{
	Host:     os.Getenv("POSTGRES_HOST"),
	Port:     os.Getenv("POSTGRES_PORT"),
	Database: os.Getenv("POSTGRES_DB"),
	Username: os.Getenv("POSTGRES_USERNAME"),
	Password: os.Getenv("POSTGRES_PASSWORD"),
}

var redisPort = os.Getenv("REDIS_PORT")
var redisHost = os.Getenv("REDIS_HOST")

var etcdHost = os.Getenv("ETCD_HOST")
var etcdPort = os.Getenv("ETCD_PORT")

var etcdUsername = os.Getenv("ETCD_USERNAME")
var etcdPassword = os.Getenv("ETCD_PASSWORD")

var metadataAddress = fmt.Sprintf("%s:%s", os.Getenv("METADATA_HOST"), os.Getenv("METADATA_PORT"))

var serialPGConfig []byte
var offlinePostgresStore provider.OfflineStore

var serialRedisConfig []byte
var onlineRedisStore provider.OnlineStore

var updateEveryMinuteSchedule = "*/1 * * * *"
var updateEveryTwoMinutesSchedule = "*/2 * * * *"

var metadataClient *metadata.Client
var logger *zap.SugaredLogger
var etcdClient *clientv3.Client

var coord *coordinator.Coordinator

func main() {
	// initialize providers and clients
	if err := initializeTestingEnvironment(); err != nil {
		logger.Fatalf("Could not initialize testing environment: %v", err)
	}
	// have coordinator watch for resource update events
	go func() {
		if err := coord.WatchForUpdateEvents(); err != nil {
			logger.Errorf("Error watching for new update events: %v", err)
		}
	}()
	// have coordinator watch for schedule change requests
	go func() {
		if err := coord.WatchForScheduleChanges(); err != nil {
			logger.Errorf("Error watching for schedule changes: %v", err)
		}
	}()
	// run tests async
	eg := &errgroup.Group{}
	eg.Go(testScheduleTrainingSet)
	eg.Go(testScheduleTransformation)
	eg.Go(testScheduleFeatureMaterialization)
	eg.Go(testUpdateExistingSchedule)
	if err := eg.Wait(); err != nil {
		logger.Fatalw("Error", err)
	}
	metadataClient.Close()
	etcdClient.Close()
	logger.Info("Completed successfully!")
}

func initializeTestingEnvironment() error {
	logger = zap.NewExample().Sugar()
	var err error
	metadataClient, err = metadata.NewClient(metadataAddress, logger)
	if err != nil {
		return fmt.Errorf("could not set up metadata client: %v", err)
	}
	etcdConnect := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	etcdClient, err = clientv3.New(clientv3.Config{Endpoints: []string{etcdConnect}, Username: etcdUsername,
		Password: etcdPassword})
	if err != nil {
		return fmt.Errorf("Could not connect to etcd: %v", err)
	}
	serialPGConfig = postgresConfig.Serialize()
	offlineProvider, err := provider.Get(provider.PostgresOffline, serialPGConfig)
	if err != nil {
		return fmt.Errorf("Could not get provider: %v", err)
	}
	offlinePostgresStore, err = offlineProvider.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("Could not convert provider to offline store: %v", err)
	}
	liveAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	redisConfig := &provider.RedisConfig{
		Addr: liveAddr,
	}
	serialRedisConfig = redisConfig.Serialized()
	onlineProvider, err := provider.Get(provider.RedisOnline, serialRedisConfig)
	if err != nil {
		return fmt.Errorf("Could not get online provider: %v", err)
	}
	onlineRedisStore, err = onlineProvider.AsOnlineStore()
	if err != nil {
		return fmt.Errorf("Could not convert provider to online store: %v", err)
	}
	kubeJobSpawner := coordinator.KubernetesJobSpawner{}
	coord, err = coordinator.NewCoordinator(metadataClient, logger, etcdClient, &kubeJobSpawner)
	if err != nil {
		return fmt.Errorf("Could not initialize coordinator: %v", err)
	}
	return nil
}

// TESTS

func testScheduleTrainingSet() error {
	// initialize random resource names
	featureName := createSafeUUID()
	labelName := createSafeUUID()
	tsName := createSafeUUID()
	originalTableName := createSafeUUID()
	sourceName := createSafeUUID()
	tsID := metadata.ResourceID{Name: tsName, Variant: "", Type: metadata.TRAINING_SET_VARIANT}
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	// create original feature and label tables for training set with original data
	featureTable, labelTable, err := initializeResourceTablesForTrainingSet(featureName, labelName)
	if err != nil {
		return fmt.Errorf("Could not initialize resource tables: %v", err)
	}
	// initialize training set in metadata
	if err := createTrainingSetWithProvider(sourceName, featureName, labelName, tsName, originalTableName, updateEveryMinuteSchedule); err != nil {
		return fmt.Errorf("could not create training set %v", err)
	}
	// initialize source in offline store via provider
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return err
	}
	// initialize traiining set in offline store via coordinator
	if err := coord.ExecuteJob(metadata.GetJobKey(tsID)); err != nil {
		return err
	}
	// check that original values are correctly set in offline store training set
	if err := checkValuesCorrectlySet(tsID, testOfflineTableValues); err != nil {
		return fmt.Errorf("Original training set values not set properly: %v", err)
	}
	// get original metadata timestamp for training set, before update
	tsBeforeUpdate, err := metadataClient.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set: %v", err)
	}
	oldTimestamp := tsBeforeUpdate.LastUpdated()
	// update resource tables with new data
	if err := updateResourceTableValues(featureTable, labelTable); err != nil {
		return fmt.Errorf("Could not update resource table values: %v", err)
	}
	// wait for kubernetes run scheduled job at least once
	time.Sleep(70 * time.Second)
	if err := kubernetesRanScheduledJob(tsID); err != nil {
		return fmt.Errorf("kubernetes did not run scheduled job: %v", err)
	}
	// check that training set correctly updated to new values
	if err := checkValuesCorrectlySet(tsID, finalUpdatedTableValues); err != nil {
		return fmt.Errorf("Updated training set values not set properly: %v", err)
	}
	// get metadata timestamp after update completed and check that it is more recent than original
	tsAfterUpdate, err := metadataClient.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set: %v", err)
	}
	newTimestamp := tsAfterUpdate.LastUpdated()
	if !oldTimestamp.Before(newTimestamp) {
		return fmt.Errorf("Resource update not signaled in metadata")
	}
	return nil
}

// training set helper functions

func initializeResourceTablesForTrainingSet(featureName string, labelName string) (provider.OfflineTable, provider.OfflineTable, error) {
	offlineFeature := provider.ResourceID{Name: featureName, Variant: "", Type: provider.Feature}
	schemaInt := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "entity", ValueType: provider.String},
			{Name: "value", ValueType: provider.Int},
			{Name: "ts", ValueType: provider.Timestamp},
		},
	}
	featureTable, err := offlinePostgresStore.CreateResourceTable(offlineFeature, schemaInt)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create feature table: %v", err)
	}
	for _, value := range testOfflineTableValues {
		if err := featureTable.Write(value); err != nil {
			return nil, nil, fmt.Errorf("could not write to offline feature table: %v", err)
		}
	}
	offlineLabel := provider.ResourceID{Name: labelName, Variant: "", Type: provider.Label}
	labelTable, err := offlinePostgresStore.CreateResourceTable(offlineLabel, schemaInt)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create label table: %v", err)
	}
	for _, value := range testOfflineTableValues {
		if err := labelTable.Write(value); err != nil {
			return nil, nil, fmt.Errorf("could not write to offline label table: %v", err)
		}
	}
	return featureTable, labelTable, nil
}

func updateResourceTableValues(featureTable provider.OfflineTable, labelTable provider.OfflineTable) error {
	for _, value := range testOfflineTableUpdateValues {
		if err := featureTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline feature table: %v", err)
		}
	}
	for _, value := range testOfflineTableUpdateValues {
		if err := labelTable.Write(value); err != nil {
			return fmt.Errorf("could not write to offline label table: %v", err)
		}
	}
	return nil
}

func checkValuesCorrectlySet(tsID metadata.ResourceID, correctTable []provider.ResourceRecord) error {
	providerTsID := provider.ResourceID{Name: tsID.Name, Variant: tsID.Variant, Type: provider.TrainingSet}
	tsIterator, err := offlinePostgresStore.GetTrainingSet(providerTsID)
	if err != nil {
		return fmt.Errorf("Coordinator did not create training set: %v", err)
	}
	for i := 0; tsIterator.Next(); i++ {
		retrievedFeatures := tsIterator.Features()
		retrievedLabel := tsIterator.Label()
		if !reflect.DeepEqual(retrievedFeatures[0], correctTable[i].Value) {
			return fmt.Errorf("Features not copied into training set")
		}
		if !reflect.DeepEqual(retrievedLabel, correctTable[i].Value) {
			return fmt.Errorf("Label not copied into training set")
		}
	}
	return nil
}

func kubernetesRanScheduledJob(resID metadata.ResourceID) error {
	jobClient, err := runner.NewKubernetesJobClient(runner.GetCronJobName(resID), runner.Namespace)
	if err != nil {
		return fmt.Errorf("Could not initialize kubernetes job client: %v", err)
	}
	cronJob, err := jobClient.GetCronJob()
	if err != nil {
		return fmt.Errorf("Could not get cron job from kubernetes: %v", err)
	}
	lastExecutionTime := cronJob.Status.LastSuccessfulTime
	if lastExecutionTime.IsZero() {
		return fmt.Errorf("job did not execute in time")
	}
	return nil
}

// feature materialization test

func testScheduleFeatureMaterialization() error {
	// initialize random resource names
	featureName := createSafeUUID()
	sourceName := createSafeUUID()
	originalTableName := createSafeUUID()
	featureID := metadata.ResourceID{Name: featureName, Variant: "", Type: metadata.FEATURE_VARIANT}
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	// create postgres table with original data
	if err := CreateOriginalPostgresTable(originalTableName); err != nil {
		return fmt.Errorf("Could not create table in postgres: %v", err)
	}
	// initialize feature in metadata
	if err := materializeFeatureWithProvider(featureName, sourceName, originalTableName, updateEveryMinuteSchedule); err != nil {
		return fmt.Errorf("could not create online feature in metadata: %v", err)
	}
	// check that feature is set in metdata
	// have coordinator run job on source resource
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return fmt.Errorf("Could not execute coordinator source job: %v", err)
	}
	// have coordinator run job on feature resource
	if err := coord.ExecuteJob(metadata.GetJobKey(featureID)); err != nil {
		return fmt.Errorf("Could not execute coordinator feature job: %v", err)
	}
	// check that original values for online store set correctly
	if err := checkOnlineStoreTables(featureName, testOfflineTableValues); err != nil {
		return fmt.Errorf("tables not set correctly before update: %v", err)
	}
	// get last_updated timestamp of feature before update job runs
	featureBeforeUpdate, err := metadataClient.GetFeatureVariant(ctx, metadata.NameVariant{Name: featureName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set: %v", err)
	}
	oldTimestamp := featureBeforeUpdate.LastUpdated()
	if err := UpdateOriginalPostgresTable(originalTableName); err != nil {
		return fmt.Errorf("Could not update postgres table: %v", err)
	}
	// wait for scheduler to run job at least once (1 minute + 10 second buffer)
	time.Sleep(70 * time.Second)
	// check that kubernetes ran scheduled job
	if err := kubernetesRanScheduledJob(featureID); err != nil {
		return fmt.Errorf("kubernetes did not run scheduled job: %v", err)
	}
	// check that online store tables have freshest data
	if err := checkOnlineStoreTables(featureName, testOfflineTableUpdateValues); err != nil {
		return fmt.Errorf("Online table did not update to new values: %v", err)
	}
	// get timestamp of feature after update and compare to original timestamp
	featureSecondTimestamp, err := metadataClient.GetFeatureVariant(ctx, metadata.NameVariant{Name: featureName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get online data: %v", err)
	}
	newTimestamp := featureSecondTimestamp.LastUpdated()
	if !oldTimestamp.Before(newTimestamp) {
		return fmt.Errorf("Resource update not signaled in metadata")
	}
	return nil
}

// feature materialization test helper functions

func checkOnlineStoreTables(featureName string, correctTable []provider.ResourceRecord) error {
	resourceTable, err := onlineRedisStore.GetTable(featureName, "")
	if err != nil {
		return err
	}
	for _, record := range correctTable {
		value, err := resourceTable.Get(record.Entity)
		if err != nil {
			return fmt.Errorf("Could not get record from online store: %v", err)
		}
		if !reflect.DeepEqual(value, record.Value) {
			return fmt.Errorf("Feature value did not materialize")
		}
	}
	return nil
}

// transformation schedule test
func testScheduleTransformation() error {
	// create random resource names
	tableName := createSafeUUID()
	sourceName := strings.Replace(createSafeUUID(), "-", "", -1)
	transformationName := strings.Replace(createSafeUUID(), "-", "", -1)
	transformationQuery := fmt.Sprintf("SELECT * FROM {{%s.}}", sourceName)
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	sourceNameVariants := []metadata.NameVariant{{Name: sourceName, Variant: ""}}
	transformationID := metadata.ResourceID{Name: transformationName, Variant: "", Type: metadata.SOURCE_VARIANT}
	// initialize source data table
	if err := CreateOriginalPostgresTable(tableName); err != nil {
		return fmt.Errorf("Could not create non-featureform source table: %v", err)
	}
	// create primary table in metadata
	if err := createSourceWithProvider(sourceName, tableName); err != nil {
		return fmt.Errorf("could not register source in metadata: %v", err)
	}
	// have coordinator initialize primary table in offline store
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return fmt.Errorf("Could not execute source job in coordinator: %v", err)
	}
	// create transformation in metadata
	if err := createTransformationWithProvider(transformationName, transformationQuery, sourceNameVariants, updateEveryMinuteSchedule); err != nil {
		return fmt.Errorf("Could not register transformation in metadata: %v", err)
	}
	// have coordinator initialize transformation in offline store
	if err := coord.ExecuteJob(metadata.GetJobKey(transformationID)); err != nil {
		return fmt.Errorf("Could not execute transformation job in coordinator: %v", err)
	}
	// get original timestamp for metadata transformation
	originalTransformationMetadataRecord, err := metadataClient.GetSourceVariant(ctx, metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set")
	}
	oldTimestamp := originalTransformationMetadataRecord.LastUpdated()
	if err := UpdateOriginalPostgresTable(tableName); err != nil {
		return fmt.Errorf("Could not update table in postgres: %v", err)
	}
	// wait for kubernetes to run scheduled job at least once
	time.Sleep(70 * time.Second)
	// check that kubernetes ran scheduled job
	if err := kubernetesRanScheduledJob(transformationID); err != nil {
		return fmt.Errorf("transformation scheduled job did not run: %v", err)
	}
	// check that transformation table is updated with latest values
	if err := checkTransformationTableValues(transformationName, finalUpdatedTableValues); err != nil {
		return fmt.Errorf("transformation table values did not update: %v", err)
	}
	// get timestamp of updated transformation table in metadata and check that timestamp is more recent than original
	updatedTransformationMetadataRecord, err := metadataClient.GetSourceVariant(ctx, metadata.NameVariant{Name: transformationName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set: %v", err)
	}
	newTimestamp := updatedTransformationMetadataRecord.LastUpdated()
	if !oldTimestamp.Before(newTimestamp) {
		return fmt.Errorf("Resource update not signaled in metadata")
	}
	return nil
}

// transformation test helper functions

func checkTransformationTableValues(transformationName string, correctValues []provider.ResourceRecord) error {
	providerJoinTransformationID := provider.ResourceID{Name: transformationName, Variant: "", Type: provider.Transformation}
	joinTransformationTable, err := offlinePostgresStore.GetTransformationTable(providerJoinTransformationID)
	if err != nil {
		return fmt.Errorf("Could not get transformation table from offline store: %v", err)
	}
	transformationJoinName, err := provider.GetTransformationName(providerJoinTransformationID)
	if err != nil {
		return fmt.Errorf("invalid transformation table name: %v", err)
	}
	if joinTransformationTable.GetName() != transformationJoinName {
		return fmt.Errorf("Transformation table did not copy name")
	}
	joinTransformationIterator, err := joinTransformationTable.IterateSegment(int64(len(correctValues)))
	if err != nil {
		return fmt.Errorf("Could not get iterate segment from transformation table: %v", err)
	}
	i := 0
	for ; joinTransformationIterator.Next(); i++ {
		if joinTransformationIterator.Err() != nil {
			return fmt.Errorf("Error iterating over transformation table: %v", err)
		}
		transformationTableRow := joinTransformationIterator.Values()
		values := reflect.ValueOf(correctValues[i])
		for j := 0; j < values.NumField(); j++ {
			if transformationTableRow[j] != values.Field(j).Interface() {
				return fmt.Errorf("Transformation table did not update")
			}
		}
	}
	return nil
}

// test changing schedule of existing resource
// (this could be tested with a training set, transformation or feature)
// (my choice of feature materialization was arbitrary)

func testUpdateExistingSchedule() error {
	// create random resource names
	featureName := createSafeUUID()
	sourceName := createSafeUUID()
	originalTableName := createSafeUUID()
	featureID := metadata.ResourceID{Name: featureName, Variant: "", Type: metadata.FEATURE_VARIANT}
	sourceID := metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}
	// intiialize feature in metadata
	if err := materializeFeatureWithProvider(featureName, sourceName, originalTableName, updateEveryMinuteSchedule); err != nil {
		return fmt.Errorf("could not create online feature in metadata: %v", err)
	}
	// have coordinator initialize feature's source in offline store
	if err := coord.ExecuteJob(metadata.GetJobKey(sourceID)); err != nil {
		return fmt.Errorf("Error executing source job in coordinator: %v", err)
	}
	// have coordinator materialize feature in online store
	if err := coord.ExecuteJob(metadata.GetJobKey(featureID)); err != nil {
		return fmt.Errorf("Error executing feature job in coordinator: %v", err)
	}
	// Check the original set schedule in kubernetes
	jobClient, err := runner.NewKubernetesJobClient(runner.GetCronJobName(featureID), runner.Namespace)
	if err != nil {
		return fmt.Errorf("Could not get kubernetes job client: %v", err)
	}
	oldCronJob, err := jobClient.GetCronJob()
	if err != nil {
		return fmt.Errorf("Could not get kubernetes cron job: %v", err)
	}
	if oldCronJob.Spec.Schedule != updateEveryMinuteSchedule {
		return fmt.Errorf("Did not set original schedule")
	}
	// run request schedule change client method, which signals the metadata to set update schedule job in etcd
	// The coordinator will then see this job and open a client to kubernetes to change the schedule
	if err := metadataClient.RequestScheduleChange(ctx, featureID, updateEveryTwoMinutesSchedule); err != nil {
		return fmt.Errorf("Could not request schedule change from metadata server: %v", err)
	}
	// wait a bit
	time.Sleep(5 * time.Second)
	// check kubernetes to see if new schedule is set
	newCronJob, err := jobClient.GetCronJob()
	if err != nil {
		return fmt.Errorf("Could not get new cron job: %v", err)
	}
	if newCronJob.Spec.Schedule != updateEveryTwoMinutesSchedule {
		return fmt.Errorf("Did not set new schedule: %v", err)
	}
	return nil
}

// helper function for setting original source data

func CreateOriginalPostgresTable(tableName string) error {
	url := fmt.Sprintf("postgres:// %s:%s@%s:%s/%s", postgresConfig.Username, postgresConfig.Password, postgresConfig.Host, postgresConfig.Port, postgresConfig.Database)
	conn, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("Error connecting to postgres deployment: %v", err)
	}
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value INT, ts TIMESTAMPTZ)", sanitize(tableName))
	if _, err := conn.Exec(ctx, createTableQuery); err != nil {
		return fmt.Errorf("Could not execute create table query in postgres: %v", err)
	}
	for _, record := range testOfflineTableValues {
		upsertQuery := fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES ($1, $2, $3)", sanitize(tableName))
		if _, err := conn.Exec(ctx, upsertQuery, record.Entity, record.Value, record.TS); err != nil {
			return fmt.Errorf("Could not insert values into postgres table: %v", err)
		}
	}
	return nil
}

func UpdateOriginalPostgresTable(tableName string) error {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresConfig.Username, postgresConfig.Password, postgresConfig.Host, postgresConfig.Port, postgresConfig.Database)
	conn, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("Error connecting to postgres deployment: %v", err)
	}
	for _, record := range testOfflineTableUpdateValues {
		upsertQuery := fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES ($1, $2, $3)", sanitize(tableName))
		if _, err := conn.Exec(ctx, upsertQuery, record.Entity, record.Value, record.TS); err != nil {
			return fmt.Errorf("Could not insert values into postgres table: %v", err)
		}
	}
	return nil
}

//   helper functions for creating resources in metadata

func createTrainingSetWithProvider(sourceName string, featureName string, labelName string, tsName string, originalTableName string, schedule string) error {
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
			SerializedConfig: serialPGConfig,
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
	if err := metadataClient.CreateAll(ctx, defs); err != nil {
		return fmt.Errorf("Could not create resources for training set in metadata: %v", err)
	}
	return nil
}

func materializeFeatureWithProvider(featureName string, sourceName string, originalTableName string, schedule string) error {
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
			SerializedConfig: serialPGConfig,
		},
		metadata.ProviderDef{
			Name:             onlineProviderName,
			Description:      "",
			Type:             "REDIS_ONLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: serialRedisConfig,
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
	if err := metadataClient.CreateAll(ctx, defs); err != nil {
		return fmt.Errorf("Could not create resources for feature in metadata: %v", err)
	}
	return nil
}

func createSourceWithProvider(sourceName string, tableName string) error {
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
			SerializedConfig: serialPGConfig,
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
	if err := metadataClient.CreateAll(ctx, defs); err != nil {
		return fmt.Errorf("Could not create resources for source in metadata: %v", err)
	}
	return nil
}

func createTransformationWithProvider(sourceName string, transformationQuery string, sources []metadata.NameVariant, schedule string) error {
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
			SerializedConfig: serialPGConfig,
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
	if err := metadataClient.CreateAll(ctx, defs); err != nil {
		return fmt.Errorf("Could not create resources for transformation in metadata: %v", err)
	}
	return nil
}
