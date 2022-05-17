package coordinator

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"

	"github.com/featureform/serving/metadata"
	pb "github.com/featureform/serving/metadata/proto"
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

// func setupMetadataServer() error {
// 	logger := zap.NewExample().Sugar()
// 	addr := ":8080"
// 	storageProvider := metadata.EtcdStorageProvider{
// 		metadata.EtcdConfig{
// 			Nodes: []metadata.EtcdNode{
// 				{"localhost", "2379"},
// 			},
// 		},
// 	}
// 	config := &metadata.Config{
// 		Logger:          logger,
// 		Address:         addr,
// 		StorageProvider: storageProvider,
// 	}
// 	server, err := metadata.NewMetadataServer(config)
// 	if err != nil {
// 		return err
// 	}
// 	if err := server.Serve(); err != nil {
// 		return err
// 	}
// 	return nil
// }

func startServ(t *testing.T) (*metadata.MetadataServer, string) {
	logger := zap.NewExample().Sugar()
	storageProvider := metadata.EtcdStorageProvider{
		metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{"localhost", "2379"},
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

// func (ctx *testContext) Create(t *testing.T) (*Client, error) {
// 	var addr string
// 	ctx.serv, addr = startServ(t)
// 	ctx.client = client(t, addr)
// 	if err := ctx.client.CreateAll(context.Background(), ctx.Defs); err != nil {
// 		return nil, err
// 	}
// 	return ctx.client, nil
// }

// func (ctx *testContext) Destroy() {
// 	ctx.serv.Stop()
// 	ctx.client.Close()
// }

func createNewCoordinator(addr string) (*Coordinator, error) {
	logger := zap.NewExample().Sugar()
	fmt.Println(addr)
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		return nil, err
	}
	memJobSpawner := MemoryJobSpawner{}
	return NewCoordinator(client, logger, cli, &memJobSpawner)
}

func TestKubernetesJobRunnerError(t *testing.T) {
	kubeJobSpawner := KubernetesJobSpawner{}
	if _, err := kubeJobSpawner.GetJobRunner("ghost_job", []byte{}); err == nil {
		t.Fatalf("did not trigger error getting nonexistent runner")
	}
}

func TestMemoryJobRunnerError(t *testing.T) {
	memJobSpawner := MemoryJobSpawner{}
	if _, err := memJobSpawner.GetJobRunner("ghost_job", []byte{}); err == nil {
		t.Fatalf("did not trigger error getting nonexistent runner")
	}
}

func TestRunSQLJobError(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	coord, err := createNewCoordinator(addr)
	if err != nil {
		t.Fatalf("could not create new basic coordinator")
	}
	defer coord.Metadata.Close()
	sourceGhostDependency := uuid.New().String()
	providerName := uuid.New().String()
	userName := uuid.New().String()
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
			SerializedConfig: postgresConfig.Serialize(),
		},
		metadata.SourceDef{
			Name:        sourceGhostDependency,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    providerName,
			Definition: metadata.TransformationSource{
				TransformationType: metadata.SQLTransformationType{
					Query:   "{{ghost_source.}}",
					Sources: []metadata.NameVariant{{"ghost_source", ""}},
				},
			},
		},
	}
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create test metadata entries: %v", err)
	}
	transformSource, err := coord.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{sourceGhostDependency, ""})
	if err != nil {
		t.Fatalf("could not fetch created source variant: %v", err)
	}
	provider, err := provider.Get(provider.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not get provider: %v", err)
	}
	offlineProvider, err := provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not get provider as offline store: %v", err)
	}
	sourceResourceID := metadata.ResourceID{sourceGhostDependency, "", metadata.SOURCE_VARIANT}
	if err := coord.runSQLTransformationJob(transformSource, sourceResourceID, offlineProvider); err == nil {
		t.Fatalf("did not catch error trying to run primary table job with no source table set")
	}
}

func TestFeatureMaterializeJobError(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	coord, err := createNewCoordinator(addr)
	if err != nil {
		t.Fatalf("could not create new basic coordinator")
	}
	defer coord.Metadata.Close()
	if err := coord.runFeatureMaterializeJob(metadata.ResourceID{"ghost_resource", "", metadata.FEATURE_VARIANT}); err == nil {
		t.Fatalf("did not catch error when trying to materialize nonexistent feature")
	}
	redisPort := os.Getenv("REDIS_PORT")
	redisHost := "localhost"
	liveAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	redisConfig := &provider.RedisConfig{
		Addr: liveAddr,
	}
	featureName := uuid.New().String()
	sourceName := uuid.New().String()
	originalTableName := uuid.New().String()
	if err := materializeFeatureWithProvider(coord.Metadata, postgresConfig.Serialize(), redisConfig.Serialized(), featureName, sourceName, originalTableName); err != nil {
		t.Fatalf("could not create example feature, %v", err)
	}
	if err := coord.Metadata.SetStatus(context.Background(), metadata.ResourceID{featureName, "", metadata.FEATURE_VARIANT}, pb.ResourceStatus{Status: pb.ResourceStatus_READY}); err != nil {
		t.Fatalf("could not set feature to ready")
	}
	if err := coord.runFeatureMaterializeJob(metadata.ResourceID{featureName, "", metadata.FEATURE_VARIANT}); err == nil {
		t.Fatalf("did not catch error when trying to materialize feature already set to ready")
	}
	providerName := uuid.New().String()
	userName := uuid.New().String()
	sourceName = uuid.New().String()
	entityName := uuid.New().String()
	originalTableName = uuid.New().String()
	featureName = uuid.New().String()
	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             providerName,
			Description:      "",
			Type:             "INVALID_PROVIDER",
			Software:         "",
			Team:             "",
			SerializedConfig: []byte{},
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
	}
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create metadata entries: %v", err)
	}
	if err := coord.Metadata.SetStatus(context.Background(), metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}, pb.ResourceStatus{Status: pb.ResourceStatus_READY}); err != nil {
		t.Fatalf("could not set source variant to ready")
	}
	if err := coord.runFeatureMaterializeJob(metadata.ResourceID{featureName, "", metadata.FEATURE_VARIANT}); err == nil {
		t.Fatalf("did not trigger error trying to run job with nonexistent provider")
	}
	providerName = uuid.New().String()
	userName = uuid.New().String()
	sourceName = uuid.New().String()
	entityName = uuid.New().String()
	originalTableName = uuid.New().String()
	featureName = uuid.New().String()
	defs = []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             providerName,
			Description:      "",
			Type:             "REDIS_ONLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: redisConfig.Serialized(),
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
	}
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create metadata entries: %v", err)
	}
	if err := coord.Metadata.SetStatus(context.Background(), metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}, pb.ResourceStatus{Status: pb.ResourceStatus_READY}); err != nil {
		t.Fatalf("could not set source variant to ready")
	}
	if err := coord.runFeatureMaterializeJob(metadata.ResourceID{featureName, "", metadata.FEATURE_VARIANT}); err == nil {
		t.Fatalf("did not trigger error trying to use online store as offline store")
	}
	providerName = uuid.New().String()
	offlineProviderName := uuid.New().String()
	userName = uuid.New().String()
	sourceName = uuid.New().String()
	entityName = uuid.New().String()
	originalTableName = uuid.New().String()
	featureName = uuid.New().String()
	defs = []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             offlineProviderName,
			Description:      "",
			Type:             "POSTGRES_OFFLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: postgresConfig.Serialize(),
		},
		metadata.ProviderDef{
			Name:             providerName,
			Description:      "",
			Type:             "INVALID_PROVIDER",
			Software:         "",
			Team:             "",
			SerializedConfig: []byte{},
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
			Provider:    providerName,
			Location: metadata.ResourceVariantColumns{
				Entity: "entity",
				Value:  "value",
				TS:     "ts",
			},
		},
	}
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create metadata entries: %v", err)
	}
	if err := coord.Metadata.SetStatus(context.Background(), metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}, pb.ResourceStatus{Status: pb.ResourceStatus_READY}); err != nil {
		t.Fatalf("could not set source variant to ready")
	}
	if err := coord.runFeatureMaterializeJob(metadata.ResourceID{featureName, "", metadata.FEATURE_VARIANT}); err == nil {
		t.Fatalf("did not trigger error trying to get invalid feature provider")
	}
}

func TestTrainingSetJobError(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	coord, err := createNewCoordinator(addr)
	if err != nil {
		t.Fatalf("could not create new basic coordinator")
	}
	defer coord.Metadata.Close()
	if err := coord.runTrainingSetJob(metadata.ResourceID{"ghost_training_set", "", metadata.TRAINING_SET_VARIANT}); err == nil {
		t.Fatalf("did not trigger error trying to run job for nonexistent training set")
	}
	providerName := uuid.New().String()
	userName := uuid.New().String()
	sourceName := uuid.New().String()
	entityName := uuid.New().String()
	labelName := uuid.New().String()
	originalTableName := uuid.New().String()
	featureName := uuid.New().String()
	tsName := uuid.New().String()
	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             providerName,
			Description:      "",
			Type:             "INVALID_PROVIDER",
			Software:         "",
			Team:             "",
			SerializedConfig: []byte{},
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
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create metadata entries: %v", err)
	}
	if err := coord.runTrainingSetJob(metadata.ResourceID{tsName, "", metadata.TRAINING_SET_VARIANT}); err == nil {
		t.Fatalf("did not trigger error trying to run job with nonexistent provider")
	}
	providerName = uuid.New().String()
	userName = uuid.New().String()
	sourceName = uuid.New().String()
	entityName = uuid.New().String()
	labelName = uuid.New().String()
	originalTableName = uuid.New().String()
	featureName = uuid.New().String()
	tsName = uuid.New().String()
	redisPort := os.Getenv("REDIS_PORT")
	redisHost := "localhost"
	liveAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	redisConfig := &provider.RedisConfig{
		Addr: liveAddr,
	}
	defs = []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             providerName,
			Description:      "",
			Type:             "REDIS_ONLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: redisConfig.Serialized(),
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
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create metadata entries: %v", err)
	}
	if err := coord.runTrainingSetJob(metadata.ResourceID{tsName, "", metadata.TRAINING_SET_VARIANT}); err == nil {
		t.Fatalf("did not trigger error trying to convert online provider to offline")
	}
}

func TestRunPrimaryTableJobError(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	coord, err := createNewCoordinator(addr)
	if err != nil {
		t.Fatalf("could not create new basic coordinator")
	}
	defer coord.Metadata.Close()
	sourceNoPrimaryNameSet := uuid.New().String()
	providerName := uuid.New().String()
	userName := uuid.New().String()
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
			SerializedConfig: postgresConfig.Serialize(),
		},
		metadata.SourceDef{
			Name:        sourceNoPrimaryNameSet,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    providerName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "",
				},
			},
		},
	}
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create test metadata entries")
	}
	transformSource, err := coord.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{sourceNoPrimaryNameSet, ""})
	if err != nil {
		t.Fatalf("could not fetch created source variant: %v", err)
	}
	provider, err := provider.Get(provider.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not get provider: %v", err)
	}
	offlineProvider, err := provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not get provider as offline store: %v", err)
	}
	sourceResourceID := metadata.ResourceID{sourceNoPrimaryNameSet, "", metadata.SOURCE_VARIANT}
	if err := coord.runPrimaryTableJob(transformSource, sourceResourceID, offlineProvider); err == nil {
		t.Fatalf("did not catch error trying to run primary table job with no source table set")
	}
	sourceNoActualPrimaryTable := uuid.New().String()
	newProviderName := uuid.New().String()
	newUserName := uuid.New().String()
	newDefs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: newUserName,
		},
		metadata.ProviderDef{
			Name:             newProviderName,
			Description:      "",
			Type:             "POSTGRES_OFFLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: postgresConfig.Serialize(),
		},
		metadata.SourceDef{
			Name:        sourceNoActualPrimaryTable,
			Variant:     "",
			Description: "",
			Owner:       newUserName,
			Provider:    newProviderName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "ghost_primary_table",
				},
			},
		},
	}
	if err := coord.Metadata.CreateAll(context.Background(), newDefs); err != nil {
		t.Fatalf("could not create test metadata entries: %v", err)
	}
	newTransformSource, err := coord.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{sourceNoActualPrimaryTable, ""})
	if err != nil {
		t.Fatalf("could not fetch created source variant: %v", err)
	}
	newSourceResourceID := metadata.ResourceID{sourceNoActualPrimaryTable, "", metadata.SOURCE_VARIANT}
	if err := coord.runPrimaryTableJob(newTransformSource, newSourceResourceID, offlineProvider); err == nil {
		t.Fatalf("did not catch error trying to create primary table when no source table exists in database")
	}
}

func TestMapNameVariantsToTablesError(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	coord, err := createNewCoordinator(addr)
	if err != nil {
		t.Fatalf("could not create new basic coordinator")
	}
	defer coord.Metadata.Close()
	ghostResourceName := uuid.New().String()
	ghostNameVariants := []metadata.NameVariant{{ghostResourceName, ""}}
	if _, err := coord.mapNameVariantsToTables(ghostNameVariants); err == nil {
		t.Fatalf("did not catch error creating map from nonexistent resource")
	}
	sourceNotReady := uuid.New().String()
	providerName := uuid.New().String()
	tableName := uuid.New().String()
	userName := uuid.New().String()
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
			SerializedConfig: []byte{},
		},
		metadata.SourceDef{
			Name:        sourceNotReady,
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
	if err := coord.Metadata.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("could not create test metadata entries")
	}
	notReadyNameVariants := []metadata.NameVariant{{sourceNotReady, ""}}
	if _, err := coord.mapNameVariantsToTables(notReadyNameVariants); err == nil {
		t.Fatalf("did not catch error creating map from not ready resource")
	}
}

func TestRegisterSourceJobErrors(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	coord, err := createNewCoordinator(addr)
	if err != nil {
		t.Fatalf("could not create new basic coordinator")
	}
	defer coord.Metadata.Close()
	ghostResourceName := uuid.New().String()
	ghostResourceID := metadata.ResourceID{ghostResourceName, "", metadata.SOURCE_VARIANT}
	if err := coord.runRegisterSourceJob(ghostResourceID); err == nil {
		t.Fatalf("did not catch error registering nonexistent resource")
	}
	sourceWithoutProvider := uuid.New().String()
	ghostProviderName := uuid.New().String()
	ghostTableName := uuid.New().String()
	userName := uuid.New().String()
	providerErrorDefs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: userName,
		},
		metadata.ProviderDef{
			Name:             ghostProviderName,
			Description:      "",
			Type:             "GHOST_PROVIDER",
			Software:         "",
			Team:             "",
			SerializedConfig: []byte{},
		},
		metadata.SourceDef{
			Name:        sourceWithoutProvider,
			Variant:     "",
			Description: "",
			Owner:       userName,
			Provider:    ghostProviderName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: ghostTableName,
				},
			},
		},
	}
	if err := coord.Metadata.CreateAll(context.Background(), providerErrorDefs); err != nil {
		t.Fatalf("could not create test metadata entries")
	}
	sourceWithoutProviderResourceID := metadata.ResourceID{sourceWithoutProvider, "", metadata.SOURCE_VARIANT}
	if err := coord.runRegisterSourceJob(sourceWithoutProviderResourceID); err == nil {
		t.Fatalf("did not catch error registering registering resource without provider in offline store")
	}
	sourceWithoutOfflineProvider := uuid.New().String()
	onlineProviderName := uuid.New().String()
	newTableName := uuid.New().String()
	newUserName := uuid.New().String()
	redisPort := os.Getenv("REDIS_PORT")
	redisHost := "localhost"
	liveAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	redisConfig := &provider.RedisConfig{
		Addr: liveAddr,
	}
	serialRedisConfig := redisConfig.Serialized()
	onlineErrorDefs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: newUserName,
		},
		metadata.ProviderDef{
			Name:             onlineProviderName,
			Description:      "",
			Type:             "REDIS_ONLINE",
			Software:         "",
			Team:             "",
			SerializedConfig: serialRedisConfig,
		},
		metadata.SourceDef{
			Name:        sourceWithoutOfflineProvider,
			Variant:     "",
			Description: "",
			Owner:       newUserName,
			Provider:    onlineProviderName,
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: newTableName,
				},
			},
		},
	}
	if err := coord.Metadata.CreateAll(context.Background(), onlineErrorDefs); err != nil {
		t.Fatalf("could not create test metadata entries")
	}
	sourceWithOnlineProvider := metadata.ResourceID{sourceWithoutOfflineProvider, "", metadata.SOURCE_VARIANT}
	if err := coord.runRegisterSourceJob(sourceWithOnlineProvider); err == nil {
		t.Fatalf("did not catch error registering registering resource with online provider")
	}
}

func TestTemplateReplace(t *testing.T) {
	templateString := "Some example text {{name1.variant1}} and more {{name2.variant2}}"
	replacements := map[string]string{"name1.variant1": "replacement1", "name2.variant2": "replacement2"}
	correctString := "Some example text replacement1 and more replacement2"
	result, err := templateReplace(templateString, replacements)
	if err != nil {
		t.Fatalf("template replace did not run correctly: %v", err)
	}
	if result != correctString {
		t.Fatalf("template replace did not replace values correctly")
	}

}

func TestTemplateReplaceError(t *testing.T) {
	templateString := "Some example text {{name1.variant1}} and more {{name2.variant2}}"
	wrongReplacements := map[string]string{"name1.variant1": "replacement1", "name3.variant3": "replacement2"}
	_, err := templateReplace(templateString, wrongReplacements)
	if err == nil {
		t.Fatalf("template replace did not catch error: %v", err)
	}

}

func TestCoordinatorCalls(t *testing.T) {
	if testing.Short() {
		return
	}
	serv, addr := startServ(t)
	defer serv.Stop()
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf("could not set up metadata client: %v", err)
	}
	defer client.Close()
	if err := testCoordinatorMaterializeFeature(addr); err != nil {
		t.Fatalf("coordinator could not materialize feature: %v", err)
	}
	if err := testCoordinatorTrainingSet(addr); err != nil {
		t.Fatalf("coordinator could not create training set: %v", err)
	}
	if err := testRegisterPrimaryTableFromSource(addr); err != nil {
		t.Fatalf("coordinator could not register primary table from source: %v", err)
	}
	if err := testRegisterTransformationFromSource(addr); err != nil {
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

func testCoordinatorTrainingSet(addr string) error {
	if err := runner.RegisterFactory(string(runner.CREATE_TRAINING_SET), runner.TrainingSetRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
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
		fmt.Println("waiting for job to be deleted")
	}
	ts_complete, err := client.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: tsName, Variant: ""})
	if err != nil {
		return fmt.Errorf("could not get training set variant")
	}
	if metadata.READY != ts_complete.Status() {
		return fmt.Errorf("Training set not set to ready once job completes")
	}
	if err := coord.runTrainingSetJob(tsID); err == nil {
		return fmt.Errorf("run training set job did not trigger error when tried to create training set that already exists")
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

func testCoordinatorMaterializeFeature(addr string) error {
	if err := runner.RegisterFactory(string(runner.COPY_TO_ONLINE), runner.MaterializedChunkRunnerFactory); err != nil {
		return fmt.Errorf("Failed to register training set runner factory: %v", err)
	}
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		return err
	}
	defer cli.Close()
	serialPGConfig := postgresConfig.Serialize()
	fmt.Println(serialPGConfig)
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
	if err := client.SetStatus(context.Background(), metadata.ResourceID{Name: sourceName, Variant: "", Type: metadata.SOURCE_VARIANT}, pb.ResourceStatus{Status: pb.ResourceStatus_READY}); err != nil {
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
		fmt.Println("waiting for job to be deleted")

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

func testRegisterPrimaryTableFromSource(addr string) error {
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
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
		fmt.Println("waiting for job to be deleted")

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

func testRegisterTransformationFromSource(addr string) error {
	/////////// this code is copied from the above, make into its own function
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer client.Close()
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
