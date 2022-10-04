package provider

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/featureform/kubernetes"

	"go.uber.org/zap"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

//parquet or csv
//register file as source
//register file as feature/label

type K8sOfflineStore struct {
	executor Executor
	store    BlobStore
	logger   *zap.SugaredLogger
	query    *defaultOfflineSQLQueries
	BaseProvider
}

func (store *K8sOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (k8s *K8sOfflineStore) Close() error {
	k8s.store.Close()
	return nil
}

type Config []byte

type ExecutorFactory func(config Config) (Executor, error)

var executorFactoryMap = make(map[string]ExecutorFactory)

func RegisterExecutorFactory(name string, executorFactory ExecutorFactory) error {
	if _, exists := executorFactoryMap[name]; exists {
		return fmt.Errorf("factory already registered: %s", name)
	}
	executorFactoryMap[name] = executorFactory
	return nil
}

func UnregisterExecutorFactory(name string) error {
	if _, exists := executorFactoryMap[name]; !exists {
		return fmt.Errorf("factory %s not registered", name)
	}
	delete(executorFactoryMap, name)
	return nil
}

func CreateExecutor(name string, config Config) (Executor, error) {
	factory, exists := executorFactoryMap[name]
	if !exists {
		return nil, fmt.Errorf("factory does not exist: %s", name)
	}
	executor, err := factory(config)
	if err != nil {
		return nil, err
	}
	return executor, nil
}

type BlobStoreFactory func(config Config) (BlobStore, error)

var blobStoreFactoryMap = make(map[string]BlobStoreFactory)

func RegisterBlobStoreFactory(name string, blobStoreFactory BlobStoreFactory) error {
	if _, exists := blobStoreFactoryMap[name]; exists {
		return fmt.Errorf("factory already registered: %s", name)
	}
	blobStoreFactoryMap[name] = blobStoreFactory
	return nil
}

func UnregisterBlobStoreFactory(name string) error {
	if _, exists := blobStoreFactoryMap[name]; !exists {
		return fmt.Errorf("factory %s not registered", name)
	}
	delete(blobStoreFactoryMap, name)
	return nil
}

func CreateBlobStore(name string, config Config) (BlobStore, error) {
	factory, exists := blobStoreFactoryMap[name]
	if !exists {
		return nil, fmt.Errorf("factory does not exist: %s", name)
	}
	blobStore, err := factory(config)
	if err != nil {
		return nil, err
	}
	return blobStore, nil
}

func init() {
	blobStoreFactoryMap := map[BlobStoreType]BlobStoreFactory{
		Memory: NewMemoryBlobStore,
	}
	executorFactoryMap := map[ExecutorType]ExecutorFactory{
		GoProc: NewLocalExecutor,
	}
	for storeType, factory := range blobStoreFactoryMap {
		RegisterBlobStoreFactory(string(storeType), factory)
	}
	for executorType, factory := range executorFactoryMap {
		RegisterExecutorFactory(string(executorType), factory)
	}
}

func k8sOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	k8 := K8sConfig{}
	logger := zap.NewExample().Sugar()
	if err := k8.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize k8s offline store", err)
		return nil, fmt.Errorf("invalid k8s config: %v", config)
	}
	logger.Info("Creating executor with type:", k8.ExecutorType)
	exec, err := CreateExecutor(string(k8.ExecutorType), Config(k8.ExecutorConfig))
	if err != nil {
		logger.Errorw("Failure initializing executor with type", k8.ExecutorType, err)
		return nil, err
	}
	logger.Info("Creating blob store with type:", k8.StoreType)
	store, err := CreateBlobStore(string(k8.StoreType), Config(k8.StoreConfig))
	if err != nil {
		logger.Errorw("Failure initializing blob store with type", k8.StoreType, err)
		return nil, err
	}
	logger.Debugf("Store type: %s, Store config: %v", k8.StoreType, k8.StoreConfig)
	queries := defaultOfflineSQLQueries{}
	k8sOfflineStore := K8sOfflineStore{
		executor: exec,
		store:    store,
		logger:   logger,
		query:    &queries,
		BaseProvider: BaseProvider{
			ProviderType:   "K8S_OFFLINE",
			ProviderConfig: config,
		},
	}
	return &k8sOfflineStore, nil
}

type ExecutorConfig []byte

type BlobStoreConfig []byte

type ExecutorType string

type BlobStoreType string

const (
	GoProc ExecutorType = "GO_PROCESS"
)

const (
	Memory BlobStoreType = "MEMORY"
)

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig ExecutorConfig
	StoreType      BlobStoreType
	StoreConfig    BlobStoreConfig
}

func (config *K8sConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *K8sConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize k8s config: %w", err)
	}
	return nil
}

type Executor interface {
	ExecuteScript(envVars map[string]string) error
}

type LocalExecutor struct {
	scriptPath string
}

type KubernetesExecutor struct {
	image string
}

func (local LocalExecutor) ExecuteScript(envVars map[string]string) error {
	for key, value := range envVars {
		os.Setenv(key, value)
	}
	cmd := exec.Command("python3", local.scriptPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not execute python function: %v", err)
	}
	return nil
}

type LocalExecutorConfig struct {
	ScriptPath string
}

func (config *LocalExecutorConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *LocalExecutorConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize executor config: %w", err)
	}
	return nil
}

func NewLocalExecutor(config Config) (Executor, error) {
	localConfig := LocalExecutorConfig{}
	if err := localConfig.Deserialize([]byte(config)); err != nil {
		return nil, fmt.Errorf("failed to deserialize config")
	}
	_, err := os.Open(localConfig.ScriptPath)
	if err != nil {
		return nil, fmt.Errorf("could not find script path: %v", err)
	}
	return LocalExecutor{
		scriptPath: localConfig.ScriptPath,
	}, nil
}

func (kube KubernetesExecutor) ExecuteScript(envVars map[string]string) error {
	config := kubernetes.KubernetesRunnerConfig{
		EnvVars:  envVars,
		Image:    kube.image,
		NumTasks: 1,
	}
	jobRunner, err := kubernetes.NewKubernetesRunner(config)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	return nil
}

func NewKubernetesExecutor(image string) (Executor, error) {
	return KubernetesExecutor{
		image: image,
	}, nil
}

type BlobStore interface {
	Write(key string, data []byte) error
	Read(key string) ([]byte, error)
	Serve(key string) (Iterator, error)
	Exists(key string) (bool, error)
	Close() error
}

type Iterator interface {
	Next() ([]string, error)
}

type MemoryBlobStore struct {
	genericBlobStore
}

type AzureBlobStore struct {
	genericBlobStore
}

type genericBlobStore struct {
	bucket *blob.Bucket
}

func (store genericBlobStore) Write(key string, data []byte) error {
	err := store.bucket.WriteAll(ctx, key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (store genericBlobStore) Read(key string) ([]byte, error) {
	data, err := store.bucket.ReadAll(ctx, key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (store genericBlobStore) Serve(key string) (Iterator, error) {
	r, err := store.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	keyParts := strings.Split(key, ".")
	switch fileType := keyParts[len(keyParts)-1]; fileType {
	case "csv":
		return csvIteratorFromReader(r)
	default:
		return nil, fmt.Errorf("unsupported file type")
	}

}

func (store genericBlobStore) Exists(key string) (bool, error) {
	return store.bucket.Exists(ctx, key)
}

func (store genericBlobStore) Close() error {
	return store.bucket.Close()
}

type csvIterator struct {
	reader *csv.Reader
}

func (iter csvIterator) Next() ([]string, error) {
	return iter.reader.Read()
}

func csvIteratorFromReader(r io.Reader) (Iterator, error) {
	csvReader := csv.NewReader(r)
	return csvIterator{
		reader: csvReader,
	}, nil
}

func NewMemoryBlobStore(config Config) (BlobStore, error) {
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		return MemoryBlobStore{}, err
	}
	return MemoryBlobStore{
		genericBlobStore{
			bucket: bucket,
		},
	}, nil
}

func NewAzureBlobStore(bucketPath string) (AzureBlobStore, error) {
	bucket, err := blob.OpenBucket(ctx, bucketPath)
	if err != nil {
		return AzureBlobStore{}, err
	}
	return AzureBlobStore{
		genericBlobStore{
			bucket: bucket,
		},
	}, nil
}

func blobResourcePath(id ResourceID) string {
	return ResourcePrefix(id)
}

type BlobOfflineTable struct {
	schema ResourceSchema
}

func (tbl *BlobOfflineTable) Write(ResourceRecord) error {
	return fmt.Errorf("not yet implemented")
}

func (k8s *K8sOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		k8s.logger.Errorw("Failure checking ID", "error", err)
		return nil, fmt.Errorf("ID check failed: %v", err)
	}
	resourceKey := blobResourcePath(id)
	resourceExists, err := k8s.store.Exists(resourceKey)
	if err != nil {
		k8s.logger.Errorw("Error checking if resource exists", "error", err)
		return nil, fmt.Errorf("error checking if resource registry exists: %v", err)
	}
	if resourceExists {
		k8s.logger.Errorw("Resource already exists in blob store", "id", id)
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	serializedSchema, err := schema.Serialize()
	if err != nil {
		return nil, fmt.Errorf("error serializing resource schema: %s: %s", schema, err)
	}
	if err := k8s.store.Write(resourceKey, serializedSchema); err != nil {
		return nil, fmt.Errorf("error writing resource schema: %s: %s", schema, err)
	}
	k8s.logger.Debugw("Registered resource table", "resourceID", id, "for source", schema.SourceTable)
	return &BlobOfflineTable{schema}, nil
}

type BlobPrimaryTable struct {
	store            BlobStore
	sourcePath       string
	isTransformation bool
	id               ResourceID
}

func (tbl *BlobPrimaryTable) Write(GenericRecord) error {
	return fmt.Errorf("not implemented")
}

func (tbl *BlobPrimaryTable) GetName() string {
	return tbl.sourcePath
}

func (tbl *BlobPrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func (tbl *BlobPrimaryTable) NumRows() (int64, error) {
	return 0, fmt.Errorf("not yet implemented")
}

func (k8s *K8sOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	resourceKey := blobResourcePath(id)
	primaryExists, err := k8s.store.Exists(resourceKey)
	if err != nil {
		k8s.logger.Errorw("Error checking if primary exists", err)
		return nil, fmt.Errorf("error checking if primary exists: %v", err)
	}
	if primaryExists {
		k8s.logger.Errorw("Error checking if primary exists")
		return nil, fmt.Errorf("primary already exists")
	}

	k8s.logger.Debugw("Registering primary table", id, "for source", sourceName)
	if err := k8s.store.Write(resourceKey, []byte(sourceName)); err != nil {
		k8s.logger.Errorw("Could not write primary table", err)
		return nil, err
	}
	k8s.logger.Debugw("Succesfully registered primary table", id, "for source", sourceName)
	return &BlobPrimaryTable{k8s.store, sourceName, false, id}, nil
}

func (k8s *K8sOfflineStore) CreateTransformation(config TransformationConfig) error {
	return nil
}

func (k8s *K8sOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return nil
}
func (k8s *K8sOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	resourceKey := blobResourcePath(id)
	k8s.logger.Debugw("Getting primary table", id)
	table, err := k8s.store.Read(resourceKey)
	if err != nil {
		return nil, fmt.Errorf("error fetching primary table: %v", err)
	}
	k8s.logger.Debugw("Succesfully retrieved primary table", id)
	return &BlobPrimaryTable{k8s.store, string(table), false, id}, nil
}

func (k8s *K8sOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	resourcekey := blobResourcePath(id)
	k8s.logger.Debugw("Getting resource table", id)
	serializedSchema, err := k8s.store.Read(resourcekey)
	if err != nil {
		return nil, fmt.Errorf("Error reading schema bytes from blob storage: %v", err)
	}
	resourceSchema := ResourceSchema{}
	if err := resourceSchema.Deserialize(serializedSchema); err != nil {
		return nil, fmt.Errorf("Error deserializing resource table: %v", err)
	}
	k8s.logger.Debugw("Succesfully fetched resource table", "id", id)
	return &S3OfflineTable{resourceSchema}, nil
}

func (k8s *K8sOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return nil
}

func (k8s *K8sOfflineStore) CreateTrainingSet(TrainingSetDef) error {
	return nil
}

func (k8s *K8sOfflineStore) UpdateTrainingSet(TrainingSetDef) error {
	return nil
}

func (k8s *K8sOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return nil, nil
}
