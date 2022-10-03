package offline

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/featureform/runner"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

//parquet or csv
//register file as source
//register file as feature/label



var ctx = context.Background()

type K8sOfflineStore struct {
	executor Executor
	store    BlobStore
	logger   *zap.SugaredLogger
	query    *defaultSparkOfflineQueries
	BaseProvider
}

func (store *K8sOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (k8s *K8sOfflineStore) Close() error {
	k8s.store.bucket.Close()
	return nil
}

func k8sOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SparkConfig{}
	logger := zap.NewExample().Sugar()
	if err := sc.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize k8s offline store", err)
		return nil, fmt.Errorf("invalid k8s config: %v", config)
	}
	logger.Info("Creating K8s executor with type:", sc.ExecutorType)
	exec, err := NewSparkExecutor(sc.ExecutorType, sc.ExecutorConfig, logger)
	if err != nil {
		logger.Errorw("Failure initializing Spark executor with type", sc.ExecutorType, err)
		return nil, err
	}

	fmt.Sprintf("Executor type: %s, Executor config: %v", sc.ExecutorType, sc.ExecutorConfig)
	logger.Info("Creating Spark store with type:", sc.StoreType)
	store, err := NewSparkStore(sc.StoreType, sc.StoreConfig, logger)
	if err != nil {
		logger.Errorw("Failure initializing Spark store with type", sc.StoreType, err)
		return nil, err
	}

	fmt.Sprintf("Store type: %s, Store config: %v", sc.StoreType, sc.StoreConfig)
	logger.Info("Uploading Spark script to store")

	logger.Debugf("Store type: %s, Store config: %v", sc.StoreType, sc.StoreConfig)
	logger.Info("Created Spark Offline Store")
	queries := defaultSparkOfflineQueries{}
	sparkOfflineStore := SparkOfflineStore{
		Executor: exec,
		Store:    store,
		Logger:   logger,
		query:    &queries,
		BaseProvider: BaseProvider{
			ProviderType:   "K8S_OFFLINE",
			ProviderConfig: config,
		},
	}
	return &sparkOfflineStore, nil
}

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig ExecutorConfig
	StoreType      BlobStoreType
	StoreConfig    BlobStoreConfig
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

func NewLocalExecutor(scriptPath string) (Executor, error) {
	_, err := os.Open(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("could not find script path: %v", err)
	}
	return LocalExecutor{
		scriptPath: scriptPath,
	}, nil
}

func (kube KubernetesExecutor) ExecuteScript(envVars map[string]string) error {
	config := runner.KubernetesRunnerConfig{
		EnvVars: envVars,
		Image: kube.image,
		NumTasks: 1,
	}
	jobRunner, err := NewKubernetesRunner(config)
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

func (store *genericBlobStore) Write(key string, data []byte) error {
	err := store.bucket.WriteAll(ctx, key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (store *genericBlobStore) Read(key string) ([]byte, error) {
	data, err := store.bucket.ReadAll(ctx, key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (store *genericBlobStore) Serve(key string) (Iterator, error) {
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

func (store *genericBlobStore) Exists(key string) (bool, error) {
	return store.bucket.Exists(ctx, key)
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

func NewMemoryBlobStore() (MemoryBlobStore, error) {
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

func (k8s *K8sOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return nil, nil
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
	return nil, nil
}

func (k8s *K8sOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, nil
}

func (k8s *K8sOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return nil, nil
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