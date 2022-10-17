package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"time"

	"github.com/featureform/helpers"
	"github.com/featureform/kubernetes"

	parquet "github.com/segmentio/parquet-go"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	azureblob "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
)

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
		Memory:     NewMemoryBlobStore,
		FileSystem: NewFileBlobStore,
		Azure:      NewAzureBlobStore,
	}
	executorFactoryMap := map[ExecutorType]ExecutorFactory{
		GoProc: NewLocalExecutor,
		K8s:    NewKubernetesExecutor,
	}
	for storeType, factory := range blobStoreFactoryMap {
		RegisterBlobStoreFactory(string(storeType), factory)
	}
	for executorType, factory := range executorFactoryMap {
		RegisterExecutorFactory(string(executorType), factory)
	}
}

func k8sAzureOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	k8 := K8sAzureConfig{}
	logger := zap.NewExample().Sugar()
	if err := k8.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize k8s offline store", err)
		return nil, fmt.Errorf("invalid k8s config: %v", config)
	}
	logger.Info("Creating executor with type:", k8.ExecutorType)

	exec, err := CreateExecutor(string(k8.ExecutorType), Config([]byte("")))
	if err != nil {
		logger.Errorw("Failure initializing executor with type", k8.ExecutorType, err)
		return nil, err
	}

	logger.Info("Creating blob store with type:", k8.StoreType)
	serializedBlob, err := k8.StoreConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialize blob store config")
	}
	store, err := CreateBlobStore(string(k8.StoreType), Config(serializedBlob))
	if err != nil {
		logger.Errorw("Failure initializing blob store with type", k8.StoreType, err)
		return nil, err
	}

	logger.Debugf("Store type: %s", k8.StoreType)
	queries := defaultSparkOfflineQueries{}
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

	serializedBlob, err := k8.StoreConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialize blob store config")
	}

	logger.Info("Creating blob store with type:", k8.StoreType)
	store, err := CreateBlobStore(string(k8.StoreType), Config(serializedBlob))
	if err != nil {
		logger.Errorw("Failure initializing blob store with type", k8.StoreType, err)
		return nil, err
	}
	logger.Debugf("Store type: %s", k8.StoreType)
	queries := defaultSparkOfflineQueries{}
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
	K8s                 = "K8S"
)

const (
	Memory     BlobStoreType = "MEMORY"
	FileSystem               = "FILE_SYSTEM"
	Azure                    = "AZURE"
)

type K8sAzureConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig KubernetesExecutorConfig
	StoreType      BlobStoreType
	StoreConfig    AzureBlobStoreConfig
}

func (config *K8sAzureConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *K8sAzureConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize k8s config: %w", err)
	}
	return nil
}

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig ExecutorConfig
	StoreType      BlobStoreType
	StoreConfig    AzureBlobStoreConfig
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

type KubernetesExecutorConfig struct {
}

type KubernetesExecutor struct {
	image string
}

func (local LocalExecutor) ExecuteScript(envVars map[string]string) error {
	envVars["MODE"] = "local"
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
	envVars["MODE"] = "k8s",
	config := kubernetes.KubernetesRunnerConfig{
		EnvVars:  envVars,
		Image:    kube.image,
		NumTasks: 1,
		Resource: metadata.ResourceID{Name: envVars["RESOURCE_NAME"], Variant: envVars["RESOURCE_VARIANT"], Type: ProviderToMetadataResourceType[OfflineResourceType(envVars["RESOURCE_TYPE"])]},
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

func NewKubernetesExecutor(config Config) (Executor, error) {
	pandas_image := helpers.GetEnv("K8S_RUNNER_IMAGE", "featureformcom/k8s_runner:0.2.0-rc")
	return KubernetesExecutor{image: pandas_image}, nil
}

type BlobStore interface {
	Write(key string, data []byte) error
	Writer(key string) (*blob.Writer, error)
	Read(key string) ([]byte, error)
	Serve(key string) (Iterator, error)
	Exists(key string) (bool, error)
	Delete(key string) error
	DeleteAll(dir string) error
	NewestBlob(prefix string) string
	PathWithPrefix(path string) string
	NumRows(key string) (int64, error)
	Close() error
}

type Iterator interface {
	Next() (map[string]interface{}, error)
}

type MemoryBlobStore struct {
	genericBlobStore
}

type AzureBlobStore struct {
	ConnectionString string
	ContainerName    string
	Path             string
	genericBlobStore
}

func (azure *AzureBlobStore) addAzureVars(envVars map[string]string) map[string]string {
	envVars["AZURE_CONNECTION_STRING"] = azure.ConnectionString
	envVars["AZURE_CONTAINER_NAME"] = azure.ContainerName
	return envVars
}

type genericBlobStore struct {
	bucket *blob.Bucket
	path   string
}

func (store genericBlobStore) PathWithPrefix(path string) string {
	if len(store.path) > 4 && store.path[0:4] == "file" {
		return fmt.Sprintf("%s%s", store.path[len("file:////"):], path)
	} else {
		return path
	}
}

func (store AzureBlobStore) PathWithPrefix(path string) string {
	if len(path) != 0 && path[0:len(store.Path)] != store.Path && store.Path != "" {
		return fmt.Sprintf("%s/%s", store.Path, path)
	}
	return path
}

func (store genericBlobStore) NewestBlob(prefix string) string {
	opts := blob.ListOptions{
		Prefix: prefix,
	}
	listIterator := store.bucket.List(&opts)
	mostRecentTime := time.UnixMilli(0)
	mostRecentKey := ""
	for listObj, err := listIterator.Next(ctx); err == nil; listObj, err = listIterator.Next(ctx) {
		if listObj == nil {
			return ""
		}
		if !listObj.IsDir && (listObj.ModTime.After(mostRecentTime) || listObj.ModTime.Equal(mostRecentTime)) {
			mostRecentTime = listObj.ModTime
			mostRecentKey = listObj.Key
		}
	}
	return mostRecentKey
}

func (store genericBlobStore) DeleteAll(dir string) error {
	opts := blob.ListOptions{
		Prefix: dir,
	}
	listIterator := store.bucket.List(&opts)
	for listObj, err := listIterator.Next(ctx); err == nil; listObj, err = listIterator.Next(ctx) {
		if !listObj.IsDir {
			if err := store.bucket.Delete(ctx, listObj.Key); err != nil {
				return fmt.Errorf("failed to delete object %s in directory %s: %v", listObj.Key, dir, err)
			}
		}
	}
	return nil
}

func (store genericBlobStore) Write(key string, data []byte) error {
	err := store.bucket.WriteAll(ctx, key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (store genericBlobStore) Writer(key string) (*blob.Writer, error) {
	return store.bucket.NewWriter(ctx, key, nil)
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
	case "parquet":
		return parquetIteratorFromReader(r)
	default:
		return nil, fmt.Errorf("unsupported file type")
	}

}

func (store genericBlobStore) NumRows(key string) (int64, error) {
	r, err := store.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return 0, err
	}
	keyParts := strings.Split(key, ".")
	switch fileType := keyParts[len(keyParts)-1]; fileType {
	case "parquet":
		return getParquetNumRows(r)
	default:
		return 0, fmt.Errorf("unsupported file type")
	}
}

type ParquetIterator struct {
	rows  []interface{}
	index int64
}

func (p *ParquetIterator) Next() (map[string]interface{}, error) {
	if p.index+1 == int64(len(p.rows)) {
		return nil, nil
	}
	p.index += 1
	currentRow := p.rows[p.index]
	v := reflect.ValueOf(currentRow)
	returnMap := make(map[string]interface{})
	for _, key := range v.MapKeys() {
		returnMap[key.String()] = v.MapIndex(key).Interface()
	}
	return returnMap, nil
}

func getParquetNumRows(r io.ReadCloser) (int64, error) {
	defer r.Close()
	buff := bytes.NewBuffer([]byte{})
	size, err := io.Copy(buff, r)
	if err != nil {
		return 0, err
	}
	return int64(size), nil
}

func parquetIteratorFromReader(r io.ReadCloser) (Iterator, error) {
	defer r.Close()
	buff := bytes.NewBuffer([]byte{})
	size, err := io.Copy(buff, r)
	if err != nil {
		return nil, err
	}
	file := bytes.NewReader(buff.Bytes())
	rows, err := parquet.Read[any](file, size)
	if err != nil {
		return nil, err
	}
	return &ParquetIterator{
		rows:  rows,
		index: int64(0),
	}, nil
}

func (store genericBlobStore) Exists(key string) (bool, error) {
	return store.bucket.Exists(ctx, key)
}

func (store genericBlobStore) Delete(key string) error {
	return store.bucket.Delete(ctx, key)
}

func (store genericBlobStore) Close() error {
	return store.bucket.Close()
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

type FileBlobStoreConfig struct {
	DirPath string
}

func (config *FileBlobStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *FileBlobStoreConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

type FileBlobStore struct {
	genericBlobStore
}

func NewFileBlobStore(config Config) (BlobStore, error) {
	fileStoreConfig := FileBlobStoreConfig{}
	if err := fileStoreConfig.Deserialize(Config(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize file store config: %v", err)
	}
	bucket, err := blob.OpenBucket(ctx, fileStoreConfig.DirPath)
	if err != nil {
		return nil, err
	}
	return FileBlobStore{
		genericBlobStore{
			bucket: bucket,
			path:   fileStoreConfig.DirPath,
		},
	}, nil
}

type AzureBlobStoreConfig struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (config *AzureBlobStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *AzureBlobStoreConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

func NewAzureBlobStore(config Config) (BlobStore, error) {
	azureStoreConfig := AzureBlobStoreConfig{}
	if err := azureStoreConfig.Deserialize(Config(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize azure store config: %v", err)
	}
	os.Setenv("AZURE_STORAGE_ACCOUNT", azureStoreConfig.AccountName)
	os.Setenv("AZURE_STORAGE_KEY", azureStoreConfig.AccountKey)
	serviceURL := azureblob.ServiceURL(fmt.Sprintf("https://%s.blob.core.windows.net", azureStoreConfig.AccountName))
	client, err := azureblob.NewDefaultServiceClient(serviceURL)
	if err != nil {
		return AzureBlobStore{}, fmt.Errorf("Could not create azure client: %v", err)
	}

	bucket, err := azureblob.OpenBucket(ctx, client, azureStoreConfig.ContainerName, nil)
	if err != nil {
		return AzureBlobStore{}, fmt.Errorf("Could not open azure bucket: %v", err)
	}
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", azureStoreConfig.AccountName, azureStoreConfig.AccountKey)
	return AzureBlobStore{
		ConnectionString: connectionString,
		ContainerName:    azureStoreConfig.ContainerName,
		Path:             azureStoreConfig.Path,
		genericBlobStore: genericBlobStore{
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
	resourceKey := k8s.store.PathWithPrefix(blobResourcePath(id))
	resourceExists, err := k8s.store.Exists(resourceKey)
	if err != nil {
		k8s.logger.Errorw("Error checking if resource exists", "error", err)
		return nil, fmt.Errorf("error checking if resource registry exists: %v", err)
	}
	if resourceExists {
		k8s.logger.Errorw("Resource already exists in blob store", "id", id, "ResourceKey", resourceKey)
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
	iterator, err := tbl.store.Serve(tbl.sourcePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create iterator from source table: %v", err)
	}
	return &BlobIterator{iter: iterator, curIdx: 0, maxIdx: n}, nil
}

func (tbl *BlobPrimaryTable) NumRows() (int64, error) {
	return tbl.store.NumRows(tbl.sourcePath)
}

type BlobIterator struct {
	iter    Iterator
	err     error
	curIdx  int64
	maxIdx  int64
	records []interface{}
	columns []string
}

func (it *BlobIterator) Next() bool {
	it.curIdx += 1
	if it.curIdx > it.maxIdx {
		return false
	}
	values, err := it.iter.Next()
	if values == nil {
		return false
	}
	if err != nil {
		it.err = err
		return false
	}
	records := make([]interface{}, 0)
	columns := make([]string, 0)
	for k, v := range values {
		columns = append(columns, k)
		records = append(records, v)
	}
	it.columns = columns
	it.records = records
	return true
}

func (it *BlobIterator) Columns() []string {
	return it.columns
}

func (it *BlobIterator) Err() error {
	return it.err
}

func (it *BlobIterator) Values() GenericRecord {
	return GenericRecord(it.records)
}

func (it *BlobIterator) Close() error {
	return nil
}

func (k8s *K8sOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	resourceKey := k8s.store.PathWithPrefix(blobResourcePath(id))
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
	return k8s.transformation(config, false)
}

func (k8s *K8sOfflineStore) transformation(config TransformationConfig, isUpdate bool) error {
	if config.Type == SQLTransformation {
		return k8s.sqlTransformation(config, isUpdate)
	} else if config.Type == DFTransformation {
		return k8s.dfTransformation(config, isUpdate)
	} else {
		k8s.logger.Errorw("the transformation type is not supported", "type", config.Type)
		return fmt.Errorf("the transformation type '%v' is not supported", config.Type)
	}
}

func addETCDVars(envVars map[string]string) map[string]string {
	etcd_host := helpers.GetEnv("ETCD_HOST", "localhost")
	etcd_port := helpers.GetEnv("ETCD_PORT", "2379")
	etcd_password := helpers.GetEnv("ETCD_PASSWORD", "secretpassword")
	etcd_username := helpers.GetEnv("ETCD_USERNAME", "root")
	envVars["ETCD_HOST"] = etcd_host
	envVars["ETCD_PASSWORD"] = etcd_password
	envVars["ETCD_PORT"] = etcd_port
	envVars["ETCD_USERNAME"] = etcd_username
	return envVars
}

func (k8s *K8sOfflineStore) pandasRunnerArgs(outputURI string, updatedQuery string, sources []string, jobType JobType) map[string]string {
	sourceList := strings.Join(sources, ",")
	envVars := map[string]string{
		"OUTPUT_URI":          outputURI,
		"SOURCES":             sourceList,
		"TRANSFORMATION_TYPE": "sql",
		"TRANSFORMATION":      updatedQuery,
	}
	azureStore, ok := k8s.store.(AzureBlobStore)
	if ok {
		envVars = azureStore.addAzureVars(envVars)
	}
	return envVars
}

func (k8s K8sOfflineStore) getDFArgs(outputURI string, code string, mapping []SourceMapping, sources []string) map[string]string {
	// mappingSources := make([]string, len(mapping))
	// for i, item := range mapping {
	// 	mappingSources[i] = item.Source
	// }
	sourceList := strings.Join(sources, ",")
	envVars := map[string]string{
		"OUTPUT_URI":          outputURI,
		"SOURCES":             sourceList,
		"TRANSFORMATION_TYPE": "df",
		"TRANSFORMATION":      code,
	}
	_, ok := k8s.executor.(KubernetesExecutor)
	if ok {
		envVars = addETCDVars(envVars)
	}
	azureStore, ok := k8s.store.(AzureBlobStore)
	if ok {
		envVars = azureStore.addAzureVars(envVars)
	}
	return envVars
}

func addResourceID(envVars map[string]string, id ResourceID) map[string]string {
	envVars["RESOURCE_NAME"] = id.Name
	envVars["RESOURCE_VARIANT"] = id.Name
	envVars["RESOURCE_Type"] = string(id.Type)
	return envVars
}

func (k8s *K8sOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool) error {
	updatedQuery, sources, err := k8s.updateQuery(config.Query, config.SourceMapping)
	if err != nil {
		k8s.logger.Errorw("Could not generate updated query for k8s transformation", err)
		return err
	}

	transformationDestination := k8s.store.PathWithPrefix(blobResourcePath(config.TargetTableID))
	transformationDestinationExactPath := k8s.store.NewestBlob(transformationDestination)
	exists := transformationDestinationExactPath != ""
	if !isUpdate && exists {
		k8s.logger.Errorw("Creation when transformation already exists", config.TargetTableID, transformationDestination)
		return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination)
	} else if isUpdate && !exists {
		k8s.logger.Errorw("Update job attempted when transformation does not exist", config.TargetTableID, transformationDestination)
		return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination)
	}
	k8s.logger.Debugw("Running SQL transformation", config)
	runnerArgs := k8s.pandasRunnerArgs(transformationDestination, updatedQuery, sources, Transform)
	runnerArgs := addResourceID(runnerArgs, config.TargetTableID)
	if err := k8s.executor.ExecuteScript(runnerArgs); err != nil {
		k8s.logger.Errorw("job for transformation failed to run", config.TargetTableID, err)
		return fmt.Errorf("job for transformation %v failed to run: %v", config.TargetTableID, err)
	}
	k8s.logger.Debugw("Succesfully ran SQL transformation", config)
	return nil
}

func (k8s *K8sOfflineStore) dfTransformation(config TransformationConfig, isUpdate bool) error {
	_, sources, err := k8s.updateQuery(config.Query, config.SourceMapping)
	if err != nil {
		return err
	}
	transformationDestination := k8s.store.PathWithPrefix(blobResourcePath(config.TargetTableID))
	exists, err := k8s.store.Exists(transformationDestination)
	if err != nil {
		k8s.logger.Errorw("Error checking if resource exists", err)
		return err
	}

	if !isUpdate && exists {
		k8s.logger.Errorw("Transformation already exists", config.TargetTableID, transformationDestination)
		return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination)
	} else if isUpdate && !exists {
		k8s.logger.Errorw("Transformation doesn't exists at destination and you are trying to update", config.TargetTableID, transformationDestination)
		return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination)
	}

	transformationFilePath := k8s.store.PathWithPrefix(blobResourcePath(config.TargetTableID))
	fileName := "transformation.pkl"
	transformationFileLocation := fmt.Sprintf("%s%s", transformationFilePath, fileName)
	err = k8s.store.Write(transformationFileLocation, config.Code)
	if err != nil {
		return fmt.Errorf("could not upload file: %s", err)
	}

	k8sArgs := k8s.getDFArgs(transformationDestination, transformationFileLocation, config.SourceMapping, sources)
	k8sArgs := addResourceID(k8sArgs, config.TargetTableID)
	k8s.logger.Debugw("Running DF transformation", config)
	if err := k8s.executor.ExecuteScript(k8sArgs); err != nil {
		k8s.logger.Errorw("Error running dataframe job", err)
		return fmt.Errorf("submit job for transformation %v failed to run: %v", config.TargetTableID, err)
	}
	k8s.logger.Debugw("Succesfully ran DF transformation", config)
	return nil
}

func (k8s *K8sOfflineStore) updateQuery(query string, mapping []SourceMapping) (string, []string, error) {
	sources := make([]string, len(mapping))
	replacements := make([]string, len(mapping)*2) // It's times 2 because each replacement will be a pair; (original, replacedValue)

	for i, m := range mapping {
		replacements = append(replacements, m.Template)
		replacements = append(replacements, fmt.Sprintf("source_%v", i))

		sourcePath := ""
		sourcePath, err := k8s.getSourcePath(m.Source)
		if err != nil {
			k8s.logger.Errorw("Error getting source path of source", m.Source, err)
			return "", nil, fmt.Errorf("could not get the sourcePath for %s because %s", m.Source, err)
		}

		sources[i] = sourcePath
	}

	replacer := strings.NewReplacer(replacements...)
	updatedQuery := replacer.Replace(query)

	if strings.Contains(updatedQuery, "{{") {
		k8s.logger.Errorw("Template replace failed", updatedQuery)
		return "", nil, fmt.Errorf("could not replace all the templates with the current mapping. Mapping: %v; Replaced Query: %s", mapping, updatedQuery)
	}
	return updatedQuery, sources, nil
}

func (k8s *K8sOfflineStore) getSourcePath(path string) (string, error) {
	fileType, fileName, fileVariant := k8s.getResourceInformationFromFilePath(path)

	var filePath string
	if fileType == "primary" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Primary}
		fileTable, err := k8s.GetPrimaryTable(fileResourceId)
		if err != nil {
			k8s.logger.Errorw("Issue getting primary table", fileResourceId, err)
			return "", fmt.Errorf("could not get the primary table for {%v} because %s", fileResourceId, err)
		}
		filePath = fileTable.GetName()
		return filePath, nil
	} else if fileType == "transformation" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Transformation}
		fileResourcePath := blobResourcePath(fileResourceId)
		exactFileResourcePath := k8s.store.NewestBlob(fileResourcePath)
		if exactFileResourcePath == "" {
			k8s.logger.Errorw("Issue getting transformation table", fileResourceId)
			return "", fmt.Errorf("could not get the transformation table for {%v}", fileResourceId)
		}
		filePath := k8s.store.PathWithPrefix(exactFileResourcePath[:strings.LastIndex(exactFileResourcePath, "/")+1])
		return filePath, nil
	} else {
		return filePath, fmt.Errorf("could not find path for %s; fileType: %s, fileName: %s, fileVariant: %s", path, fileType, fileName, fileVariant)
	}
}

func (k8s *K8sOfflineStore) getResourceInformationFromFilePath(path string) (string, string, string) {
	var fileType string
	var fileName string
	var fileVariant string
	if path[:5] == "s3://" {
		filePaths := strings.Split(path[len("s3://"):], "/")
		if len(filePaths) <= 4 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = strings.ToLower(filePaths[2]), filePaths[3], filePaths[4]
	} else {
		filePaths := strings.Split(path[len("featureform_"):], "__")
		if len(filePaths) <= 2 {
			return "", "", ""
		}
		fileType, fileName, fileVariant = filePaths[0], filePaths[1], filePaths[2]
	}
	return fileType, fileName, fileVariant
}

func (k8s *K8sOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	k8s.logger.Debugw("Getting transformation table", "ResourceID", id)
	transformationPath := k8s.store.PathWithPrefix(blobResourcePath(id))
	transformationExactPath := k8s.store.NewestBlob(transformationPath)
	if transformationPath == "" {
		k8s.logger.Errorw("Could not get transformation table")
		return nil, fmt.Errorf("could not get transformation table (%v)", id)
	}
	k8s.logger.Debugw("Succesfully retrieved transformation table", "ResourceID", id)
	return &BlobPrimaryTable{k8s.store, transformationExactPath, true, id}, nil
}

func (k8s *K8sOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return k8s.transformation(config, true)
}
func (k8s *K8sOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (k8s *K8sOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	resourceKey := k8s.store.PathWithPrefix(blobResourcePath(id))
	k8s.logger.Debugw("Getting primary table", id)
	table, err := k8s.store.Read(resourceKey)
	if err != nil {
		return nil, fmt.Errorf("error fetching primary table: %v", err)
	}
	k8s.logger.Debugw("Succesfully retrieved primary table", id)
	return &BlobPrimaryTable{k8s.store, string(table), false, id}, nil
}

func (k8s *K8sOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (k8s *K8sOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	resourcekey := k8s.store.PathWithPrefix(blobResourcePath(id))
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
	return &BlobOfflineTable{resourceSchema}, nil
}

func (k8s *K8sOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	return k8s.materialization(id, false)
}

func (k8s *K8sOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	s := strings.Split(string(id), "/")
	if len(s) != 3 {
		k8s.logger.Errorw("Invalid materialization id", id)
		return nil, fmt.Errorf("invalid materialization id")
	}
	materializationID := ResourceID{s[1], s[2], FeatureMaterialization}
	k8s.logger.Debugw("Getting materialization", "id", id)
	materializationPath := k8s.store.PathWithPrefix(blobResourcePath(materializationID))
	materializationExactPath := k8s.store.NewestBlob(materializationPath)
	if materializationExactPath == "" {
		k8s.logger.Errorw("Could not fetch materialization resource key")
		return nil, fmt.Errorf("Could not fetch materialization resource key")
	}
	k8s.logger.Debugw("Succesfully retrieved materialization", "id", id)
	return &BlobMaterialization{materializationID, k8s.store, materializationExactPath}, nil
}

type BlobMaterialization struct {
	id    ResourceID
	store BlobStore
	key   string
}

func (mat BlobMaterialization) ID() MaterializationID {
	return MaterializationID(fmt.Sprintf("%s/%s/%s", FeatureMaterialization, mat.id.Name, mat.id.Variant))
}

func (mat BlobMaterialization) NumRows() (int64, error) {
	materializationPath := mat.store.PathWithPrefix(blobResourcePath(mat.id))
	latestMaterializationPath := mat.store.NewestBlob(materializationPath)
	return mat.store.NumRows(latestMaterializationPath)
}

func (mat BlobMaterialization) IterateSegment(begin, end int64) (FeatureIterator, error) {
	materializationPath := mat.store.PathWithPrefix(blobResourcePath(mat.id))
	latestMaterializationPath := mat.store.NewestBlob(materializationPath)
	iter, err := mat.store.Serve(latestMaterializationPath)
	if err != nil {
		return nil, err
	}
	for i := int64(0); i < begin; i++ {
		_, _ = iter.Next()
	}
	return &BlobFeatureIterator{iter: iter, curIdx: 0, maxIdx: end}, nil
}

type BlobFeatureIterator struct {
	iter   Iterator
	err    error
	cur    ResourceRecord
	curIdx int64
	maxIdx int64
}

func (iter *BlobFeatureIterator) Next() bool {
	iter.curIdx += 1
	if iter.curIdx > iter.maxIdx {
		return false
	}
	nextVal, err := iter.iter.Next()
	if err != nil {
		iter.err = err
		return false
	}
	if nextVal == nil {
		return false
	}
	formatDate := "2006-01-02 15:04:05 UTC" // hardcoded golang format date
	var timestamp time.Time
	timeString, ok := nextVal["ts"].(string)
	if !ok {
		iter.cur = ResourceRecord{Entity: string(nextVal["entity"].(string)), Value: nextVal["value"]}
	} else {
		timestamp, err = time.Parse(formatDate, timeString)
		if err != nil {
			iter.err = fmt.Errorf("could not parse timestamp: %v: %v", nextVal["ts"], err)
			return false
		}
		iter.cur = ResourceRecord{Entity: string(nextVal["entity"].(string)), Value: nextVal["value"], TS: timestamp}
	}

	return true
}

func (iter *BlobFeatureIterator) Value() ResourceRecord {
	return iter.cur
}

func (iter *BlobFeatureIterator) Err() error {
	return iter.err
}

func (iter *BlobFeatureIterator) Close() error {
	return nil
}

func (k8s *K8sOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return k8s.materialization(id, true)
}

func (k8s *K8sOfflineStore) materialization(id ResourceID, isUpdate bool) (Materialization, error) {
	if id.Type != Feature {
		k8s.logger.Errorw("Attempted to create a materialization of a non feature resource", id.Type)
		return nil, fmt.Errorf("only features can be materialized")
	}
	resourceTable, err := k8s.GetResourceTable(id)
	if err != nil {
		k8s.logger.Errorw("Attempted to fetch resource table of non registered resource", err)
		return nil, fmt.Errorf("resource not registered: %v", err)
	}
	k8sResourceTable, ok := resourceTable.(*BlobOfflineTable)
	if !ok {
		k8s.logger.Errorw("Could not convert resource table to blob offline table", id)
		return nil, fmt.Errorf("could not convert offline table with id %v to k8sResourceTable", id)
	}
	materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	destinationPath := k8s.store.PathWithPrefix(blobResourcePath(materializationID))
	materializationExists, err := k8s.store.Exists(destinationPath)
	if err != nil {
		k8s.logger.Errorw("Could not determine whether materialization exists", err)
		return nil, fmt.Errorf("error checking if materialization exists: %v", err)
	}
	if !isUpdate && materializationExists {
		k8s.logger.Errorw("Attempted to materialize a materialization that already exists", id)
		return nil, fmt.Errorf("materialization already exists")
	} else if isUpdate && !materializationExists {
		k8s.logger.Errorw("Attempted to update a materialization that does not exist", id)
		return nil, fmt.Errorf("materialization does not exist")
	}
	materializationQuery := k8s.query.materializationCreate(k8sResourceTable.schema)
	sourcePath := k8s.store.PathWithPrefix(k8sResourceTable.schema.SourceTable)
	k8sArgs := k8s.pandasRunnerArgs(destinationPath, materializationQuery, []string{sourcePath}, Materialize)
	k8sArgs := addResourceID(k8sArgs, id)
	k8s.logger.Debugw("Creating materialization", "id", id)
	if err := k8s.executor.ExecuteScript(k8sArgs); err != nil {
		k8s.logger.Errorw("Job failed to run", err)
		return nil, fmt.Errorf("job for materialization %v failed to run: %v", materializationID, err)
	}
	matPath := k8s.store.PathWithPrefix(blobResourcePath(materializationID))
	latestMatPath := k8s.store.NewestBlob(matPath)
	if latestMatPath == "" {
		return nil, fmt.Errorf("Materialization does not exist")
	}
	k8s.logger.Debugw("Succesfully created materialization", "id", id)
	return &BlobMaterialization{materializationID, k8s.store, latestMatPath}, nil
}

func (k8s *K8sOfflineStore) DeleteMaterialization(id MaterializationID) error {
	s := strings.Split(string(id), "/")
	if len(s) != 3 {
		k8s.logger.Errorw("Invalid materialization id", id)
		return fmt.Errorf("invalid materialization id")
	}
	materializationID := ResourceID{s[1], s[2], FeatureMaterialization}
	materializationPath := k8s.store.PathWithPrefix(blobResourcePath(materializationID))
	materializationExactPath := k8s.store.NewestBlob(materializationPath)
	if materializationExactPath == "" {
		return fmt.Errorf("materialization does not exist")
	}
	return k8s.store.Delete(materializationExactPath)
}

func (k8s *K8sOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	return k8s.trainingSet(def, false)
}

func (k8s *K8sOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return k8s.trainingSet(def, true)
}

func (k8s *K8sOfflineStore) registeredResourceSchema(id ResourceID) (ResourceSchema, error) {
	k8s.logger.Debugw("Getting resource schema", "id", id)
	table, err := k8s.GetResourceTable(id)
	if err != nil {
		k8s.logger.Errorw("Resource not registered in blob store", id, err)
		return ResourceSchema{}, fmt.Errorf("resource not registered: %v", err)
	}
	blobResourceTable, ok := table.(*BlobOfflineTable)
	if !ok {
		k8s.logger.Errorw("could not convert offline table to blobResourceTable", id)
		return ResourceSchema{}, fmt.Errorf("could not convert offline table with id %v to blobResourceTable", id)
	}
	k8s.logger.Debugw("Succesfully retrieved resource schema", "id", id)
	return blobResourceTable.schema, nil
}

func (s *S3Store) BlobPath(sourceKey string) string {
	if !strings.Contains(sourceKey, "s3://") {
		return fmt.Sprintf("%s%s", s.BucketPrefix(), sourceKey)
	}
	return sourceKey
}

func (k8s *K8sOfflineStore) trainingSet(def TrainingSetDef, isUpdate bool) error {
	if err := def.check(); err != nil {
		k8s.logger.Errorw("Training set definition not valid", def, err)
		return err
	}
	sourcePaths := make([]string, 0)
	featureSchemas := make([]ResourceSchema, 0)
	destinationPath := k8s.store.PathWithPrefix(blobResourcePath(def.ID))
	trainingSetExactPath := k8s.store.NewestBlob(destinationPath)
	trainingSetExists := !(trainingSetExactPath == "")
	if !isUpdate && trainingSetExists {
		k8s.logger.Errorw("Training set already exists", def.ID)
		return fmt.Errorf("training set already exists: %v", def.ID)
	} else if isUpdate && !trainingSetExists {
		k8s.logger.Errorw("Training set doesn't exist for update job", def.ID)
		return fmt.Errorf("Training set doesn't exist for update job: %v", def.ID)
	}
	labelSchema, err := k8s.registeredResourceSchema(def.Label)
	if err != nil {
		k8s.logger.Errorw("Could not get schema of label in store", def.Label, err)
		return fmt.Errorf("Could not get schema of label %s: %v", def.Label, err)
	}
	labelPath := k8s.store.PathWithPrefix(labelSchema.SourceTable)
	sourcePaths = append(sourcePaths, labelPath)
	for _, feature := range def.Features {
		featureSchema, err := k8s.registeredResourceSchema(feature)
		if err != nil {
			k8s.logger.Errorw("Could not get schema of feature in store", feature, err)
			return fmt.Errorf("Could not get schema of feature %s: %v", feature, err)
		}
		featurePath := k8s.store.PathWithPrefix(featureSchema.SourceTable)
		sourcePaths = append(sourcePaths, featurePath)
		featureSchemas = append(featureSchemas, featureSchema)
	}
	trainingSetQuery := k8s.query.trainingSetCreate(def, featureSchemas, labelSchema)
	pandasArgs := k8s.pandasRunnerArgs(k8s.store.PathWithPrefix(destinationPath), trainingSetQuery, sourcePaths, CreateTrainingSet)
	pandasArgs := addResourceID(pandasArgs, def.ID)
	k8s.logger.Debugw("Creating training set", "definition", def)
	if err := k8s.executor.ExecuteScript(pandasArgs); err != nil { //
		k8s.logger.Errorw("training set job failed to run", "definition", def.ID, "error", err)
		return fmt.Errorf("job for training set %v failed to run: %v", def.ID, err)
	}
	k8s.logger.Debugw("Succesfully created training set:", "definition", def)
	return nil
}

func (k8s *K8sOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		k8s.logger.Errorw("id is not of type training set", err)
		return nil, err
	}
	resourceKeyPrefix := k8s.store.PathWithPrefix(blobResourcePath(id))
	trainingSetExactPath := k8s.store.NewestBlob(resourceKeyPrefix)
	if trainingSetExactPath == "" {
		return nil, fmt.Errorf("No training set resource found")
	}
	iterator, err := k8s.store.Serve(trainingSetExactPath)
	if err != nil {
		return nil, err
	}
	return &BlobTrainingSet{id: id, store: k8s.store, key: trainingSetExactPath, iter: iterator}, nil
}

type BlobTrainingSet struct {
	id       ResourceID
	store    BlobStore
	key      string
	iter     Iterator
	Error    error
	features []interface{}
	label    interface{}
}

func (ts *BlobTrainingSet) Next() bool {
	row, err := ts.iter.Next()
	if err != nil {
		ts.Error = err
		return false
	}
	if row == nil {
		return false
	}
	values := make([]interface{}, 0)
	for _, val := range row {
		values = append(values, val)
	}
	ts.features = values[0 : len(row)-1]
	ts.label = values[len(row)-1]
	return true
}

func (ts *BlobTrainingSet) Features() []interface{} {
	return ts.features
}

func (ts *BlobTrainingSet) Label() interface{} {
	return ts.label
}

func (ts *BlobTrainingSet) Err() error {
	return ts.Error
}
