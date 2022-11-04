package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/featureform/helpers"
	"github.com/featureform/kubernetes"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"

	"github.com/segmentio/parquet-go"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
)

type K8sOfflineStore struct {
	executor Executor
	store    FileStore
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

type FileStoreFactory func(config Config) (FileStore, error)

var fileStoreFactoryMap = make(map[string]FileStoreFactory)

func RegisterFileStoreFactory(name string, FileStoreFactory FileStoreFactory) error {
	if _, exists := fileStoreFactoryMap[name]; exists {
		return fmt.Errorf("factory already registered: %s", name)
	}
	fileStoreFactoryMap[name] = FileStoreFactory
	return nil
}

func UnregisterFileStoreFactory(name string) error {
	if _, exists := fileStoreFactoryMap[name]; !exists {
		return fmt.Errorf("factory %s not registered", name)
	}
	delete(fileStoreFactoryMap, name)
	return nil
}

func CreateFileStore(name string, config Config) (FileStore, error) {
	factory, exists := fileStoreFactoryMap[name]
	if !exists {
		return nil, fmt.Errorf("factory does not exist: %s", name)
	}
	FileStore, err := factory(config)
	if err != nil {
		return nil, err
	}
	return FileStore, nil
}

func init() {
	FileStoreFactoryMap := map[FileStoreType]FileStoreFactory{
		FileSystem: NewFileFileStore,
		Azure:      NewAzureFileStore,
	}
	executorFactoryMap := map[ExecutorType]ExecutorFactory{
		GoProc: NewLocalExecutor,
		K8s:    NewKubernetesExecutor,
	}
	for storeType, factory := range FileStoreFactoryMap {
		RegisterFileStoreFactory(string(storeType), factory)
	}
	for executorType, factory := range executorFactoryMap {
		RegisterExecutorFactory(string(executorType), factory)
	}
}

func k8sAzureOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	k8 := K8sAzureConfig{}
	logger := logging.NewLogger("kubernetes")
	if err := k8.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize k8s offline store", "error", err)
		return nil, fmt.Errorf("invalid k8s config: %v", config)
	}
	logger.Info("Creating executor with type:", k8.ExecutorType)

	exec, err := CreateExecutor(string(k8.ExecutorType), Config([]byte("")))
	if err != nil {
		logger.Errorw("Failure initializing executor with type", "executor_type", k8.ExecutorType, "error", err)
		return nil, err
	}

	logger.Info("Creating blob store with type:", k8.StoreType)
	serializedBlob, err := k8.StoreConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialize blob store config")
	}
	store, err := CreateFileStore(string(k8.StoreType), Config(serializedBlob))
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
	logger := logging.NewLogger("kubernetes")
	if err := k8.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize k8s offline store", "error", err)
		return nil, fmt.Errorf("invalid k8s config: %v", config)
	}
	logger.Info("Creating executor with type:", k8.ExecutorType)
	exec, err := CreateExecutor(string(k8.ExecutorType), Config(k8.ExecutorConfig))
	if err != nil {
		logger.Errorw("Failure initializing executor with type", "executor_type", k8.ExecutorType, "error", err)
		return nil, err
	}

	serializedBlob, err := k8.StoreConfig.Serialize()
	if err != nil {
		return nil, fmt.Errorf("could not serialize blob store config")
	}

	logger.Info("Creating blob store with type:", k8.StoreType)
	store, err := CreateFileStore(string(k8.StoreType), Config(serializedBlob))
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

type FileStoreConfig []byte

type ExecutorType string

type FileStoreType string

const (
	GoProc ExecutorType = "GO_PROCESS"
	K8s                 = "K8S"
)

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem               = "FILE_SYSTEM"
	Azure                    = "AZURE"
	S3                       = "S3"
)

type K8sAzureConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig KubernetesExecutorConfig
	StoreType      FileStoreType
	StoreConfig    AzureFileStoreConfig
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
	StoreType      FileStoreType
	StoreConfig    AzureFileStoreConfig
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
	envVars["MODE"] = "k8s"
	resourceType, err := strconv.Atoi(envVars["RESOURCE_TYPE"])
	if err != nil {
		resourceType = 0
	}
	config := kubernetes.KubernetesRunnerConfig{
		EnvVars:  envVars,
		Image:    kube.image,
		NumTasks: 1,
		Resource: metadata.ResourceID{
			Name:    envVars["RESOURCE_NAME"],
			Variant: envVars["RESOURCE_VARIANT"],
			Type:    ProviderToMetadataResourceType[OfflineResourceType(resourceType)],
		},
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
	pandas_image := helpers.GetEnv("PANDAS_RUNNER_IMAGE", "featureformcom/k8s_runner:0.3.0-rc")
	return KubernetesExecutor{image: pandas_image}, nil
}

type FileStore interface {
	Write(key string, data []byte) error
	Writer(key string) (*blob.Writer, error)
	Read(key string) ([]byte, error)
	Serve(key string) (Iterator, error)
	Exists(key string) (bool, error)
	Delete(key string) error
	DeleteAll(dir string) error
	NewestFile(prefix string) (string, error)
	PathWithPrefix(path string, remote bool) string
	NumRows(key string) (int64, error)
	Close() error
}

type Iterator interface {
	Next() (map[string]interface{}, error)
}

type AzureFileStore struct {
	AccountName string
	ConnectionString string
	ContainerName    string
	Path             string
	genericFileStore
}

func (azure *AzureFileStore) addAzureVars(envVars map[string]string) map[string]string {
	envVars["AZURE_CONNECTION_STRING"] = azure.ConnectionString
	envVars["AZURE_CONTAINER_NAME"] = azure.ContainerName
	return envVars
}

type genericFileStore struct {
	bucket *blob.Bucket
	path   string
}

func (store genericFileStore) PathWithPrefix(path string) string {
	if len(store.path) > 4 && store.path[0:4] == "file" {
		return fmt.Sprintf("%s%s", store.path[len("file:///"):], path)
	} else {
		return path
	}
}

func (store AzureFileStore) PathWithPrefix(path string, remote bool) string {
	if !remote {
		if len(path) != 0 && path[0:len(store.Path)] != store.Path && store.Path != "" {
			return fmt.Sprintf("%s/%s", store.Path, path)
		}
	}
	if remote {
		return fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/%s%s", store.ContainerName, store.AccountName, store.Path, path)
	}
	return path
}

func (store genericFileStore) NewestFile(prefix string) (string, error) {
	opts := blob.ListOptions{
		Prefix: prefix,
	}
	listIterator := store.bucket.List(&opts)
	mostRecentTime := time.UnixMilli(0)
	mostRecentKey := ""
	for {
		if listObj, err := listIterator.Next(context.TODO()); err == nil {
			if !listObj.IsDir && (listObj.ModTime.After(mostRecentTime) || listObj.ModTime.Equal(mostRecentTime)) {
				mostRecentTime = listObj.ModTime
				mostRecentKey = listObj.Key
			}
		} else if err == io.EOF {
			return mostRecentKey, nil
		} else {
			return "", err
		}
	}
}

func (store genericFileStore) outputFileList(prefix string) []string {
	opts := blob.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	}
	listIterator := store.bucket.List(&opts)
	mostRecentOutputPartTime := "0000-00-00 00:00:00.000000"
	mostRecentOutputPartPath := ""
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if listObj == nil {
			return []string{}
		}
		dirParts := strings.Split(listObj.Key[:len(listObj.Key)-1], "/")
		timestamp := dirParts[len(dirParts)-1]
		if listObj.IsDir && timestamp > mostRecentOutputPartTime {
			mostRecentOutputPartTime = timestamp
			mostRecentOutputPartPath = listObj.Key
		}
	}
	opts = blob.ListOptions{
		Prefix: mostRecentOutputPartPath,
	}
	partsIterator := store.bucket.List(&opts)
	partsList := make([]string, 0)
	for listObj, err := partsIterator.Next(context.TODO()); err == nil; listObj, err = partsIterator.Next(context.TODO()) {
		pathParts := strings.Split(listObj.Key, ".")

		fileType := pathParts[len(pathParts)-1]
		if fileType == "parquet" {
			partsList = append(partsList, listObj.Key)
		}
	}
	sort.Strings(partsList)
	return partsList
}

func (store genericFileStore) DeleteAll(dir string) error {
	opts := blob.ListOptions{
		Prefix: dir,
	}
	listIterator := store.bucket.List(&opts)
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if !listObj.IsDir {
			if err := store.bucket.Delete(context.TODO(), listObj.Key); err != nil {
				return fmt.Errorf("failed to delete object %s in directory %s: %v", listObj.Key, dir, err)
			}
		}
	}
	return nil
}

func (store genericFileStore) Write(key string, data []byte) error {
	err := store.bucket.WriteAll(context.TODO(), key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (store genericFileStore) Writer(key string) (*blob.Writer, error) {
	return store.bucket.NewWriter(context.TODO(), key, nil)
}

func (store genericFileStore) Read(key string) ([]byte, error) {
	data, err := store.bucket.ReadAll(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (store genericFileStore) ServeDirectory(dir string) (Iterator, error) {
	fileParts := store.outputFileList(dir)
	if len(fileParts) == 0 {
		return nil, fmt.Errorf("no files in given directory")
	}
	// assume file type is parquet
	return parquetIteratorOverMultipleFiles(fileParts, store)
}

func convertToParquetBytes(list []any) ([]byte, error) {
	if len(list) == 0 {
		return nil, fmt.Errorf("list is empty")
	}
	schema := parquet.SchemaOf(list[0])
	buf := new(bytes.Buffer)
	err := parquet.Write[any](
		buf,
		list,
		schema,
	)
	if err != nil {
		return nil, fmt.Errorf("Could not write parquet file to bytes: %v", err)
	}
	return buf.Bytes(), nil
}

type ParquetIteratorMultipleFiles struct {
	fileList     []string
	currentFile  int64
	fileIterator Iterator
	store        genericFileStore
}

func parquetIteratorOverMultipleFiles(fileParts []string, store genericFileStore) (Iterator, error) {
	b, err := store.bucket.ReadAll(context.TODO(), fileParts[0])
	if err != nil {
		return nil, err
	}
	iterator, err := parquetIteratorFromBytes(b)
	if err != nil {
		return nil, fmt.Errorf("could not open first parquet file: %v", err)
	}
	return &ParquetIteratorMultipleFiles{
		fileList:     fileParts,
		currentFile:  int64(0),
		fileIterator: iterator,
		store:        store,
	}, nil
}

func (p *ParquetIteratorMultipleFiles) Next() (map[string]interface{}, error) {
	nextRow, err := p.fileIterator.Next()
	if err != nil {
		return nil, err
	}
	if nextRow == nil {
		if p.currentFile+1 == int64(len(p.fileList)) {
			return nil, nil
		}
		p.currentFile += 1
		b, err := p.store.bucket.ReadAll(context.TODO(), p.fileList[p.currentFile])
		if err != nil {
			return nil, err
		}
		iterator, err := parquetIteratorFromBytes(b)
		if err != nil {
			return nil, err
		}
		p.fileIterator = iterator
		return p.fileIterator.Next()
	}
	return nextRow, nil
}

func (store genericFileStore) Serve(key string) (Iterator, error) {
	keyParts := strings.Split(key, ".")
	if len(keyParts) == 1 {
		return store.ServeDirectory(key)
	}
	b, err := store.bucket.ReadAll(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	switch fileType := keyParts[len(keyParts)-1]; fileType {
	case "parquet":
		return parquetIteratorFromBytes(b)
	case "csv":
		return nil, fmt.Errorf("could not find CSV reader")
	default:
		return nil, fmt.Errorf("unsupported file type")
	}

}

func (store genericFileStore) NumRows(key string) (int64, error) {
	b, err := store.bucket.ReadAll(context.TODO(), key)
	if err != nil {
		return 0, err
	}
	keyParts := strings.Split(key, ".")
	switch fileType := keyParts[len(keyParts)-1]; fileType {
	case "parquet":
		return getParquetNumRows(b)
	default:
		return 0, fmt.Errorf("unsupported file type")
	}
}

type ParquetIterator struct {
	reader *parquet.Reader
	index  int64
}

func (p *ParquetIterator) Next() (map[string]interface{}, error) {
	value := make(map[string]interface{})
	err := p.reader.Read(&value)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return value, nil
}

func getParquetNumRows(b []byte) (int64, error) {
	file := bytes.NewReader(b)
	r := parquet.NewReader(file)
	return r.NumRows(), nil
}

func parquetIteratorFromBytes(b []byte) (Iterator, error) {
	file := bytes.NewReader(b)
	r := parquet.NewReader(file)
	return &ParquetIterator{
		reader: r,
		index:  int64(0),
	}, nil
}

func (store genericFileStore) Exists(key string) (bool, error) {
	return store.bucket.Exists(context.TODO(), key)
}

func (store genericFileStore) Delete(key string) error {
	return store.bucket.Delete(context.TODO(), key)
}

func (store genericFileStore) Close() error {
	return store.bucket.Close()
}

type FileFileStoreConfig struct {
	DirPath string
}

func (config *FileFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *FileFileStoreConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

type FileFileStore struct {
	DirPath string
	genericFileStore
}

func NewFileFileStore(config Config) (FileStore, error) {
	fileStoreConfig := FileFileStoreConfig{}
	if err := fileStoreConfig.Deserialize(Config(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize file store config: %v", err)
	}
	bucket, err := blob.OpenBucket(context.TODO(), fileStoreConfig.DirPath)
	if err != nil {
		return nil, err
	}
	return FileFileStore{
		DirPath: fileStoreConfig.DirPath[len("file:///"):],
		genericFileStore: genericFileStore{
			bucket: bucket,
			path:   fileStoreConfig.DirPath,
		},
	}, nil
}

type AzureFileStoreConfig struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (config *AzureFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *AzureFileStoreConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

func NewAzureFileStore(config Config) (FileStore, error) {
	azureStoreConfig := AzureFileStoreConfig{}
	if err := azureStoreConfig.Deserialize(Config(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize azure store config: %v", err)
	}
	if err := os.Setenv("AZURE_STORAGE_ACCOUNT", azureStoreConfig.AccountName); err != nil {
		return nil, fmt.Errorf("could not set storage account env: %w", err)
	}

	if err := os.Setenv("AZURE_STORAGE_KEY", azureStoreConfig.AccountKey); err != nil {
		return nil, fmt.Errorf("could not set storage key env: %w", err)
	}
	serviceURL := azureblob.ServiceURL(fmt.Sprintf("https://%s.blob.core.windows.net", azureStoreConfig.AccountName))
	client, err := azureblob.NewDefaultServiceClient(serviceURL)
	if err != nil {
		return AzureFileStore{}, fmt.Errorf("Could not create azure client: %v", err)
	}

	bucket, err := azureblob.OpenBucket(context.TODO(), client, azureStoreConfig.ContainerName, nil)
	if err != nil {
		return AzureFileStore{}, fmt.Errorf("Could not open azure bucket: %v", err)
	}
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", azureStoreConfig.AccountName, azureStoreConfig.AccountKey)
	return AzureFileStore{
		AccountName: azureStoreConfig.AccountName,
		ConnectionString: connectionString,
		ContainerName:    azureStoreConfig.ContainerName,
		Path:             azureStoreConfig.Path,
		genericFileStore: genericFileStore{
			bucket: bucket,
		},
	}, nil
}

func ResourcePrefix(id ResourceID) string {
	return fmt.Sprintf("featureform/%s/%s/%s", id.Type, id.Name, id.Variant)
}

func fileStoreResourcePath(id ResourceID) string {
	return ResourcePrefix(id)
}

type BlobOfflineTable struct {
	schema ResourceSchema
}

func (tbl *BlobOfflineTable) Write(ResourceRecord) error {
	return fmt.Errorf("not yet implemented")
}

func (k8s *K8sOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema) (OfflineTable, error) {
	return blobRegisterResource(id, schema, k8s.logger, k8s.store)

}

func blobRegisterResource(id ResourceID, schema ResourceSchema, logger *zap.SugaredLogger, store FileStore) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		logger.Errorw("Failure checking ID", "error", err)
		return nil, fmt.Errorf("ID check failed: %v", err)
	}
	resourceKey := store.PathWithPrefix(fileStoreResourcePath(id))
	resourceExists, err := store.Exists(resourceKey)
	if err != nil {
		logger.Errorw("Error checking if resource exists", "error", err)
		return nil, fmt.Errorf("error checking if resource registry exists: %v", err)
	}
	if resourceExists {
		logger.Errorw("Resource already exists in blob store", "id", id, "ResourceKey", resourceKey)
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	serializedSchema, err := schema.Serialize()
	if err != nil {
		return nil, fmt.Errorf("error serializing resource schema: %s: %s", schema, err)
	}
	if err := store.Write(resourceKey, serializedSchema); err != nil {
		return nil, fmt.Errorf("error writing resource schema: %s: %s", schema, err)
	}
	logger.Debugw("Registered resource table", "resourceID", id, "for source", schema.SourceTable)
	return &BlobOfflineTable{schema}, nil
}

type FileStorePrimaryTable struct {
	store            FileStore
	sourcePath       string
	isTransformation bool
	id               ResourceID
}

func (tbl *FileStorePrimaryTable) Write(GenericRecord) error {
	return fmt.Errorf("not implemented")
}

func (tbl *FileStorePrimaryTable) GetName() string {
	return tbl.sourcePath
}

func (tbl *FileStorePrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	iterator, err := tbl.store.Serve(tbl.sourcePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create iterator from source table: %v", err)
	}
	return &FileStoreIterator{iter: iterator, curIdx: 0, maxIdx: n}, nil
}

func (tbl *FileStorePrimaryTable) NumRows() (int64, error) {
	return tbl.store.NumRows(tbl.sourcePath)
}

type FileStoreIterator struct {
	iter    Iterator
	err     error
	curIdx  int64
	maxIdx  int64
	records []interface{}
	columns []string
}

func (it *FileStoreIterator) Next() bool {
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

func (it *FileStoreIterator) Columns() []string {
	return it.columns
}

func (it *FileStoreIterator) Err() error {
	return it.err
}

func (it *FileStoreIterator) Values() GenericRecord {
	return GenericRecord(it.records)
}

func (it *FileStoreIterator) Close() error {
	return nil
}

func (k8s *K8sOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, sourceName string) (PrimaryTable, error) {
	return blobRegisterPrimary(id, sourceName, k8s.logger, k8s.store)
}

func blobRegisterPrimary(id ResourceID, sourceName string, logger *zap.SugaredLogger, store FileStore) (PrimaryTable, error) {
	resourceKey := store.PathWithPrefix(fileStoreResourcePath(id))
	primaryExists, err := store.Exists(resourceKey)
	if err != nil {
		logger.Errorw("Error checking if primary exists", err)
		return nil, fmt.Errorf("error checking if primary exists: %v", err)
	}
	if primaryExists {
		logger.Errorw("Error checking if primary exists")
		return nil, fmt.Errorf("primary already exists")
	}

	logger.Debugw("Registering primary table", id, "for source", sourceName)
	if err := store.Write(resourceKey, []byte(sourceName)); err != nil {
		logger.Errorw("Could not write primary table", err)
		return nil, err
	}
	logger.Debugw("Succesfully registered primary table", id, "for source", sourceName)
	return &FileStorePrimaryTable{store, sourceName, false, id}, nil
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
	azureStore, ok := k8s.store.(AzureFileStore)
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
	azureStore, ok := k8s.store.(AzureFileStore)
	if ok {
		envVars = azureStore.addAzureVars(envVars)
	}
	return envVars
}

func addResourceID(envVars map[string]string, id ResourceID) map[string]string {
	envVars["RESOURCE_NAME"] = id.Name
	envVars["RESOURCE_VARIANT"] = id.Name
	envVars["RESOURCE_TYPE"] = fmt.Sprintf("%d", id.Type)
	return envVars
}

func (k8s *K8sOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool) error {
	updatedQuery, sources, err := k8s.updateQuery(config.Query, config.SourceMapping)
	if err != nil {
		k8s.logger.Errorw("Could not generate updated query for k8s transformation", err)
		return err
	}

	transformationDestination := k8s.store.PathWithPrefix(fileStoreResourcePath(config.TargetTableID))
	transformationDestinationExactPath, err := k8s.store.NewestFile(transformationDestination)
	if err != nil {
		k8s.logger.Errorw("Could not get newest blob", "location", transformationDestination, "error", err)
		return fmt.Errorf("could not get newest blob: %s: %v", transformationDestination, err)
	}
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

	runnerArgs = addResourceID(runnerArgs, config.TargetTableID)
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
	transformationDestination := k8s.store.PathWithPrefix(fileStoreResourcePath(config.TargetTableID))
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

	transformationFilePath := k8s.store.PathWithPrefix(fileStoreResourcePath(config.TargetTableID))
	fileName := "transformation.pkl"
	transformationFileLocation := fmt.Sprintf("%s%s", transformationFilePath, fileName)
	err = k8s.store.Write(transformationFileLocation, config.Code)
	if err != nil {
		return fmt.Errorf("could not upload file: %s", err)
	}

	k8sArgs := k8s.getDFArgs(transformationDestination, transformationFileLocation, config.SourceMapping, sources)
	k8sArgs = addResourceID(k8sArgs, config.TargetTableID)
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
			k8s.logger.Errorw("Error getting source path of source", "source", m.Source, "error", err)
			return "", nil, fmt.Errorf("could not get the sourcePath for %s because %s", m.Source, err)
		}

		sources[i] = sourcePath
	}

	replacer := strings.NewReplacer(replacements...)
	updatedQuery := replacer.Replace(query)

	if strings.Contains(updatedQuery, "{{") {
		k8s.logger.Errorw("Template replace failed", "query", updatedQuery)
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
			k8s.logger.Errorw("Issue getting primary table", "id", fileResourceId, "error", err)
			return "", fmt.Errorf("could not get the primary table for {%v} because %s", fileResourceId, err)
		}
		filePath = fileTable.GetName()
		return filePath, nil
	} else if fileType == "transformation" {
		fileResourceId := ResourceID{Name: fileName, Variant: fileVariant, Type: Transformation}
		fileResourcePath := fileStoreResourcePath(fileResourceId)
		exactFileResourcePath, err := k8s.store.NewestFile(fileResourcePath)
		if err != nil {
			k8s.logger.Errorw("Could not get newest blob", "location", fileResourcePath, "error", err)
			return "", fmt.Errorf("could not get newest blob: %s: %v", fileResourcePath, err)
		}
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
	transformationPath := k8s.store.PathWithPrefix(fileStoreResourcePath(id))
	transformationExactPath, err := k8s.store.NewestFile(transformationPath)
	if err != nil {
		k8s.logger.Errorw("Could not get transformation table", "error", err)
		return nil, fmt.Errorf("could not get transformation table (%v): %v", id, err)
	}
	k8s.logger.Debugw("Succesfully retrieved transformation table", "ResourceID", id)
	return &FileStorePrimaryTable{k8s.store, transformationExactPath, true, id}, nil
}

func (k8s *K8sOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return k8s.transformation(config, true)
}
func (k8s *K8sOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (k8s *K8sOfflineStore) GetPrimaryTable(id ResourceID) (PrimaryTable, error) {
	return fileStoreGetPrimary(id, k8s.store, k8s.logger)
}

func fileStoreGetPrimary(id ResourceID, store FileStore, logger *zap.SugaredLogger) (PrimaryTable, error) {
	resourceKey := store.PathWithPrefix(fileStoreResourcePath(id))
	logger.Debugw("Getting primary table", id)
	table, err := store.Read(resourceKey)
	if err != nil {
		return nil, fmt.Errorf("error fetching primary table: %v", err)
	}
	logger.Debugw("Succesfully retrieved primary table", id)
	return &FileStorePrimaryTable{store, string(table), false, id}, nil
}

func (k8s *K8sOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (k8s *K8sOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return fileStoreGetResourceTable(id, k8s.store, k8s.logger)
}

func fileStoreGetResourceTable(id ResourceID, store FileStore, logger *zap.SugaredLogger) (OfflineTable, error) {
	resourcekey := store.PathWithPrefix(fileStoreResourcePath(id))
	logger.Debugw("Getting resource table", id)
	serializedSchema, err := store.Read(resourcekey)
	if err != nil {
		return nil, fmt.Errorf("Error reading schema bytes from blob storage: %v", err)
	}
	resourceSchema := ResourceSchema{}
	if err := resourceSchema.Deserialize(serializedSchema); err != nil {
		return nil, fmt.Errorf("Error deserializing resource table: %v", err)
	}
	logger.Debugw("Succesfully fetched resource table", "id", id)
	return &BlobOfflineTable{resourceSchema}, nil
}

func (k8s *K8sOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	return k8s.materialization(id, false)
}

func (k8s *K8sOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return fileStoreGetMaterialization(id, k8s.store, k8s.logger)
}

func fileStoreGetMaterialization(id MaterializationID, store FileStore, logger *zap.SugaredLogger) (Materialization, error) {
	s := strings.Split(string(id), "/")
	if len(s) != 3 {
		logger.Errorw("Invalid materialization id", id)
		return nil, fmt.Errorf("invalid materialization id")
	}
	materializationID := ResourceID{s[1], s[2], FeatureMaterialization}
	logger.Debugw("Getting materialization", "id", id)
	materializationPath := store.PathWithPrefix(fileStoreResourcePath(materializationID))
	materializationExactPath, err := store.NewestFile(materializationPath)
	if err != nil {
		logger.Errorw("Could not fetch materialization resource key", "error", err)
		return nil, fmt.Errorf("Could not fetch materialization resource key: %v", err)
	}
	logger.Debugw("Succesfully retrieved materialization", "id", id)
	return &FileStoreMaterialization{materializationID, store, materializationExactPath}, nil
}

type FileStoreMaterialization struct {
	id    ResourceID
	store FileStore
	key   string
}

func (mat FileStoreMaterialization) ID() MaterializationID {
	return MaterializationID(fmt.Sprintf("%s/%s/%s", FeatureMaterialization, mat.id.Name, mat.id.Variant))
}

func (mat FileStoreMaterialization) NumRows() (int64, error) {
	materializationPath := mat.store.PathWithPrefix(fileStoreResourcePath(mat.id))
	latestMaterializationPath, err := mat.store.NewestFile(materializationPath)
	if err != nil {
		return 0, fmt.Errorf("Could not get materialization num rows; %v", err)
	}
	return mat.store.NumRows(latestMaterializationPath)
}

func (mat FileStoreMaterialization) IterateSegment(begin, end int64) (FeatureIterator, error) {
	materializationPath := mat.store.PathWithPrefix(fileStoreResourcePath(mat.id))
	latestMaterializationPath, err := mat.store.NewestFile(materializationPath)
	if err != nil {
		return nil, fmt.Errorf("Could not get materialization iterate segment: %v", err)
	}
	iter, err := mat.store.Serve(latestMaterializationPath)
	if err != nil {
		return nil, err
	}
	for i := int64(0); i < begin; i++ {
		_, _ = iter.Next()
	}
	return &FileStoreFeatureIterator{
		iter:   iter,
		curIdx: 0,
		maxIdx: end,
	}, nil
}

type FileStoreFeatureIterator struct {
	iter   Iterator
	err    error
	cur    ResourceRecord
	curIdx int64
	maxIdx int64
}

func (iter *FileStoreFeatureIterator) Next() bool {
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
	timeString, ok := nextVal["ts"].(string)
	if !ok {
		iter.cur = ResourceRecord{Entity: string(nextVal["entity"].(string)), Value: nextVal["value"]}
	} else {
		timestamp, err1 := time.Parse(formatDate, timeString)
		formatDateWithoutUTC := "2006-01-02 15:04:05"
		timestamp2, err2 := time.Parse(formatDateWithoutUTC, timeString)
		if err1 != nil && err2 != nil {
			iter.err = fmt.Errorf("could not parse timestamp: %v: %v, %v", nextVal["ts"], err1, err2)
			return false
		}
		if err1 != nil {
			timestamp = timestamp2
		}
		iter.cur = ResourceRecord{Entity: string(nextVal["entity"].(string)), Value: nextVal["value"], TS: timestamp}
	}

	return true
}

func (iter *FileStoreFeatureIterator) Value() ResourceRecord {
	return iter.cur
}

func (iter *FileStoreFeatureIterator) Err() error {
	return iter.err
}

func (iter *FileStoreFeatureIterator) Close() error {
	return nil
}

func (k8s *K8sOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	return k8s.materialization(id, true)
}

func (k8s *K8sOfflineStore) materialization(id ResourceID, isUpdate bool) (Materialization, error) {
	if id.Type != Feature {
		k8s.logger.Errorw("Attempted to create a materialization of a non feature resource", "type", id.Type)
		return nil, fmt.Errorf("only features can be materialized")
	}
	resourceTable, err := k8s.GetResourceTable(id)
	if err != nil {
		k8s.logger.Errorw("Attempted to fetch resource table of non registered resource", "error", err)
		return nil, fmt.Errorf("resource not registered: %v", err)
	}
	k8sResourceTable, ok := resourceTable.(*BlobOfflineTable)
	if !ok {
		k8s.logger.Errorw("Could not convert resource table to blob offline table", id)
		return nil, fmt.Errorf("could not convert offline table with id %v to k8sResourceTable", id)
	}
	materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	destinationPath := k8s.store.PathWithPrefix(fileStoreResourcePath(materializationID))
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

	k8sArgs = addResourceID(k8sArgs, id)
	k8s.logger.Debugw("Creating materialization", "id", id)
	if err := k8s.executor.ExecuteScript(k8sArgs); err != nil {
		k8s.logger.Errorw("Job failed to run", err)
		return nil, fmt.Errorf("job for materialization %v failed to run: %v", materializationID, err)
	}
	matPath := k8s.store.PathWithPrefix(fileStoreResourcePath(materializationID))
	latestMatPath, err := k8s.store.NewestFile(matPath)
	if err != nil {
		return nil, fmt.Errorf("Materialization does not exist; %v", err)
	}
	k8s.logger.Debugw("Succesfully created materialization", "id", id)
	return &FileStoreMaterialization{materializationID, k8s.store, latestMatPath}, nil
}

func (k8s *K8sOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return fileStoreDeleteMaterialization(id, k8s.store, k8s.logger)
}

func fileStoreDeleteMaterialization(id MaterializationID, store FileStore, logger *zap.SugaredLogger) error {
	s := strings.Split(string(id), "/")
	if len(s) != 3 {
		logger.Errorw("Invalid materialization id", id)
		return fmt.Errorf("invalid materialization id")
	}
	materializationID := ResourceID{s[1], s[2], FeatureMaterialization}
	materializationPath := store.PathWithPrefix(fileStoreResourcePath(materializationID))
	materializationExactPath, err := store.NewestFile(materializationPath)
	if err != nil {
		return fmt.Errorf("materialization does not exist: %v", err)
	}
	return store.Delete(materializationExactPath)
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
		k8s.logger.Errorw("Resource not registered in blob store", "id", id, "error", err)
		return ResourceSchema{}, fmt.Errorf("resource not registered: %v", err)
	}
	blobResourceTable, ok := table.(*BlobOfflineTable)
	if !ok {
		k8s.logger.Errorw("could not convert offline table to blobResourceTable", "id", id)
		return ResourceSchema{}, fmt.Errorf("could not convert offline table with id %v to blobResourceTable", id)
	}
	k8s.logger.Debugw("Succesfully retrieved resource schema", "id", id)
	return blobResourceTable.schema, nil
}

func (s *S3FileStore) BlobPath(sourceKey string) string {
	// if !strings.Contains(sourceKey, "s3://") {
	// 	return fmt.Sprintf("%s%s", BucketPrefix(), sourceKey)
	// }
	return sourceKey
}

func (k8s *K8sOfflineStore) trainingSet(def TrainingSetDef, isUpdate bool) error {
	if err := def.check(); err != nil {
		k8s.logger.Errorw("Training set definition not valid", def, err)
		return err
	}
	sourcePaths := make([]string, 0)
	featureSchemas := make([]ResourceSchema, 0)
	destinationPath := k8s.store.PathWithPrefix(fileStoreResourcePath(def.ID))
	trainingSetExactPath, err := k8s.store.NewestFile(destinationPath)
	if err != nil {
		return fmt.Errorf("could not get training set path: %v", err)
	}
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
		k8s.logger.Errorw("Could not get schema of label in store", "id", def.Label, "error", err)
		return fmt.Errorf("Could not get schema of label %s: %v", def.Label, err)
	}
	labelPath := labelSchema.SourceTable
	sourcePaths = append(sourcePaths, labelPath)
	for _, feature := range def.Features {
		featureSchema, err := k8s.registeredResourceSchema(feature)
		if err != nil {
			k8s.logger.Errorw("Could not get schema of feature in store", "feature", feature, "error", err)
			return fmt.Errorf("Could not get schema of feature %s: %v", feature, err)
		}
		featurePath := featureSchema.SourceTable
		sourcePaths = append(sourcePaths, featurePath)
		featureSchemas = append(featureSchemas, featureSchema)
	}
	trainingSetQuery := k8s.query.trainingSetCreate(def, featureSchemas, labelSchema)
	k8s.logger.Debugw("Training set query", "query", sourcePaths)
	k8s.logger.Debugw("Source list", "list", trainingSetQuery)
	pandasArgs := k8s.pandasRunnerArgs(k8s.store.PathWithPrefix(destinationPath), trainingSetQuery, sourcePaths, CreateTrainingSet)
	pandasArgs = addResourceID(pandasArgs, def.ID)
	k8s.logger.Debugw("Creating training set", "definition", def)
	if err := k8s.executor.ExecuteScript(pandasArgs); err != nil { //
		k8s.logger.Errorw("training set job failed to run", "definition", def.ID, "error", err)
		return fmt.Errorf("job for training set %v failed to run: %v", def.ID, err)
	}
	k8s.logger.Debugw("Succesfully created training set:", "definition", def)
	return nil
}

func (k8s *K8sOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return fileStoreGetTrainingSet(id, k8s.store, k8s.logger)
}

func fileStoreGetTrainingSet(id ResourceID, store FileStore, logger *zap.SugaredLogger) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		logger.Errorw("id is not of type training set", err)
		return nil, err
	}
	resourceKeyPrefix := store.PathWithPrefix(fileStoreResourcePath(id))
	trainingSetExactPath, err := store.NewestFile(resourceKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("Could not get training set: %v", err)
	}
	iterator, err := store.Serve(trainingSetExactPath)
	if err != nil {
		return nil, err
	}
	return &FileStoreTrainingSet{id: id, store: store, key: trainingSetExactPath, iter: iterator}, nil
}

type FileStoreTrainingSet struct {
	id       ResourceID
	store    FileStore
	key      string
	iter     Iterator
	Error    error
	features []interface{}
	label    interface{}
}

func (ts *FileStoreTrainingSet) Next() bool {
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

func (ts *FileStoreTrainingSet) Features() []interface{} {
	return ts.features
}

func (ts *FileStoreTrainingSet) Label() interface{} {
	return ts.label
}

func (ts *FileStoreTrainingSet) Err() error {
	return ts.Error
}
