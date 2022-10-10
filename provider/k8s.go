package provider

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
	// "io/ioutil"
	"os/exec"
	"strings"
	"reflect"
	"bytes"

	"github.com/featureform/kubernetes"
	

	"go.uber.org/zap"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/fileblob"
	azureblob "gocloud.dev/blob/azureblob"
	parquet "github.com/segmentio/parquet-go"
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
		Memory: NewMemoryBlobStore,
		FileSystem: NewFileBlobStore,
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
)

const (
	Memory BlobStoreType = "MEMORY"
	FileSystem           = "FILE_SYSTEM"
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
	Writer(key string) (*blob.Writer, error)
	Read(key string) ([]byte, error)
	Serve(key string) (Iterator, error)
	Exists(key string) (bool, error)
	Delete(key string) error
	NewestBlob(prefix string) string
	ResourcePath(id ResourceID) string
	PathWithPrefix(path string) string
	Close() error
}

type Iterator interface {
	Next() (interface{}, error)
}

type MemoryBlobStore struct {
	genericBlobStore
}

type AzureBlobStore struct {
	genericBlobStore
}

type genericBlobStore struct {
	bucket *blob.Bucket
	path string
}

func (store genericBlobStore) PathWithPrefix(path string) string {
	return fmt.Sprintf("%s%s", store.path, path)
}

func (store genericBlobStore) ResourcePath(id ResourceID) string {
	return ""
}

func (store genericBlobStore) NewestBlob(prefix string) string {
	opts := blob.ListOptions{
		Prefix: prefix,
		Delimiter: "/",
	}
	listIterator := store.bucket.List(&opts)
	mostRecentTime := time.UnixMilli(0)
	mostRecentKey := ""
	for listObj, err := listIterator.Next(ctx); err != nil; listObj, err = listIterator.Next(ctx){
		if !listObj.IsDir && (listObj.ModTime.After(mostRecentTime) || listObj.ModTime.Equal(mostRecentTime)) {
			mostRecentTime = listObj.ModTime
			mostRecentKey = listObj.Key
		}	
	}
	return mostRecentKey
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
	case "csv":
		return csvIteratorFromReader(r)
	case "parquet":
		return parquetIteratorFromReader(r)
	default:
		return nil, fmt.Errorf("unsupported file type")
	}

}

type ParquetIterator struct {
	rows []interface{}
	index int64
}

func (p *ParquetIterator) Next() (interface{}, error) {
	if p.index+1 == int64(len(p.rows)) {
		return nil, fmt.Errorf("end of iterator")
	}
	p.index += 1
	return p.rows[p.index], nil
}

func parquetIteratorFromReader(r io.Reader) (Iterator, error) {
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
		rows: rows,
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

type csvIterator struct {
	reader *csv.Reader
}

func (iter csvIterator) Next() (interface{}, error) {
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
			path: fileStoreConfig.DirPath,
		},
	}, nil
}

type AzureBlobStoreConfig struct {
	AccountName string
	AccountKey string
	BucketName string
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
	bucket, err := azureblob.OpenBucket(ctx, client, azureStoreConfig.BucketName, nil)
	if err != nil {
		return AzureBlobStore{}, fmt.Errorf("Could not open azure bucket: %v", err)
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
	return k8s.transformation(config, false)
}

func (k8s *K8sOfflineStore) transformation(config TransformationConfig, isUpdate bool) error {
	if config.Type == SQLTransformation {
		return k8s.sqlTransformation(config, isUpdate)
	} else if config.Type == DFTransformation {
		return k8s.dfTransformation(config, isUpdate)
	} else {
		k8s.logger.Errorw("Unsupported transformation type", config.Type)
		return fmt.Errorf("the transformation type '%v' is not supported", config.Type)
	}
}

func (k8s *K8sOfflineStore) pandasRunnerArgs(transformationDestination string, updatedQuery string, sources []string, jobType JobType) map[string]string {
	return map[string]string{}
}

func (k8s K8sOfflineStore) getDFArgs(toutputURI string, code string, awsRegion string, mapping []SourceMapping) (map[string]string, error ){
	return map[string]string{}, nil
}

func (k8s *K8sOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool) error {
	updatedQuery, sources, err := k8s.updateQuery(config.Query, config.SourceMapping)
	if err != nil {
		k8s.logger.Errorw("Could not generate updated query for k8s transformation", err)
		return err
	}

	transformationDestination := k8s.store.ResourcePath(config.TargetTableID)
	exists, err := k8s.store.Exists(transformationDestination)
	if err != nil {
		k8s.logger.Errorw("Error checking existence of resource", config.TargetTableID, err)
		return err
	}

	if !isUpdate && exists {
		k8s.logger.Errorw("Creation when transformation already exists", config.TargetTableID, transformationDestination)
		return fmt.Errorf("transformation %v already exists at %s", config.TargetTableID, transformationDestination)
	} else if isUpdate && !exists {
		k8s.logger.Errorw("Update job attempted when transformation does not exist", config.TargetTableID, transformationDestination)
		return fmt.Errorf("transformation %v doesn't exist at %s and you are trying to update", config.TargetTableID, transformationDestination)
	}
	k8s.logger.Debugw("Running SQL transformation", config)
	runnerArgs := k8s.pandasRunnerArgs(transformationDestination, updatedQuery, sources, Transform)
	if err := k8s.executor.ExecuteScript(runnerArgs); err != nil {
		k8s.logger.Errorw("job for transformation failed to run", config.TargetTableID, err)
		return fmt.Errorf("job for transformation %v failed to run: %v", config.TargetTableID, err)
	}
	k8s.logger.Debugw("Succesfully ran SQL transformation", config)
	return nil
}

func (k8s *K8sOfflineStore) dfTransformation(config TransformationConfig, isUpdate bool) error {
	transformationDestination := blobResourcePath(config.TargetTableID)
	// latestKey//bm22
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

	transformationFilePath := k8s.store.ResourcePath(config.TargetTableID) //*
	fileName := "transformation.pkl"
	transformationFileLocation := fmt.Sprintf("%s/%s", transformationFilePath, fileName)

	// f, err := os.Create(fileName)
	// if err != nil {
	// 	return fmt.Errorf("could not create file: %s", err)
	// }
	// defer f.Close()

	// err = ioutil.WriteFile(fileName, config.Code, 0644)
	// if err != nil {
	// 	return fmt.Errorf("could not write to file: %s", err)
	// }

	// // Write byte to file
	// f, err = os.Open(fileName)
	// if err != nil {
	// 	return fmt.Errorf("could not open file: %s", err)
	// }
	err = k8s.store.Write(transformationFileLocation, config.Code)
	if err != nil {
		return fmt.Errorf("could not upload file: %s", err)
	}

	k8sArgs, err := k8s.getDFArgs(transformationDestination, transformationFileLocation, "", config.SourceMapping)
	if err != nil {
		k8s.logger.Errorw("Problem creating spark dataframe arguments", err)
		return fmt.Errorf("error with getting df arguments %v", k8sArgs)
	}
	k8s.logger.Debugw("Running DF transformation", config)
	if err := k8s.executor.ExecuteScript(k8sArgs); err != nil {
		k8s.logger.Errorw("Error running Spark dataframe job", err)
		return fmt.Errorf("spark submit job for transformation %v failed to run: %v", config.TargetTableID, err)
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
		// sourcePath, err := k8s.getSourcePath(m.Source)
		// if err != nil {
		// 	k8s.logger.Errorw("Error getting source path of source", m.Source, err)
		// 	return "", nil, fmt.Errorf("could not get the sourcePath for %s because %s", m.Source, err)
		// }

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



func (k8s *K8sOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	k8s.logger.Debugw("Getting transformation table", "ResourceID", id)
	//bm22
	transformationPath := k8s.store.ResourcePath(id)
	// if err != nil {
	// 	k8s.logger.Errorw("Could not get transformation table", "error", err)
	// 	return nil, fmt.Errorf("could not get transformation table (%v) because %s", id, err)
	// }
	k8s.logger.Debugw("Succesfully retrieved transformation table", "ResourceID", id)
	return &BlobPrimaryTable{k8s.store, transformationPath, true, id}, nil
}

func (k8s *K8sOfflineStore) UpdateTransformation(config TransformationConfig) error {
	return k8s.transformation(config, true)
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
	return &BlobOfflineTable{resourceSchema}, nil
}

func (k8s *K8sOfflineStore) CreateMaterialization(id ResourceID) (Materialization, error) {
	// if id.Type != Feature {
	// 	k8s.logger.Errorw("Attempted to create a materialization of a non feature resource", id.Type)
	// 	return nil, fmt.Errorf("only features can be materialized")
	// }
	// resourceTable, err := k8s.GetResourceTable(id)
	// if err != nil {
	// 	k8s.logger.Errorw("Attempted to fetch resource table of non registered resource", err)
	// 	return nil, fmt.Errorf("resource not registered: %v", err)
	// }
	// k8sResourceTable, ok := resourceTable.(*BlobOfflineTable)
	// if !ok {
	// 	k8s.logger.Errorw("Could not convert resource table to blob offline table", id)
	// 	return nil, fmt.Errorf("could not convert offline table with id %v to k8sResourceTable", id)
	// }
	// materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	// destinationPath := k8s.store.ResourcePath(materializationID)
	// materializationExists, err := k8s.store.Exists(destinationPath)
	// if err != nil {
	// 	k8s.logger.Errorw("Could not determine whether materialization exists", err)
	// 	return nil, fmt.Errorf("error checking if materialization exists: %v", err)
	// }
	// if materializationExists {
	// 	k8s.logger.Errorw("Attempted to materialize a materialization that already exists", id)
	// 	return nil, fmt.Errorf("materialization already exists")
	// }
	// materializationQuery := k8s.query.materializationCreate(k8sResourceTable.schema)
	// sourcePath := k8s.store.KeyPath(k8sResourceTable.schema.SourceTable)
	// k8sArgs := k8s.store.PandasRunnerArgs(destinationPath, materializationQuery, []string{sourcePath}, Materialize)
	// k8s.logger.Debugw("Creating materialization", "id", id)
	// if err := k8s.executor.ExecuteScript(k8sArgs); err != nil {
	// 	k8s.logger.Errorw("Spark submit job failed to run", err)
	// 	return nil, fmt.Errorf("job for materialization %v failed to run: %v", materializationID, err)
	// }
	// key, err := k8s.store.ResourceKey(materializationID)
	// if err != nil {
	// 	k8s.logger.Errorw("Created materialization not found in store", err)
	// 	return nil, fmt.Errorf("Materialization result does not exist in offline store: %v", err)
	// }
	// k8s.logger.Debugw("Succesfully created materialization", "id", id)
	// return &BlobMaterialization{materializationID, k8s.store, key}, nil
	return nil, nil
}

func (k8s *K8sOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	// s := strings.Split(string(id), "/")
	// if len(s) != 3 {
	// 	k8s.logger.Errorw("Invalid materialization id", id)
	// 	return nil, fmt.Errorf("invalid materialization id")
	// }
	// materializationID := ResourceID{s[1], s[2], FeatureMaterialization}
	// k8s.logger.Debugw("Getting materialization", "id", id)
	// key, err := k8s.store.ResourceKey(materializationID)
	// if err != nil {
	// 	k8s.logger.Errorw("Could not fetch materialization resource key", err)
	// 	return nil, err
	// }
	// k8s.logger.Debugw("Succesfully retrieved materialization", "id", id)
	// return &BlobMaterialization{materializationID, k8s.store, key}, nil
	return nil, nil
}

func (k8s *K8sOfflineStore) UpdateMaterialization(id ResourceID) (Materialization, error) {
	// return k8s.materialization(id, true)
	return nil, nil
}

func (k8s *K8sOfflineStore) DeleteMaterialization(id MaterializationID) error {
	// materializationKey, err := k8s.materializationKey(id)
	// if err != nil {
	// 	return err
	// }
	// exists, err := k8s.store.Exists(materializationKey)
	// if err != nil {
	// 	return err
	// }
	// if !exists {
	// 	return fmt.Errorf("materialization does not exist")
	// }
	// return store.Delete(materializationKey)
	return nil
}

func (k8s *K8sOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	return k8s.trainingSet(def, false)
}

func (k8s *K8sOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return k8s.trainingSet(def, false)
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
	destinationPath := blobResourcePath(def.ID)
	trainingSetExactPath := k8s.store.NewestBlob(destinationPath)
	trainingSetExists := trainingSetExactPath == ""
	if !isUpdate {
		if trainingSetExists {
			k8s.logger.Errorw("Training set already exists", def.ID)
			return fmt.Errorf("training set already exists: %v", def.ID)
		}
	} else {
		if !trainingSetExists {
			k8s.logger.Errorw("Training set doesn't exist for update job", def.ID)
			return fmt.Errorf("Training set doesn't exist for update job: %v", def.ID)
		}
	}
	labelSchema, err := k8s.registeredResourceSchema(def.Label)
	if err != nil {
		k8s.logger.Errorw("Could not get schema of label in spark store", def.Label, err)
		return fmt.Errorf("Could not get schema of label %s: %v", def.Label, err)
	}
	labelPath := k8s.store.PathWithPrefix(labelSchema.SourceTable)
	sourcePaths = append(sourcePaths, labelPath)
	for _, feature := range def.Features {
		featureSchema, err := k8s.registeredResourceSchema(feature)
		if err != nil {
			k8s.logger.Errorw("Could not get schema of feature in spark store", feature, err)
			return fmt.Errorf("Could not get schema of feature %s: %v", feature, err)
		}
		featurePath := k8s.store.PathWithPrefix(featureSchema.SourceTable)
		sourcePaths = append(sourcePaths, featurePath)
		featureSchemas = append(featureSchemas, featureSchema)
	}
	trainingSetQuery := k8s.query.trainingSetCreate(def, featureSchemas, labelSchema)
	pandasArgs := k8s.pandasRunnerArgs(destinationPath, trainingSetQuery, sourcePaths, CreateTrainingSet)
	k8s.logger.Debugw("Creating training set", "definition", def)
	if err := k8s.executor.ExecuteScript(pandasArgs); err != nil { //
		k8s.logger.Errorw("training set job failed to run", "definition", def.ID, "error", err)
		return fmt.Errorf("job for training set %v failed to run: %v", def.ID, err)
	}
	// _, err = k8s.store.ResourceKeySinglePart(def.ID)
	// if err != nil {
	// 	k8s.logger.Errorw("Could not get training set resource key in offline store", "error", err)
	// 	return fmt.Errorf("Training Set result does not exist in offline store: %v", err)
	// }
	k8s.logger.Debugw("Succesfully created training set:", "definition", def)
	return nil
}

func (k8s *K8sOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		k8s.logger.Errorw("id is not of type training set", err)
		return nil, err
	}
	resourceKeyPrefix := blobResourcePath(id)
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
	id ResourceID
	store BlobStore
	key string
	iter Iterator
	err error
	row []interface{}
}


// type BlobStore interface {
// 	Write(key string, data []byte) error
// 	Writer(key string) (*blob.Writer, error)
// 	Read(key string) ([]byte, error)
// 	Serve(key string) (Iterator, error)
// 	Exists(key string) (bool, error)
// 	Delete(key string) error
// 	NewestBlob(prefix string) string
// 	ResourcePath(id ResourceID) string
// 	Close() error
// }

// type Iterator interface {
// 	Next() (interface{}, error)
//}
//bm222
func (ts BlobTrainingSet) Next() bool {
	row, err := ts.iter.Next()
	if err != nil {
		ts.err = err
		return false
	}
	//assume map[string]interface{}
	values := make([]interface{}, 0)
	val := reflect.ValueOf(row)
	for _, e := range val.MapKeys() {
		v := val.MapIndex(e)
		values = append(values, v)
	}
	ts.row = values
	return true
}

func (ts BlobTrainingSet) Features() []interface{} {
	return ts.row[:len(ts.row)-1]
}

func (ts BlobTrainingSet) Label() interface{} {
	return ts.row[len(ts.row)-1]
}

func (ts BlobTrainingSet) Err() error {
	return ts.err
}

// type FeatureIterator interface {
// 	Next() bool
// 	Value() ResourceRecord
// 	Err() error
// 	Close() error
// }


// initialize store using config (blob and executor)

// register primary table

// create transformation

// register feature and label

// create training set

// iterate through training set