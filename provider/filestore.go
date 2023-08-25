package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	re "github.com/avast/retry-go/v4"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv2cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	hdfs "github.com/colinmarc/hdfs/v2"
	filestore "github.com/featureform/filestore"
	pc "github.com/featureform/provider/provider_config"

	"io/fs"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
)

type FileStore interface {
	Write(key filestore.Filepath, data []byte) error
	Read(key filestore.Filepath) ([]byte, error)
	Serve(key filestore.Filepath) (Iterator, error)
	Exists(key filestore.Filepath) (bool, error)
	Delete(key filestore.Filepath) error
	DeleteAll(dir filestore.Filepath) error
	NewestFileOfType(prefix filestore.Filepath, fileType filestore.FileType) (filestore.Filepath, error)
	NumRows(key filestore.Filepath) (int64, error)
	Close() error
	Upload(sourcePath filestore.Filepath, destPath filestore.Filepath) error
	Download(sourcePath filestore.Filepath, destPath filestore.Filepath) error
	FilestoreType() filestore.FileStoreType
	AddEnvVars(envVars map[string]string) map[string]string
	// CreateFilePath creates a new filepath object with the bucket and scheme from a Key
	CreateFilePath(key string) (filestore.Filepath, error)
	CreateDirPath(key string) (filestore.Filepath, error)
}

type Iterator interface {
	Next() (map[string]interface{}, error)
	FeatureColumns() []string
	LabelColumn() string
}

type LocalFileStore struct {
	DirPath string
	genericFileStore
}

func NewLocalFileStore(config Config) (FileStore, error) {
	fileStoreConfig := pc.LocalFileStoreConfig{}
	if err := fileStoreConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("could not deserialize file store config: %v", err)
	}
	bucket, err := blob.OpenBucket(context.TODO(), fileStoreConfig.DirPath)
	if err != nil {
		return nil, err
	}
	filepath, err := filestore.NewEmptyFilepath(filestore.FileSystem)
	if err != nil {
		return nil, err
	}
	// TODO: double check that this is the appropriate method in this case
	err = filepath.ParseDirPath(fileStoreConfig.DirPath)
	if err != nil {
		return nil, err
	}
	return &LocalFileStore{
		DirPath: fileStoreConfig.DirPath[len("file:///"):],
		genericFileStore: genericFileStore{
			bucket: bucket,
			path:   filepath,
		},
	}, nil
}

func (fs *LocalFileStore) FilestoreType() filestore.FileStoreType {
	return filestore.FileSystem
}

type AzureFileStore struct {
	AccountName      string
	AccountKey       string
	ConnectionString string
	ContainerName    string
	Path             string
	genericFileStore
}

func (store *AzureFileStore) CreateFilePath(key string) (filestore.Filepath, error) {
	fp := filestore.AzureFilepath{
		StorageAccount: store.AccountName,
	}
	fp.SetScheme(filestore.AzureBlobPrefix)
	fp.SetBucket(store.ContainerName)
	if store.Path != "" {
		fp.SetKey(fmt.Sprintf("%s/%s", store.Path, key))
	} else {
		fp.SetKey(key)
	}
	fp.SetIsDir(false)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (store *AzureFileStore) CreateDirPath(key string) (filestore.Filepath, error) {
	fp := filestore.AzureFilepath{
		StorageAccount: store.AccountName,
	}
	fp.SetScheme(filestore.AzureBlobPrefix)
	fp.SetBucket(store.ContainerName)
	if store.Path != "" {
		fp.SetKey(fmt.Sprintf("%s/%s", store.Path, key))
	} else {
		fp.SetKey(key)
	}
	fp.SetIsDir(true)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (store *AzureFileStore) configString() string {
	return fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net=%s", store.AccountName, store.AccountKey)
}
func (store *AzureFileStore) connectionString() string {
	return store.ConnectionString
}
func (store *AzureFileStore) containerName() string {
	return store.ContainerName
}

func (store *AzureFileStore) AddEnvVars(envVars map[string]string) map[string]string {
	envVars["BLOB_STORE_TYPE"] = "azure"
	envVars["AZURE_CONNECTION_STRING"] = store.ConnectionString
	envVars["AZURE_CONTAINER_NAME"] = store.ContainerName
	return envVars
}

func (store *AzureFileStore) AsAzureStore() *AzureFileStore {
	return store
}

func (store *AzureFileStore) FilestoreType() filestore.FileStoreType {
	return filestore.Azure
}

func NewAzureFileStore(config Config) (FileStore, error) {
	azureStoreConfig := &pc.AzureFileStoreConfig{}
	if err := azureStoreConfig.Deserialize(pc.SerializedConfig(config)); err != nil {
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
		return nil, fmt.Errorf("could not create azure client: %v", err)
	}

	bucket, err := azureblob.OpenBucket(context.TODO(), client, azureStoreConfig.ContainerName, nil)
	if err != nil {
		return nil, fmt.Errorf("could not open azure bucket: %v", err)
	}
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", azureStoreConfig.AccountName, azureStoreConfig.AccountKey)
	return &AzureFileStore{
		AccountName:      azureStoreConfig.AccountName,
		AccountKey:       azureStoreConfig.AccountKey,
		ConnectionString: connectionString,
		ContainerName:    azureStoreConfig.ContainerName,
		Path:             azureStoreConfig.Path,
		genericFileStore: genericFileStore{
			bucket:    bucket,
			storeType: filestore.Azure,
		},
	}, nil
}

type S3FileStore struct {
	Credentials  pc.AWSCredentials
	BucketRegion string
	Bucket       string
	Path         string
	genericFileStore
}

func (s *S3FileStore) BlobPath(sourceKey string) string {
	return sourceKey
}

func NewS3FileStore(config Config) (FileStore, error) {
	s3StoreConfig := pc.S3FileStoreConfig{}
	if err := s3StoreConfig.Deserialize(pc.SerializedConfig(config)); err != nil {
		return nil, fmt.Errorf("could not deserialize s3 store config: %v", err)
	}
	cfg, err := awsv2cfg.LoadDefaultConfig(context.TODO(),
		awsv2cfg.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: s3StoreConfig.Credentials.AWSAccessKeyId, SecretAccessKey: s3StoreConfig.Credentials.AWSSecretKey,
			},
		}))
	if err != nil {
		return nil, fmt.Errorf("could not load aws config: %v", err)
	}
	cfg.Region = s3StoreConfig.BucketRegion
	clientV2 := s3v2.NewFromConfig(cfg)
	bucket, err := s3blob.OpenBucketV2(context.TODO(), clientV2, s3StoreConfig.BucketPath, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create connection to s3 bucket: config: %v, name: %s, %v", s3StoreConfig, s3StoreConfig.BucketPath, err)
	}
	return &S3FileStore{
		Bucket:       s3StoreConfig.BucketPath,
		BucketRegion: s3StoreConfig.BucketRegion,
		Credentials:  s3StoreConfig.Credentials,
		Path:         s3StoreConfig.Path,
		genericFileStore: genericFileStore{
			bucket: bucket,
		},
	}, nil
}

func (s3 *S3FileStore) CreateFilePath(key string) (filestore.Filepath, error) {
	fp := filestore.S3Filepath{}
	fp.SetBucket(s3.Bucket)
	fp.SetKey(key)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (s3 *S3FileStore) CreateDirPath(key string) (filestore.Filepath, error) {
	fp := filestore.S3Filepath{}
	fp.SetBucket(s3.Bucket)
	fp.SetKey(key)
	fp.SetIsDir(true)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (s3 *S3FileStore) FilestoreType() filestore.FileStoreType {
	return filestore.S3
}

func (s3 *S3FileStore) AddEnvVars(envVars map[string]string) map[string]string {
	envVars["BLOB_STORE_TYPE"] = "s3"
	envVars["AWS_ACCESS_KEY_ID"] = s3.Credentials.AWSAccessKeyId
	envVars["AWS_SECRET_KEY"] = s3.Credentials.AWSSecretKey
	envVars["S3_BUCKET_REGION"] = s3.BucketRegion
	envVars["S3_BUCKET_NAME"] = s3.Bucket
	return envVars
}

func (s3 *S3FileStore) Read(path filestore.Filepath) ([]byte, error) {
	data, err := s3.bucket.ReadAll(context.TODO(), path.Key())
	if err != nil {
		return nil, err
	}
	return data, nil
}

type GCSFileStore struct {
	Bucket      string
	Path        string
	Credentials pc.GCPCredentials
	genericFileStore
}

func (gs *GCSFileStore) CreateFilePath(key string) (filestore.Filepath, error) {
	fp := filestore.GCSFilepath{}
	fp.SetBucket(gs.Bucket)
	fp.SetKey(key)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (gs *GCSFileStore) CreateDirPath(key string) (filestore.Filepath, error) {
	fp := filestore.GCSFilepath{}
	fp.SetBucket(gs.Bucket)
	fp.SetKey(key)
	fp.SetIsDir(true)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (g *GCSFileStore) FilestoreType() filestore.FileStoreType {
	return filestore.GCS
}

func (g *GCSFileStore) AddEnvVars(envVars map[string]string) map[string]string {
	// TODO: add environment variables for GCS
	panic("GCS Filestore is not supported for K8s at the moment.")
}

type GCSFileStoreConfig struct {
	BucketName  string
	BucketPath  string
	Credentials pc.GCPCredentials
}

func (s *GCSFileStoreConfig) Deserialize(config pc.SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *GCSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("could not serialize GCS config: %v", err)
	}
	return conf, nil
}

func (config *GCSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func NewGCSFileStore(config Config) (FileStore, error) {
	GCSConfig := pc.GCSFileStoreConfig{}

	err := GCSConfig.Deserialize(pc.SerializedConfig(config))
	if err != nil {
		return nil, fmt.Errorf("could not deserialize config: %v", err)
	}

	serializedFile, err := json.Marshal(GCSConfig.Credentials.JSON)
	if err != nil {
		return nil, fmt.Errorf("could not serialize GCS config: %v", err)
	}

	creds, err := google.CredentialsFromJSON(context.TODO(), serializedFile, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("could not get credentials from JSON: %v", err)
	}

	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, fmt.Errorf("could not create client: %v", err)
	}

	bucket, err := gcsblob.OpenBucket(context.TODO(), client, GCSConfig.BucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("could not open bucket: %v", err)
	}
	return &GCSFileStore{
		Bucket:      GCSConfig.BucketName,
		Path:        GCSConfig.BucketPath,
		Credentials: GCSConfig.Credentials,
		genericFileStore: genericFileStore{
			bucket: bucket,
		},
	}, nil
}

func NewHDFSFileStore(config Config) (FileStore, error) {
	HDFSConfig := pc.HDFSFileStoreConfig{}

	err := HDFSConfig.Deserialize(pc.SerializedConfig(config))
	if err != nil {
		return nil, fmt.Errorf("could not deserialize config: %v", err)
	}

	address := fmt.Sprintf("%s:%s", HDFSConfig.Host, HDFSConfig.Port)
	var username string
	if HDFSConfig.Username == "" {
		username = "hduser"
	} else {
		username = HDFSConfig.Username
	}

	ops := hdfs.ClientOptions{
		Addresses:           []string{address},
		User:                username,
		UseDatanodeHostname: true,
	}
	client, err := hdfs.NewClient(ops)
	if err != nil {
		return nil, fmt.Errorf("could not create hdfs client: %v", err)
	}

	// TODO: add updated HDFS implementation
	filepath := filestore.HDFSFilepath{}

	return &HDFSFileStore{
		Client: client,
		Path:   filepath,
		Host:   address, // authority (e.g. <host>:<port>)
	}, nil
}

type HDFSFileStore struct {
	Client *hdfs.Client
	Host   string
	Path   filestore.HDFSFilepath
}

func (fs *HDFSFileStore) alreadyExistsError(err error) bool {
	return strings.Contains(err.Error(), "file already exists")
}

func (fs *HDFSFileStore) doesNotExistsError(err error) bool {
	return strings.Contains(err.Error(), "file does not exist")
}

func (fs *HDFSFileStore) removeFile(path filestore.Filepath) (*hdfs.FileWriter, error) {
	if err := fs.Client.Remove(path.Key()); err != nil && fs.doesNotExistsError(err) {
		return fs.getFile(path)
	} else if err != nil {
		return nil, fmt.Errorf("could not remove file %s: %v", path.Key(), err)
	}
	return fs.getFile(path)
}

func (fs *HDFSFileStore) getFile(path filestore.Filepath) (*hdfs.FileWriter, error) {
	if w, err := fs.Client.Create(path.Key()); err != nil && fs.alreadyExistsError(err) {
		return fs.removeFile(path)
	} else if err != nil {
		return nil, fmt.Errorf("could not get file: %v", err)
	} else {
		return w, nil
	}
}

func (fs *HDFSFileStore) isFile(key string) bool {
	parsedPath := strings.Split(key, "/")
	return len(parsedPath) == 1
}

func (fs *HDFSFileStore) getFileWriter(path filestore.Filepath) (*hdfs.FileWriter, error) {
	if w, err := fs.getFile(path); err != nil {
		return nil, fmt.Errorf("could get writer: %v", err)
	} else {
		return w, nil
	}
}

func (fs *HDFSFileStore) createFile(path filestore.Filepath) (*hdfs.FileWriter, error) {
	if !path.IsDir() {
		return fs.getFileWriter(path)
	}
	err := fs.Client.MkdirAll(path.KeyPrefix(), os.ModeDir)
	if err != nil {
		return nil, fmt.Errorf("could not create all: %v", err)
	}
	return fs.getFileWriter(path)
}

func (fs *HDFSFileStore) Write(path filestore.Filepath, data []byte) error {
	file, err := fs.createFile(path)
	if err != nil {
		return fmt.Errorf("could not create file: %v", err)
	}
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("could not write: %v", err)
	}
	if err := file.Flush(); err != nil {
		return fmt.Errorf("flush: %v", err)
	}
	file.Close()
	return nil
}

func (fs *HDFSFileStore) Read(path filestore.Filepath) ([]byte, error) {
	return fs.Client.ReadFile(path.Key())
}

func (fs *HDFSFileStore) ServeDirectory(path filestore.Filepath) (Iterator, error) {
	files, err := fs.Client.ReadDir(path.Key())
	if err != nil {
		return nil, err
	}
	var filepaths []filestore.Filepath
	for _, file := range files {
		if filepath, err := fs.CreateFilePath(fmt.Sprintf("%s/%s", path.KeyPrefix(), file.Name())); err == nil {
			filepaths = append(filepaths, filepath)
		} else {
			return nil, fmt.Errorf("could not create filepath: %v", err)
		}
	}
	// assume file type is parquet
	return parquetIteratorOverMultipleFiles(filepaths, fs)
}

func (fs *HDFSFileStore) Serve(path filestore.Filepath) (Iterator, error) {
	if path.IsDir() {
		return fs.ServeDirectory(path)
	}
	b, err := fs.Client.ReadFile(path.Key())
	if err != nil {
		return nil, fmt.Errorf("could not read file: %w", err)
	}
	switch path.KeyPrefix() {
	case "parquet":
		return parquetIteratorFromBytes(b)
	case "csv":
		return nil, fmt.Errorf("could not find CSV reader")
	default:
		return nil, fmt.Errorf("unsupported file type")
	}
}
func (fs *HDFSFileStore) Exists(path filestore.Filepath) (bool, error) {
	_, err := fs.Client.Stat(path.Key())
	fmt.Println("CHECKING EXISTS", err)
	if err != nil && strings.Contains(err.Error(), "file does not exist") {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (fs *HDFSFileStore) Delete(path filestore.Filepath) error {
	return fs.Client.Remove(path.Key())
}

func (fs *HDFSFileStore) deleteFile(file os.FileInfo, dir filestore.Filepath) error {
	if file.IsDir() {
		err := fs.DeleteAll(dir)
		if err != nil {
			return fmt.Errorf("could not delete directory: %v", err)
		}
	} else {
		err := fs.Delete(dir)
		if err != nil {
			return fmt.Errorf("could not delete file: %v", err)
		}
	}
	return nil
}

func (fs *HDFSFileStore) DeleteAll(dir filestore.Filepath) error {
	files, err := fs.Client.ReadDir(dir.Key())
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := fs.deleteFile(file, dir); err != nil {
			return fmt.Errorf("could not delete: %v", err)
		}
	}
	return fs.Client.Remove(dir.Key())
}

func (fs *HDFSFileStore) isPartialPath(prefix, path string) bool {
	return strings.Contains(prefix, path)
}

func (fs *HDFSFileStore) containsPrefix(prefix, path string) bool {
	return strings.Contains(path, prefix)
}

func (fs *HDFSFileStore) isMoreRecentFile(newFileTime, oldFileTime time.Time, fileType filestore.FileType, path string) bool {
	return (newFileTime.After(oldFileTime) || newFileTime.Equal(oldFileTime)) && fileType.Matches(path)
}

func (hdfs *HDFSFileStore) NewestFileOfType(rootpath filestore.Filepath, fileType filestore.FileType) (filestore.Filepath, error) {
	var lastModTime time.Time
	var lastModName string
	err := hdfs.Client.Walk("/", func(path string, info fs.FileInfo, err error) error {
		if hdfs.isPartialPath(rootpath.Key(), path) {
			return nil
		}
		if hdfs.containsPrefix(rootpath.Key(), path) && hdfs.isMoreRecentFile(info.ModTime(), lastModTime, fileType, path) {
			lastModTime = info.ModTime()
			lastModName = strings.TrimPrefix(path, "/")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if lastModName == "" {
		return nil, fmt.Errorf("could not find file")
	}
	filepath, err := filestore.NewEmptyFilepath(filestore.HDFS)
	if err != nil {
		return nil, err
	}
	// TODO: determine if this is actually correct
	err = filepath.ParseFilePath(lastModName)
	if err != nil {
		return nil, err
	}
	return filepath, nil
}

func (fs *HDFSFileStore) NumRows(path filestore.Filepath) (int64, error) {
	file, err := fs.Read(path)
	if err != nil {
		return 0, err
	}
	rows, err := getParquetNumRows(file)
	if err != nil {
		return 0, err
	}
	return rows, nil
}
func (fs *HDFSFileStore) Close() error {
	return fs.Client.Close()
}
func (fs *HDFSFileStore) Upload(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	return fs.Client.CopyToRemote(sourcePath.Key(), destPath.Key())
}
func (fs *HDFSFileStore) Download(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	return fs.Client.CopyToLocal(sourcePath.Key(), destPath.Key())
}
func (fs *HDFSFileStore) AsAzureStore() *AzureFileStore {
	return nil
}

func (fs *HDFSFileStore) FilestoreType() filestore.FileStoreType {
	return filestore.HDFS
}

func (fs *HDFSFileStore) AddEnvVars(envVars map[string]string) map[string]string {
	panic("HDFS Filestore is not supported for K8s at the moment.")
}

func (fs *HDFSFileStore) CreateDirPath(key string) (filestore.Filepath, error) {
	fp := filestore.HDFSFilepath{}
	fp.SetKey(key)
	fp.SetIsDir(true)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

// TODO: implement
func (fs *HDFSFileStore) CreateFilePath(key string) (filestore.Filepath, error) {
	fp := filestore.HDFSFilepath{}
	// fp.SetKey(key)
	// err := fp.Validate()
	// if err != nil {
	// 	return nil, err
	// }
	return &fp, nil
}

type genericFileStore struct {
	bucket    *blob.Bucket
	path      filestore.Filepath
	storeType filestore.FileStoreType
}

// TODO: deprecate this in favor of List
func (store *genericFileStore) NewestFileOfType(searchPath filestore.Filepath, fileType filestore.FileType) (filestore.Filepath, error) {
	opts := blob.ListOptions{
		Prefix: searchPath.Key(),
	}
	listIterator := store.bucket.List(&opts)
	mostRecentTime := time.UnixMilli(0)
	mostRecentKey := ""
	for {
		if newObj, err := listIterator.Next(context.TODO()); err == nil {
			mostRecentTime, mostRecentKey = store.getMoreRecentFile(newObj, fileType, mostRecentTime, mostRecentKey)
		} else if err == io.EOF {
			path, err := filestore.NewEmptyFilepath(store.FilestoreType())
			if err != nil {
				return nil, err
			}
			// TODO: Prior to adding this guard claus, the call to path.ParseFilePath would fail
			// with the following error if mostRecentKey is empty:
			// invalid scheme '://', must be one of [gs:// s3:// s3a:// abfss:// hdfs://]
			if mostRecentKey == "" {
				return path, nil
			}
			// **NOTE:** this is a hack to address the fact that genericFileStore is ignorant of the scheme, bucket, etc.
			// which means we're forced to use everything up to the path from the searchPath and replace its key with
			// the latest key found at the prefix. The long-term fix could/should be to implement all Filepath methods on
			// each implementation and call into the genericFileStore with additional parameters for the scheme, bucket, etc.
			err = path.ParseFilePath(searchPath.PathWithBucket())
			path.SetKey(mostRecentKey)
			if err != nil {
				return nil, err
			}
			return path, nil
		} else {
			return nil, err
		}
	}
}

func (store *genericFileStore) getMoreRecentFile(newObj *blob.ListObject, expectedFileType filestore.FileType, oldTime time.Time, oldKey string) (time.Time, string) {
	pathParts := strings.Split(newObj.Key, ".")
	fileType := pathParts[len(pathParts)-1]
	if fileType == string(expectedFileType) && !newObj.IsDir && store.isMostRecentFile(newObj, oldTime) {
		return newObj.ModTime, newObj.Key
	}
	return oldTime, oldKey
}

func (store *genericFileStore) isMostRecentFile(listObj *blob.ListObject, time time.Time) bool {
	return listObj.ModTime.After(time) || listObj.ModTime.Equal(time)
}

func (store *genericFileStore) outputFileList(path filestore.Filepath) []filestore.Filepath {
	opts := blob.ListOptions{
		Prefix:    path.Key(),
		Delimiter: "/",
	}
	listIterator := store.bucket.List(&opts)
	mostRecentOutputPartTime := "0000-00-00 00:00:00.000000"
	mostRecentOutputPartPath := ""
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if listObj == nil {
			return []filestore.Filepath{}
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
	filepathList := make([]filestore.Filepath, 0)
	for listObj, err := partsIterator.Next(context.TODO()); err == nil; listObj, err = partsIterator.Next(context.TODO()) {
		pathParts := strings.Split(listObj.Key, ".")

		fileType := pathParts[len(pathParts)-1]
		if fileType == "parquet" {
			// partsList = append(partsList, listObj.Key)
			if filepath, err := store.CreateFilePath(listObj.Key); err == nil {
				filepathList = append(filepathList, filepath)
			} else {
				fmt.Printf("Could not create filepath: %v\n", err)
			}
		}
	}
	// TODO: implement sort for Filepath interface
	// sort.Strings(partsList)
	return filepathList
}

func (store *genericFileStore) DeleteAll(path filestore.Filepath) error {
	opts := blob.ListOptions{
		Prefix: path.Key(),
	}
	listIterator := store.bucket.List(&opts)
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if !listObj.IsDir {
			if err := store.bucket.Delete(context.TODO(), listObj.Key); err != nil {
				return fmt.Errorf("failed to delete object %s in directory %s: %v", listObj.Key, path.Key(), err)
			}
		}
	}
	return nil
}

func (store *genericFileStore) Write(path filestore.Filepath, data []byte) error {
	ctx := context.TODO()
	err := store.bucket.WriteAll(ctx, path.Key(), data, nil)
	if err != nil {
		return err
	}
	err = re.Do(
		func() error {
			blob, errRetr := store.bucket.ReadAll(ctx, path.Key())
			fmt.Printf("Read (%d) bytes from bucket (%s) after write\n", len(data), path.Key())
			if errRetr != nil {
				return re.Unrecoverable(errRetr)
			} else if !bytes.Equal(blob, data) {
				return fmt.Errorf("blob read from bucket does not match blob written to bucket")
			}
			return nil
		},
		re.DelayType(func(n uint, err error, config *re.Config) time.Duration {
			return re.BackOffDelay(n, err, config)
		}),
		re.Attempts(10),
	)
	return err
}

func (store *genericFileStore) Read(path filestore.Filepath) ([]byte, error) {
	data, err := store.bucket.ReadAll(context.TODO(), path.Key())
	if err != nil {
		return nil, err
	}
	fmt.Printf("Read (%d) bytes of object with key (%s)\n", len(data), path.Key())
	return data, nil
}

func (store *genericFileStore) ServeDirectory(dir filestore.Filepath) (Iterator, error) {
	fileParts := store.outputFileList(dir)
	if len(fileParts) == 0 {
		return nil, fmt.Errorf("no files in given directory")
	}
	// assume file type is parquet
	return parquetIteratorOverMultipleFiles(fileParts, store)
}

func (store *genericFileStore) Upload(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	content, err := ioutil.ReadFile(sourcePath.Key())
	if err != nil {
		return fmt.Errorf("cannot read %s file: %v", sourcePath, err)
	}

	err = store.Write(destPath, content)
	if err != nil {
		return fmt.Errorf("cannot upload %s file to %s destination: %v", sourcePath, destPath, err)
	}

	return nil
}

func (store *genericFileStore) Download(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	content, err := store.Read(sourcePath)
	if err != nil {
		return fmt.Errorf("cannot read %s file: %v", sourcePath, err)
	}

	f, err := os.Create(destPath.Key())
	if err != nil {
		return fmt.Errorf("cannot create %s file: %v", destPath, err)
	}
	defer f.Close()

	f.Write(content)

	return nil
}

func (store *genericFileStore) FilestoreType() filestore.FileStoreType {
	if store.storeType == "" {
		return filestore.Memory
	} else {
		return store.storeType
	}
}

func (store *genericFileStore) AddEnvVars(envVars map[string]string) map[string]string {
	return envVars
}

func (store *genericFileStore) Exists(path filestore.Filepath) (bool, error) {
	return store.bucket.Exists(context.TODO(), path.Key())
}

func (store *genericFileStore) Delete(path filestore.Filepath) error {
	return store.bucket.Delete(context.TODO(), path.Key())
}

func (store *genericFileStore) Close() error {
	return store.bucket.Close()
}

func (store *genericFileStore) ServeFile(path filestore.Filepath) (Iterator, error) {
	b, err := store.bucket.ReadAll(context.TODO(), path.Key())
	if err != nil {
		return nil, fmt.Errorf("could not read file: %w", err)
	}
	switch path.Ext() {
	case "parquet":
		return parquetIteratorFromBytes(b)
	case "csv":
		return nil, fmt.Errorf("csv iterator not implemented")
	default:
		return nil, fmt.Errorf("unsupported file type")
	}
}

func (store *genericFileStore) Serve(path filestore.Filepath) (Iterator, error) {
	if path.IsDir() {
		return store.ServeDirectory(path)
	} else {
		return store.ServeFile(path)
	}
}

func (store *genericFileStore) NumRows(path filestore.Filepath) (int64, error) {
	b, err := store.bucket.ReadAll(context.TODO(), path.Key())
	if err != nil {
		return 0, err
	}
	switch path.Ext() {
	case "parquet":
		return getParquetNumRows(b)
	default:
		return 0, fmt.Errorf("unsupported file type")
	}
}

// TODO: implement
func (store *genericFileStore) CreateDirPath(key string) (filestore.Filepath, error) {
	fp := filestore.FilePath{}
	// fp.SetKey(key)
	// fp.SetIsDir(true)
	// err := fp.Validate()
	// if err != nil {
	// 	return nil, err
	// }
	return &fp, nil
}

// TODO: implement
func (store *genericFileStore) CreateFilePath(key string) (filestore.Filepath, error) {
	fp := filestore.FilePath{}
	// fp.SetKey(key)
	// err := fp.Validate()
	// if err != nil {
	// 	return nil, err
	// }
	return &fp, nil
}
