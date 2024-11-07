// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"

	re "github.com/avast/retry-go/v4"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsv2cfg "github.com/aws/aws-sdk-go-v2/config"
	awsv2creds "github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/featureform/fferr"
	filestore "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	"github.com/google/uuid"

	"path/filepath"
	"time"

	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
)

const (
	// Default timeout on S3 calls
	defaultS3Timeout = 30 * time.Second
)

type FileStore interface {
	// Read Operations
	Open(key filestore.Filepath) (File, error)
	ReaderAt(key filestore.Filepath) (io.ReaderAt, error)
	Read(key filestore.Filepath) ([]byte, error)
	Exists(location pl.Location) (bool, error)
	NewestFileOfType(prefix filestore.Filepath, fileType filestore.FileType) (filestore.Filepath, error)
	List(dirPath filestore.Filepath, fileType filestore.FileType) ([]filestore.Filepath, error)
	NumRows(key filestore.Filepath) (int64, error)
	Download(sourcePath filestore.Filepath, destPath filestore.Filepath) error
	Serve(keys []filestore.Filepath) (Iterator, error)

	// Write Operations
	Write(key filestore.Filepath, data []byte) error
	Delete(key filestore.Filepath) error
	DeleteAll(dir filestore.Filepath) error
	Upload(sourcePath filestore.Filepath, destPath filestore.Filepath) error

	// Utility Operations
	AddEnvVars(envVars map[string]string) map[string]string
	Close() error
	FilestoreType() filestore.FileStoreType
	ParseFilePath(path string) (filestore.Filepath, error)
	// CreateFilePath creates a new filepath object with the bucket and scheme from a Key
	CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error)
}

type File interface {
	io.ReadSeeker
	Size() int64
}

type blobAdapter struct {
	bucket *blob.Bucket
	key    string
	mu     *sync.Mutex
}

func newBlobAdapter(bucket *blob.Bucket, key string) *blobAdapter {
	return &blobAdapter{
		bucket: bucket,
		key:    key,
		mu:     &sync.Mutex{},
	}
}

func (r *blobAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx := context.TODO()
	reader, err := r.bucket.NewRangeReader(ctx, r.key, off, int64(len(p)), nil)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	return io.ReadFull(reader, p)
}

func (r *blobAdapter) Size() int64 {
	ctx := context.TODO()
	reader, err := r.bucket.NewReader(ctx, r.key, nil)
	if err != nil {
		fmt.Printf("Unable to open file: %s", r.key)
		return 0
	}
	return reader.Size()
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
		return nil, err
	}
	bucket, err := blob.OpenBucket(context.TODO(), fileStoreConfig.DirPath)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.FileSystem), err)
		wrapped.AddDetail("dir_path", fileStoreConfig.DirPath)
		return nil, wrapped
	}
	filepath, err := filestore.NewEmptyFilepath(filestore.FileSystem)
	if err != nil {
		return nil, err
	}
	err = filepath.ParseDirPath(fileStoreConfig.DirPath)
	if err != nil {
		return nil, err
	}
	return &LocalFileStore{
		DirPath: fileStoreConfig.DirPath[len("file:///"):],
		genericFileStore: genericFileStore{
			bucket:    bucket,
			path:      filepath,
			storeType: filestore.FileSystem,
		},
	}, nil
}

func (fs *LocalFileStore) FilestoreType() filestore.FileStoreType {
	return filestore.FileSystem
}

func (fs *LocalFileStore) CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error) {
	fp := filestore.LocalFilepath{}
	if fs.FilestoreType() != filestore.FileSystem {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("filestore type: %v; use store-specific implementation instead", fs.FilestoreType()))
	}
	if err := fp.SetScheme(filestore.FileSystemPrefix); err != nil {
		return nil, err
	}
	if err := fp.SetBucket(fs.path.Bucket()); err != nil {
		return nil, err
	}
	if err := fp.SetKey(key); err != nil {
		return nil, err
	}
	if err := fp.Validate(); err != nil {
		return nil, err
	}
	fp.SetIsDir(false)
	return &fp, nil
}

type AzureFileStore struct {
	AccountName      string
	AccountKey       string
	ConnectionString string
	ContainerName    string
	Path             string
	genericFileStore
}

func (store *AzureFileStore) CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error) {
	fp := filestore.AzureFilepath{
		StorageAccount: store.AccountName,
	}
	if err := fp.SetScheme(filestore.AzureBlobPrefix); err != nil {
		return nil, err
	}
	fp.SetBucket(store.ContainerName)
	var err error
	if store.Path != "" && !strings.HasPrefix(key, store.Path) {
		err = fp.SetKey(fmt.Sprintf("%s/%s", store.Path, strings.Trim(key, "/")))
	} else {
		err = fp.SetKey(strings.Trim(key, "/"))
	}
	if err != nil {
		return nil, err
	}
	fp.SetIsDir(isDirectory)
	if err := fp.Validate(); err != nil {
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
		return nil, err
	}
	if azureStoreConfig.AccountName == "" {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("account name cannot be empty"))
	}
	if err := os.Setenv("AZURE_STORAGE_ACCOUNT", azureStoreConfig.AccountName); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("could not set storage account env: %w", err))
	}
	if azureStoreConfig.AccountKey == "" {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("account key cannot be empty"))
	}
	if err := os.Setenv("AZURE_STORAGE_KEY", azureStoreConfig.AccountKey); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("could not set storage key env: %w", err))
	}

	azureStoreConfig.ContainerName = strings.TrimPrefix(azureStoreConfig.ContainerName, "abfss://")
	if strings.Contains(azureStoreConfig.ContainerName, "/") {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("container_name cannot contain '/'. container_name should be the name of the Azure Blobstore container only"))
	}

	serviceURL := azureblob.ServiceURL(fmt.Sprintf("https://%s.blob.core.windows.net", azureStoreConfig.AccountName))
	client, err := azureblob.NewDefaultClient(serviceURL, azureblob.ContainerName(azureStoreConfig.ContainerName))
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.Azure), err)
		wrapped.AddDetail("service_url", string(serviceURL))
		return nil, wrapped
	}
	bucket, err := azureblob.OpenBucket(context.TODO(), client, nil)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.Azure), err)
		wrapped.AddDetail("container_name", azureStoreConfig.ContainerName)
		return nil, wrapped
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
		return nil, err
	}

	trimmedBucket := strings.TrimPrefix(s3StoreConfig.BucketPath, "s3://")
	trimmedBucket = strings.TrimPrefix(trimmedBucket, "s3a://")

	if strings.Contains(trimmedBucket, "/") {
		wrapped := fferr.NewInvalidArgumentError(fmt.Errorf("bucket_name cannot contain '/'. bucket_name should be the name of the AWS S3 bucket only"))
		wrapped.AddDetail("bucket_name", trimmedBucket)
		return nil, wrapped
	}

	opts := []func(*awsv2cfg.LoadOptions) error{
		awsv2cfg.WithRegion(s3StoreConfig.BucketRegion),
		awsv2cfg.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxBackoffDelay(retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
			}), defaultS3Timeout)
		}),
	}
	// If the user provides pc.AWSAssumeRoleCredentials, we will use the default credentials provider chain
	// to get the credentials stored on the pod. This is only possible if an IAM for Service Accounts has been
	// correctly configured on the EMR cluster and the K8s pod(s). See the following link for more information:
	// https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
	if staticCreds, ok := s3StoreConfig.Credentials.(pc.AWSStaticCredentials); ok {
		opts = append(opts, awsv2cfg.WithCredentialsProvider(
			awsv2creds.NewStaticCredentialsProvider(
				staticCreds.AccessKeyId,
				staticCreds.SecretKey,
				"",
			),
		),
		)
	}
	// If we are using a custom endpoint, such as when running localstack, we should point at it. We'd never set this when
	// directly accessing DynamoDB on AWS.
	if s3StoreConfig.Endpoint != "" {
		opts = append(opts,
			awsv2cfg.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           s3StoreConfig.Endpoint,
					SigningRegion: s3StoreConfig.BucketRegion,
				}, nil
			})))
	}
	cfg, err := awsv2cfg.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		wrapped := fferr.NewConnectionError(pt.SparkOffline.String(), err)
		wrapped.AddDetail("bucket_name", trimmedBucket)
		return nil, wrapped
	}

	clientV2 := s3v2.NewFromConfig(cfg)
	bucket, err := s3blob.OpenBucketV2(context.TODO(), clientV2, trimmedBucket, nil)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.S3), err)
		wrapped.AddDetail("bucket_name", trimmedBucket)
		return nil, wrapped
	}
	return &S3FileStore{
		Bucket:       trimmedBucket,
		BucketRegion: s3StoreConfig.BucketRegion,
		Credentials:  s3StoreConfig.Credentials,
		Path:         s3StoreConfig.Path,
		genericFileStore: genericFileStore{
			bucket:    bucket,
			storeType: filestore.S3,
		},
	}, nil
}

func (s3 *S3FileStore) CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error) {
	fp := filestore.S3Filepath{}
	// **NOTE:** It's possible we'll need to change this default based on whether the
	// user employs EMR as their Spark executor
	// See here for details: https://stackoverflow.com/questions/69984233/spark-s3-write-s3-vs-s3a-connectors
	if err := fp.SetScheme(filestore.S3APrefix); err != nil {
		return nil, err
	}
	if err := fp.SetBucket(s3.Bucket); err != nil {
		return nil, err
	}
	var err error
	if s3.Path != "" && !strings.HasPrefix(key, s3.Path) {
		err = fp.SetKey(fmt.Sprintf("%s/%s", s3.Path, strings.Trim(key, "/")))
	} else {
		err = fp.SetKey(strings.Trim(key, "/"))
	}
	if err != nil {
		return nil, err
	}
	fp.SetIsDir(isDirectory)
	if err := fp.Validate(); err != nil {
		return nil, err
	}
	return &fp, nil
}

func (s3 *S3FileStore) FilestoreType() filestore.FileStoreType {
	return filestore.S3
}

func (s3 *S3FileStore) AddEnvVars(envVars map[string]string) map[string]string {
	if staticCreds, ok := s3.Credentials.(pc.AWSStaticCredentials); ok {
		envVars["AWS_ACCESS_KEY_ID"] = staticCreds.AccessKeyId
		envVars["AWS_SECRET_KEY"] = staticCreds.SecretKey
	}
	envVars["BLOB_STORE_TYPE"] = "s3"
	envVars["S3_BUCKET_REGION"] = s3.BucketRegion
	envVars["S3_BUCKET_NAME"] = s3.Bucket
	return envVars
}

type GCSFileStore struct {
	Bucket      string
	Path        string
	Credentials pc.GCPCredentials
	genericFileStore
}

func (gs *GCSFileStore) CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error) {
	fp := filestore.GCSFilepath{}
	if err := fp.SetScheme(filestore.GSPrefix); err != nil {
		return nil, err
	}
	if err := fp.SetBucket(gs.Bucket); err != nil {
		return nil, err
	}
	var err error
	if gs.Path != "" && !strings.HasPrefix(key, gs.Path) {
		err = fp.SetKey(fmt.Sprintf("%s/%s", gs.Path, strings.Trim(key, "/")))
	} else {
		err = fp.SetKey(strings.Trim(key, "/"))
	}
	if err != nil {
		return nil, err
	}
	fp.SetIsDir(isDirectory)
	if err := fp.Validate(); err != nil {
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
		return fferr.NewInternalError(err)
	}
	return nil
}

func (s *GCSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
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
		return nil, err
	}

	GCSConfig.BucketName = strings.TrimPrefix(GCSConfig.BucketName, "gs://")

	if strings.Contains(GCSConfig.BucketName, "/") {
		wrapped := fferr.NewInvalidArgumentError(fmt.Errorf("bucket_name cannot contain '/'. bucket_name should be the name of the GCS bucket only"))
		wrapped.AddDetail("bucket_name", GCSConfig.BucketName)
		return nil, wrapped
	}

	serializedFile, err := json.Marshal(GCSConfig.Credentials.JSON)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	creds, err := google.CredentialsFromJSON(context.TODO(), serializedFile, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.GCS), err)
		wrapped.AddDetail("bucket_name", GCSConfig.BucketName)
		return nil, wrapped
	}

	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds))
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.GCS), err)
		wrapped.AddDetail("bucket_name", GCSConfig.BucketName)
		return nil, wrapped
	}

	bucket, err := gcsblob.OpenBucket(context.TODO(), client, GCSConfig.BucketName, nil)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(filestore.GCS), err)
		wrapped.AddDetail("bucket_name", GCSConfig.BucketName)
		return nil, wrapped
	}
	return &GCSFileStore{
		Bucket:      GCSConfig.BucketName,
		Path:        GCSConfig.BucketPath,
		Credentials: GCSConfig.Credentials,
		genericFileStore: genericFileStore{
			bucket:    bucket,
			storeType: filestore.GCS,
		},
	}, nil
}

type genericFileStore struct {
	bucket    *blob.Bucket
	path      filestore.Filepath
	storeType filestore.FileStoreType
}

func (store *genericFileStore) ParseFilePath(path string) (filestore.Filepath, error) {
	fp, err := filestore.NewEmptyFilepath(store.FilestoreType())
	if err != nil {
		return nil, err
	}
	err = fp.ParseFilePath(path)
	if err != nil {
		return nil, err
	}
	return fp, nil
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
			// Prior to adding this guard clause, the call to path.ParseFilePath would fail
			// with the following error if mostRecentKey is empty:
			// invalid scheme '://', must be one of [gs:// s3:// s3a:// abfss:// hdfs://]
			if mostRecentKey == "" {
				return path, nil
			}
			// **NOTE:** this is a hack to address the fact that genericFileStore is ignorant of the scheme, bucket, etc.
			// which means we're forced to use everything up to the path from the searchPath and replace its key with
			// the latest key found at the prefix. The long-term fix could/should be to implement all Filepath methods on
			// each implementation and call into the genericFileStore with additional parameters for the scheme, bucket, etc.
			err = path.ParseFilePath(searchPath.ToURI())
			if err != nil {
				return nil, err
			}
			if err := path.SetKey(mostRecentKey); err != nil {
				return nil, err
			}
			// TODO: consider reevaluating whether a path is a directory or file path when setting the key
			path.SetIsDir(false)
			return path, nil
		} else {
			wrapped := fferr.NewInternalError(err)
			wrapped.AddDetail("uri", searchPath.ToURI())
			wrapped.AddDetail("file_type", string(fileType))
			return nil, wrapped
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

func (store *genericFileStore) List(searchPath filestore.Filepath, fileType filestore.FileType) ([]filestore.Filepath, error) {
	opts := blob.ListOptions{
		Prefix: searchPath.Key(),
	}
	files := make([]filestore.Filepath, 0)
	iter := store.bucket.List(&opts)
	var iterError error
	for {
		if obj, err := iter.Next(context.TODO()); err == nil {
			path, err := filestore.NewEmptyFilepath(store.FilestoreType())
			if err != nil {
				fmt.Println("could not get empty filepath", err)
				iterError = err
				break
			}
			// **NOTE:** this is a hack to address the fact that genericFileStore is ignorant of the scheme, bucket, etc.
			// which means we're forced to use everything up to the path from the searchPath and replace its key with
			// the latest key found at the prefix. The long-term fix could/should be to implement all Filepath methods on
			// each implementation and call into the genericFileStore with additional parameters for the scheme, bucket, etc.
			err = path.ParseFilePath(searchPath.ToURI())
			if err != nil {
				fmt.Println("could not parse filepath", err)
				iterError = err
				break
			}
			if err = path.SetKey(obj.Key); err != nil {
				fmt.Printf("could not set key %s: %v", obj.Key, err)
				iterError = err
				break
			}
			if err = path.Validate(); err != nil {
				fmt.Println("could not validate", err)
				iterError = err
				break
			}
			if path.Ext() == fileType {
				files = append(files, path)
			}
		} else if err == io.EOF {
			fmt.Printf("EOF for %s reached\n", searchPath.Key())
			iterError = nil
			break
		} else {
			fmt.Printf("error iterating over search path %s: %v", searchPath.Key(), err)
			break
		}
	}
	return files, iterError
}

func (store *genericFileStore) isMostRecentFile(listObj *blob.ListObject, time time.Time) bool {
	return listObj.ModTime.After(time) || listObj.ModTime.Equal(time)
}

func (store *genericFileStore) DeleteAll(path filestore.Filepath) error {
	opts := blob.ListOptions{
		Prefix: path.Key(),
	}
	listIterator := store.bucket.List(&opts)
	for listObj, err := listIterator.Next(context.TODO()); err == nil; listObj, err = listIterator.Next(context.TODO()) {
		if !listObj.IsDir {
			if err := store.bucket.Delete(context.TODO(), listObj.Key); err != nil {
				wrapped := fferr.NewExecutionError(string(store.FilestoreType()), fmt.Errorf("failed to delete object in directory: %v", err))
				wrapped.AddDetail("object", listObj.Key)
				wrapped.AddDetail("directory", path.Key())
				return wrapped
			}
		}
	}
	return nil
}

func (store *genericFileStore) Write(path filestore.Filepath, data []byte) error {
	ctx := context.TODO()
	err := store.bucket.WriteAll(ctx, path.Key(), data, nil)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return wrapped
	}
	err = re.Do(
		func() error {
			blob, errRetr := store.bucket.ReadAll(ctx, path.Key())
			if errRetr != nil {
				return errRetr
			} else if !bytes.Equal(blob, data) {
				return fferr.NewInternalError(fmt.Errorf("blob read from bucket does not match blob written to bucket"))
			}
			fmt.Printf("Read (%d) bytes from bucket (%s) after write\n", len(blob), path.Key())
			return nil
		},
		re.DelayType(func(n uint, err error, config *re.Config) time.Duration {
			return re.BackOffDelay(n, err, config)
		}),
		re.Attempts(10),
	)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return wrapped
	}
	return nil
}

func (store *genericFileStore) Open(path filestore.Filepath) (File, error) {
	reader, err := store.bucket.NewReader(context.TODO(), path.Key(), nil)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return nil, wrapped
	}
	return reader, nil
}

func (store *genericFileStore) ReaderAt(path filestore.Filepath) (io.ReaderAt, error) {
	return newBlobAdapter(store.bucket, path.Key()), nil
}

func (store *genericFileStore) Read(path filestore.Filepath) ([]byte, error) {
	data, err := store.bucket.ReadAll(context.TODO(), path.Key())
	if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return nil, wrapped
	}
	fmt.Printf("Read (%d) bytes of object with key (%s)\n", len(data), path.Key())
	return data, nil
}

func (store *genericFileStore) ServeDirectory(files []filestore.Filepath) (Iterator, error) {
	// assume file type is parquet
	return parquetIteratorOverMultipleFiles(files, store)
}

func (store *genericFileStore) Upload(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	content, err := os.ReadFile(sourcePath.Key())
	if err != nil {
		wrapped := fferr.NewInternalError(fmt.Errorf("cannot read file: %v", err))
		wrapped.AddDetail("source_uri", sourcePath.ToURI())
		wrapped.AddDetail("dest_uri", destPath.ToURI())
		return wrapped
	}

	err = store.Write(destPath, content)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), fmt.Errorf("cannot upload file to destination: %v", err))
		wrapped.AddDetail("source_uri", sourcePath.ToURI())
		wrapped.AddDetail("dest_uri", destPath.ToURI())
		return wrapped
	}

	return nil
}

func (store *genericFileStore) Serve(files []filestore.Filepath) (Iterator, error) {
	if len(files) > 1 {
		return store.ServeDirectory(files)
	} else {
		return store.ServeFile(files[0])
	}
}

func (store *genericFileStore) NumRows(path filestore.Filepath) (int64, error) {
	reader, err := store.ReaderAt(path)
	if err != nil {
		return 0, err
	}
	switch path.Ext() {
	case filestore.Parquet:
		return getParquetNumRows(reader)
	default:
		return 0, fmt.Errorf("unsupported file type")
	}
}

func (store *genericFileStore) CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error) {
	fp := filestore.FilePath{}
	if store.FilestoreType() != filestore.FileSystem {
		return nil, fmt.Errorf("filestore type: %v; use store-specific implementation instead", store.FilestoreType())
	}
	if err := fp.SetScheme(filestore.FileSystemPrefix); err != nil {
		return nil, err
	}
	if err := fp.SetBucket(store.path.Bucket()); err != nil {
		return nil, err
	}
	if err := fp.SetKey(key); err != nil {
		return nil, err
	}
	if err := fp.Validate(); err != nil {
		return nil, err
	}
	fp.SetIsDir(isDirectory)
	return &fp, nil
}

func (store *genericFileStore) Download(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	content, err := store.Read(sourcePath)
	if err != nil {
		return err
	}

	f, err := os.Create(destPath.Key())
	if err != nil {
		wrapped := fferr.NewInternalError(fmt.Errorf("cannot create file: %v", err))
		wrapped.AddDetail("source_uri", sourcePath.ToURI())
		wrapped.AddDetail("dest_uri", destPath.ToURI())
		return wrapped
	}
	defer f.Close()

	if _, err := f.Write(content); err != nil {
		wrapped := fferr.NewInternalError(fmt.Errorf("cannot write %s file: %v", destPath, err))
		wrapped.AddDetail("source_uri", sourcePath.ToURI())
		wrapped.AddDetail("dest_uri", destPath.ToURI())
		return wrapped
	}

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

// Unlike Azure Blob Storage, which does return true for "partial keys" (i.e. keys that are prefixes of other keys,
// which we're treating as a directory path), S3 and GCS does not. To sidestep this difference in behavior, we've added
// `Exists` to `S3FileStore`, which uses `List` with a key prefix under the hood to determine whether there are
// objects "under" the partial key/path.
// The assumption here is:
// * If the iterator returns the `EOF` error upon the first iteration, the "key" doesn't exist
// * If the iterator returns a non-`EOF` error, the "key" may or may not exist; however, we have to address the error
// * If neither the `EOF` nor non-`EOF` error is returned, the "key" exists and we break from the loop to avoid unnecessary iteration
func (store *genericFileStore) Exists(location pl.Location) (bool, error) {
	fileStoreLocation, isFileStoreLocation := location.(*pl.FileStoreLocation)
	if !isFileStoreLocation {
		return false, fferr.NewInvalidArgumentErrorf("location is not a filestore.Filepath")
	}
	path := fileStoreLocation.Filepath()
	iter := store.bucket.List(&blob.ListOptions{Prefix: path.Key()})
	_, err := iter.Next(context.Background())
	if err == io.EOF {
		return false, nil
	} else if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return false, wrapped
	} else {
		return true, nil
	}
}

func (store *genericFileStore) Delete(path filestore.Filepath) error {
	if err := store.bucket.Delete(context.TODO(), path.Key()); err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return wrapped
	}
	return nil
}

func (store *genericFileStore) Close() error {
	if err := store.bucket.Close(); err != nil {
		return fferr.NewExecutionError(string(store.FilestoreType()), err)
	}
	return nil
}

func (store *genericFileStore) ServeFile(path filestore.Filepath) (Iterator, error) {
	src, err := store.Read(path)
	if err != nil {
		wrapped := fferr.NewExecutionError(string(store.FilestoreType()), err)
		wrapped.AddDetail("uri", path.ToURI())
		return nil, wrapped
	}
	switch path.Ext() {
	case filestore.Parquet:
		return parquetIteratorFromBytes(bytes.NewReader(src))
	case filestore.CSV:
		return nil, fferr.NewInternalError(fmt.Errorf("csv iterator not implemented"))
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("unsupported file type"))
	}
}

func NewHDFSFileStore(config Config) (FileStore, error) {
	// Unfortunately, we couldn't use the kerberos package because of issues with encryption type with a client.
	// In order to work around it, we decided to use the kinit and hdfs cli as a work around.

	HDFSConfig := pc.HDFSFileStoreConfig{}

	err := HDFSConfig.Deserialize(pc.SerializedConfig(config))
	if err != nil {
		return nil, fmt.Errorf("could not deserialize config: %v", err)
	}

	var username string = "hduser"
	address := fmt.Sprintf("%s:%s", HDFSConfig.Host, HDFSConfig.Port)

	switch HDFSConfig.CredentialType {
	case pc.BasicCredential:
		credentials, ok := HDFSConfig.CredentialConfig.(*pc.BasicCredentialConfig)
		if !ok {
			return nil, fmt.Errorf("could not convert credential config to basic credential config")
		}

		if credentials.Username != "" {
			username = credentials.Username
		}

	case pc.KerberosCredential:
		credentials, ok := HDFSConfig.CredentialConfig.(*pc.KerberosCredentialConfig)
		if !ok {
			return nil, fmt.Errorf("could not convert credential config to kerberos credential config")
		}

		if credentials.Username != "" {
			username = credentials.Username
		}

		// Potential race condition here if multiple runs are trying to run at the same time
		if credentials.Krb5Conf == "" {
			return nil, fmt.Errorf("krb5.conf cannot be empty when using Kerberos Credentials")
		}
		krb5ConfFilePath := helpers.GetEnv("KRB5_CONF_FILE_PATH", "/etc/krb5.conf")
		err = writeToFile(krb5ConfFilePath, credentials.Krb5Conf)
		if err != nil {
			return nil, fmt.Errorf("could not write krb5.conf: %v", err)
		}

		err = runKinitCommand(credentials.Username, credentials.Password)
		if err != nil {
			return nil, fmt.Errorf("could not run kinit command: %v", err)
		}

	default:
		return nil, fmt.Errorf("unknown credential type: %v", HDFSConfig.CredentialType)
	}

	// Potential race condition here if multiple runs are trying to run at the same time
	if HDFSConfig.HDFSSiteConf != "" {
		hdfsSiteFilePath := helpers.GetEnv("HDFS_SITE_FILE_PATH", "/usr/local/hadoop/etc/hadoop/hdfs-site.xml")
		err = writeToFile(hdfsSiteFilePath, HDFSConfig.HDFSSiteConf)
		if err != nil {
			return nil, fmt.Errorf("could not write hdfs-site.xml: %v", err)
		}
	}

	// Potential race condition here if multiple runs are trying to run at the same time
	if HDFSConfig.CoreSiteConf != "" {
		coreSiteFilePath := helpers.GetEnv("CORE_SITE_FILE_PATH", "/usr/local/hadoop/etc/hadoop/core-site.xml")
		err = writeToFile(coreSiteFilePath, HDFSConfig.CoreSiteConf)
		if err != nil {
			return nil, fmt.Errorf("could not write core-site.xml: %v", err)
		}
	}

	return &HDFSFileStore{
		Username: username,
		Host:     address,
		Path:     HDFSConfig.Path,
	}, nil
}

func runKinitCommand(username, password string) error {
	kinitPath := "/usr/bin/kinit"

	// Create the kinit command
	cmd := exec.Command(kinitPath, username)

	// Start the command and provide the password as input
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fferr.NewConnectionError(pt.HDFS.String(), err)
	}
	go func() {
		defer stdin.Close()
		_, err := bytes.NewBufferString(password).WriteTo(stdin)
		if err != nil {
			return
		}
	}()

	// Execute the kinit command
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fferr.NewConnectionError(pt.HDFS.String(), fmt.Errorf("could not connect to Kerberos: %v, %s", err, output))
	}
	return nil
}

func writeToFile(filename, data string) error {
	// Extract the directory path from the file path
	dirPath := filepath.Dir(filename)

	// Create the entire path to the directory (including parent directories)
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	file, err := os.Create(filename)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	defer file.Close()

	if _, err = file.WriteString(data); err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

type HDFSFileStore struct {
	Username     string
	Password     string
	Host         string
	Krb5sConf    []byte
	HDFSSiteConf []byte
	Path         string
}

func (fs *HDFSFileStore) ParseFilePath(path string) (filestore.Filepath, error) {
	fp, err := filestore.NewEmptyFilepath(filestore.HDFS)
	if err != nil {
		return nil, err
	}
	err = fp.ParseFilePath(path)
	if err != nil {
		return nil, err
	}
	return fp, nil
}

func (fs *HDFSFileStore) Write(key filestore.Filepath, data []byte) error {
	uniqueID := uuid.New().String()
	filePath := fmt.Sprintf("/tmp/%s/%s", uniqueID, strings.TrimLeft(key.Key(), "/"))

	localFilePath, err := filestore.NewEmptyFilepath(filestore.FileSystem)
	if err != nil {
		return err
	}
	if err = localFilePath.SetKey(filePath); err != nil {
		return err
	}
	if err := os.MkdirAll(localFilePath.KeyPrefix(), 0777); err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("local_file_path", localFilePath.Key())
		return wrapped
	}

	localFile, err := os.Create(localFilePath.Key())
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("local_file_path", localFilePath.Key())
		return wrapped
	}
	defer localFile.Close()

	_, err = localFile.Write(data)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("local_file_path", localFilePath.Key())
		return wrapped
	}

	err = fs.Upload(localFilePath, key)
	if err != nil {
		return err
	}

	err = fs.deleteLocalFile(localFilePath)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("local_file_path", localFilePath.Key())
		return wrapped
	}
	return nil
}

func (fs *HDFSFileStore) Writer(key string) (*blob.Writer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (fs *HDFSFileStore) Open(key filestore.Filepath) (File, error) {
	return fs.bytesReader(key)
}

func (fs *HDFSFileStore) ReaderAt(key filestore.Filepath) (io.ReaderAt, error) {
	return fs.bytesReader(key)
}

func (fs *HDFSFileStore) bytesReader(key filestore.Filepath) (*bytes.Reader, error) {
	// This is quite wasteful and slow compared to the other file stores, because it
	// reads everything into memory.
	byteSlice, err := fs.Read(key)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(byteSlice), nil
}

func (fs *HDFSFileStore) Read(key filestore.Filepath) ([]byte, error) {
	uniqueID := uuid.New().String()
	filePath := fmt.Sprintf("/tmp/%s/%s", uniqueID, strings.TrimLeft(key.Key(), "/"))

	localFilePath, err := filestore.NewEmptyFilepath(filestore.FileSystem)
	if err != nil {
		return nil, fmt.Errorf("could not create local file path: %v", err)
	}
	if err = localFilePath.SetKey(filePath); err != nil {
		return nil, fmt.Errorf("could not set local file path: %v", err)
	}
	if err := os.MkdirAll(localFilePath.KeyPrefix(), 0777); err != nil {
		return nil, fmt.Errorf("could not create local directories '%s': %v", localFilePath.KeyPrefix(), err)
	}

	command := []string{"dfs", "-get", key.Key(), localFilePath.Key()}
	_, err = executeCommand(command)
	if err != nil {
		return nil, fmt.Errorf("could not copy from hdfs '%s' to local '%s': %v", key, localFilePath.Key(), err)
	}

	// Read the file or directory from the local filesystem
	data, err := ioutil.ReadFile(localFilePath.Key())
	if err != nil {
		return nil, fmt.Errorf("could not read local file '%s': %v", localFilePath.Key(), err)
	}

	err = fs.deleteLocalFile(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not delete local file '%s' after reading from HDFS: %v", localFilePath.Key(), err)
	}

	return data, nil
}

func (fs *HDFSFileStore) ServeDirectory(files []filestore.Filepath) (Iterator, error) {
	return parquetIteratorOverMultipleFiles(files, fs)
}

func (fs *HDFSFileStore) Serve(files []filestore.Filepath) (Iterator, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to serve")
	}
	if len(files) > 1 {
		return fs.ServeDirectory(files)
	}

	file := files[0]
	b, err := fs.Read(file)
	if err != nil {
		return nil, fmt.Errorf("could not read file: %w", err)
	}
	switch file.Ext() {
	case "parquet":
		return parquetIteratorFromBytes(bytes.NewReader(b))
	case "csv":
		return nil, fmt.Errorf("could not find CSV reader")
	default:
		return nil, fmt.Errorf("unsupported file type")
	}
}

func (fs *HDFSFileStore) Exists(location pl.Location) (bool, error) {
	fileStoreLocation, isFileStoreLocation := location.(*pl.FileStoreLocation)
	if !isFileStoreLocation {
		return false, fferr.NewInvalidArgumentErrorf("location is not a filestore.Filepath")
	}
	path := fileStoreLocation.Filepath()
	lsCommand := []string{"dfs", "-ls", path.Key()}
	_, err := executeCommand(lsCommand)
	if err != nil && strings.Contains(err.Error(), "No such file or directory") {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (fs *HDFSFileStore) Delete(key filestore.Filepath) error {
	rmCommand := []string{"dfs", "-rm", key.Key()}
	_, err := executeCommand(rmCommand)
	if err != nil {
		return err
	}
	return nil
}

func (fs *HDFSFileStore) DeleteAll(dir filestore.Filepath) error {
	deleteDirectoryCommand := []string{"dfs", "-rm", "-r", dir.Key()}

	_, err := executeCommand(deleteDirectoryCommand)
	if err != nil {
		return err
	}
	return nil
}

func (fs *HDFSFileStore) isPartialPath(prefix, path string) bool {
	// TODO: Might need to reimplement this method with filestore.FilePath
	return strings.Contains(prefix, path)
}

func (fs *HDFSFileStore) containsPrefix(prefix, path string) bool {
	// TODO: Might need to reimplement this method with filestore.FilePath
	return strings.Contains(path, prefix)
}

func (hdfs *HDFSFileStore) NewestFileOfType(rootpath filestore.Filepath, fileType filestore.FileType) (filestore.Filepath, error) {
	output, err := executeCommand([]string{"dfs", "-ls", "-R", rootpath.Key()})
	if err != nil && strings.Contains(err.Error(), "No such file or directory") {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("could not find the files: %v", err)
	}

	latestFile := hdfs.getLatestFile(output, fileType)

	fileExtension := filepath.Ext(latestFile)
	isParquetDirectory := fileType == filestore.Parquet && fileExtension == ""
	if hdfs.containsPrefix(rootpath.Key(), latestFile) && (fileType.Matches(latestFile) || isParquetDirectory) {
		latestFilePath, err := filestore.NewEmptyFilepath(hdfs.FilestoreType())
		if err != nil {
			return nil, err
		}
		fullLatestPath := fmt.Sprintf("%s/%s", filestore.HDFSPrefix, latestFile)
		err = latestFilePath.ParseFilePath(fullLatestPath)
		if err != nil {
			return nil, err
		}
		return latestFilePath, nil
	}

	return nil, fmt.Errorf("could not find the file with extension '%s' in the directory '%s'", fileType, rootpath.Key())
}

func (hdfs *HDFSFileStore) getLatestFile(output string, fileType filestore.FileType) string {
	// Based on the output of the dfs -ls -R, the fields are as follows below. We want to pull
	// the latest modified file from the output.

	// TODO: add unit tests for it
	const (
		PERMISSIONS = iota
		REPLICATION = iota
		OWNER       = iota
		GROUP       = iota
		FILESIZE    = iota
		MOD_DATE    = iota
		MOD_TIME    = iota
		NAME        = iota
	)

	var latestFile string
	var latestModTime time.Time

	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		parts := strings.Fields(line)
		re := regexp.MustCompile(`^-[rwxs-]{9}$`)
		if len(parts) >= 7 && re.MatchString(parts[PERMISSIONS]) && (fileType.Matches(parts[NAME])) {
			modifiedTime, err := hdfs.getModifiedTime(parts[NAME])
			if err != nil {
				continue
			}
			if modifiedTime.After(latestModTime) {
				latestModTime = modifiedTime
				latestFile = parts[NAME]
			}
		}
	}
	return latestFile
}

func (hdfs *HDFSFileStore) getModifiedTime(path string) (time.Time, error) {
	// TODO: reimplement with filestore.FilePath
	output, err := executeCommand([]string{"dfs", "-stat", "featureform --%y", path})
	if err != nil {
		fmt.Printf("could not get stats on the file '%s': %v", path, err)
		return time.Time{}, err
	}

	regex := regexp.MustCompile("featureform --")
	match := regex.FindString(output)
	findIdx := strings.Index(output, match)
	if findIdx == -1 {
		return time.Time{}, fmt.Errorf("could not index pattern in output '%s'", output)
	}

	modifiedTime := strings.TrimRight(output[findIdx+len(match):], "\n\r\t ")
	parsedModifiedTime, err := time.Parse("2006-01-02 15:04:05", modifiedTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse modified time '%s': %v", modifiedTime, err)
	}

	return parsedModifiedTime, nil
}

func (fs *HDFSFileStore) NumRows(key filestore.Filepath) (int64, error) {
	file, err := fs.Read(key)
	if err != nil {
		return 0, err
	}
	rows, err := getParquetNumRows(bytes.NewReader(file))
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (fs *HDFSFileStore) Close() error {
	return nil
}

func (fs *HDFSFileStore) Upload(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	directoriesToCreate := destPath.KeyPrefix()
	createHDFSDirectoriesCommand := []string{"dfs", "-mkdir", "-p", directoriesToCreate}
	_, err := executeCommand(createHDFSDirectoriesCommand)
	if err != nil {
		return err
	}

	copyCommand := []string{"dfs", "-put", "-f", sourcePath.Key(), destPath.Key()}
	_, err = executeCommand(copyCommand)
	if err != nil {
		return err
	}

	return nil
}

func (fs *HDFSFileStore) Download(sourcePath filestore.Filepath, destPath filestore.Filepath) error {
	err := os.MkdirAll(destPath.KeyPrefix(), 0777)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	copyCommand := []string{"dfs", "-get", sourcePath.Key(), destPath.Key()}
	_, err = executeCommand(copyCommand)
	if err != nil {
		return err
	}

	return nil
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

func (fs *HDFSFileStore) deleteLocalFile(file filestore.Filepath) error {
	err := os.RemoveAll(file.Key())
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (fs *HDFSFileStore) CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error) {
	fp := filestore.HDFSFilepath{}
	if err := fp.SetScheme(filestore.HDFSPrefix); err != nil {
		return nil, err
	}

	fullKey := fmt.Sprintf("/%s", strings.TrimPrefix(key, "/"))
	if fs.Path != "" {
		fullKey = fmt.Sprintf("/%s/%s", strings.TrimPrefix(fs.Path, "/"), strings.TrimPrefix(key, "/"))
	}
	if err := fp.SetKey(fullKey); err != nil {
		return nil, err
	}
	fp.SetIsDir(isDirectory)
	err := fp.Validate()
	if err != nil {
		return nil, err
	}
	return &fp, nil
}

func (fs *HDFSFileStore) List(dirPath filestore.Filepath, fileType filestore.FileType) ([]filestore.Filepath, error) {
	output, err := executeCommand([]string{"dfs", "-ls", "-t", dirPath.Key()})
	if err != nil {
		return nil, err
	}

	// use regex to parse the output to get 'Found %d items' index
	pattern := `Found \d+ items`
	regex := regexp.MustCompile(pattern)
	match := regex.FindString(output)
	findIdx := strings.Index(output, match)

	if findIdx == -1 {
		return nil, fmt.Errorf("could not find files in directory '%s'", dirPath.Key())
	}

	rows := strings.Split(output[findIdx+len(match):], "\n")
	fileNames := []filestore.Filepath{}
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		columns := strings.Split(row, " ")
		filename := columns[len(columns)-1]

		filePath, err := fs.CreateFilePath(filename, false)
		if err != nil {
			return nil, fmt.Errorf("could not create file path '%s': %v", filename, err)
		}
		fileNames = append(fileNames, filePath)
	}

	return fileNames, nil
}

func executeCommand(command []string) (string, error) {
	HDFSPath := "/usr/local/hadoop/bin/hdfs"
	cmd := exec.Command(HDFSPath, command...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fferr.NewExecutionError(pt.HDFS.String(), fmt.Errorf("could not execute command: %v, %s", err, output))
	}
	return string(output), nil
}
