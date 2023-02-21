package provider

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv2cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	pc "github.com/featureform/provider/provider_config"

	"github.com/colinmarc/hdfs"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
)

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem               = "LOCAL_FILESYSTEM"
	Azure                    = "AZURE"
	S3                       = "S3"
	GCS                      = "GCS"
	HDFS                     = "HDFS"
)

type FileType string

const (
	Parquet FileType = "parquet"
	CSV     FileType = "csv"
	DB      FileType = "db"
)

const (
	gsPrefix        = "gs://"
	s3Prefix        = "s3://"
	azureBlobPrefix = "abfss://"
)

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
	return LocalFileStore{
		DirPath: fileStoreConfig.DirPath[len("file:///"):],
		genericFileStore: genericFileStore{
			bucket: bucket,
			path:   fileStoreConfig.DirPath,
		},
	}, nil
}

type AzureFileStore struct {
	AccountName      string
	AccountKey       string
	ConnectionString string
	ContainerName    string
	Path             string
	genericFileStore
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

func (store *AzureFileStore) addAzureVars(envVars map[string]string) map[string]string {
	envVars["AZURE_CONNECTION_STRING"] = store.ConnectionString
	envVars["AZURE_CONTAINER_NAME"] = store.ContainerName
	return envVars
}

func (store AzureFileStore) AsAzureStore() *AzureFileStore {
	return &store
}

func (store AzureFileStore) PathWithPrefix(path string, remote bool) string {
	if !remote {
		if len(path) != 0 && path[0:len(store.Path)] != store.Path && store.Path != "" {
			return fmt.Sprintf("%s/%s", store.Path, path)
		}
	}
	if remote {
		prefix := ""
		pathContainsPrefix := path[:len(store.Path)] == store.Path
		if store.Path != "" && !pathContainsPrefix {
			prefix = fmt.Sprintf("%s/", store.Path)
		}
		return fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/%s%s", store.ContainerName, store.AccountName, prefix, path)
	}
	return path
}

func NewAzureFileStore(config Config) (FileStore, error) {
	azureStoreConfig := pc.AzureFileStoreConfig{}
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
		return AzureFileStore{}, fmt.Errorf("could not create azure client: %v", err)
	}

	bucket, err := azureblob.OpenBucket(context.TODO(), client, azureStoreConfig.ContainerName, nil)
	if err != nil {
		return AzureFileStore{}, fmt.Errorf("could not open azure bucket: %v", err)
	}
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", azureStoreConfig.AccountName, azureStoreConfig.AccountKey)
	return AzureFileStore{
		AccountName:      azureStoreConfig.AccountName,
		AccountKey:       azureStoreConfig.AccountKey,
		ConnectionString: connectionString,
		ContainerName:    azureStoreConfig.ContainerName,
		Path:             azureStoreConfig.Path,
		genericFileStore: genericFileStore{
			bucket: bucket,
		},
	}, nil
}

type S3FileStore struct {
	Bucket string
	Path   string
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
		return nil, err
	}
	cfg.Region = s3StoreConfig.BucketRegion
	clientV2 := s3v2.NewFromConfig(cfg)
	bucket, err := s3blob.OpenBucketV2(context.TODO(), clientV2, s3StoreConfig.BucketPath, nil)
	if err != nil {
		return nil, err
	}
	return &S3FileStore{
		Bucket: s3StoreConfig.BucketPath,
		Path:   s3StoreConfig.Path,
		genericFileStore: genericFileStore{
			bucket: bucket,
		},
	}, nil
}

func (s3 *S3FileStore) PathWithPrefix(path string, remote bool) string {
	s3PrefixLength := len("s3://")
	noS3Prefix := path[:s3PrefixLength] != "s3://"

	if remote && noS3Prefix {
		s3Path := ""
		if s3.Path != "" {
			s3Path = fmt.Sprintf("/%s", s3.Path)
		}
		return fmt.Sprintf("s3://%s%s/%s", s3.Bucket, s3Path, path)
	} else {
		return path
	}
}

type GCSFileStore struct {
	Bucket string
	Path   string
	genericFileStore
}

func (gs *GCSFileStore) PathWithPrefix(path string, remote bool) string {
	noGSPrefix := !strings.HasPrefix(path, gsPrefix)

	if remote && noGSPrefix {
		gsPathPrefix := ""
		if gs.Path != "" {
			gsPathPrefix = fmt.Sprintf("/%s", gs.Path)
		}
		return fmt.Sprintf("gs://%s%s/%s", gs.Bucket, gsPathPrefix, path)
	} else {
		return path
	}
}

func NewGCSFileStore(config Config) (FileStore, error) {
	GCSConfig := pc.GCSFileStoreConfig{}

	err := GCSConfig.Deserialize(pc.SerializedConfig(config))
	if err != nil {
		return nil, fmt.Errorf("could not deserialize config: %v", err)
	}

	creds, err := google.CredentialsFromJSON(context.TODO(), GCSConfig.Credentials.SerializedFile, "https://www.googleapis.com/auth/cloud-platform")
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
		Bucket: GCSConfig.BucketName,
		Path:   GCSConfig.BucketPath,
		genericFileStore: genericFileStore{
			bucket: bucket,
		},
	}, nil
}

type HDFSFileStoreConfig struct {
	Host string
	Port string
}

func (s *HDFSFileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *HDFSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *HDFSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func NewHDFSFileStore(config Config) (FileStore, error) {
	HDFSConfig := HDFSFileStoreConfig{}

	err := HDFSConfig.Deserialize(SerializedConfig(config))
	if err != nil {
		return nil, fmt.Errorf("could not deserialize config: %v", err)
	}

	address := fmt.Sprintf("%s:%s", HDFSConfig.Host, HDFSConfig.Port)
	fmt.Println("Connecting HDFS to", address)
	ops := hdfs.ClientOptions{
		Addresses: []string{address},
		User:      "hduser",
	}
	client, err := hdfs.NewClient(ops)
	//client, err := hdfs.New(address)
	if err != nil {
		panic(err)
	}

	return &HDFSFileStore{
		client,
	}, nil
}

type HDFSFileStore struct {
	Client *hdfs.Client
}

func (fs *HDFSFileStore) addPrefix(key string) string {
	return fmt.Sprintf("/%s", key)
}

func (fs *HDFSFileStore) Write(key string, data []byte) error {

	file, err := fs.Client.Create(fs.addPrefix(key))
	if err != nil {
		return fmt.Errorf("could not create file: %v", err)
	}
	fmt.Println("Created file")
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("could not write: %v", err)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("could not close file: %v", err)
	}
	return nil
}

func (fs *HDFSFileStore) Writer(key string) (*blob.Writer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (fs *HDFSFileStore) Read(key string) ([]byte, error) {
	return fs.Client.ReadFile(fs.addPrefix(key))
}

func (fs *HDFSFileStore) Serve(key string) (Iterator, error) {
	//keyParts := strings.Split(key, ".")
	//if len(keyParts) == 1 {
	//	return fs.ServeDirectory(key)
	//}
	//b, err := store.bucket.ReadAll(context.TODO(), key)
	//if err != nil {
	//	return nil, fmt.Errorf("could not read file: %w", err)
	//}
	//switch fileType := keyParts[len(keyParts)-1]; fileType {
	//case "parquet":
	//	return parquetIteratorFromBytes(b)
	//case "csv":
	//	return nil, fmt.Errorf("could not find CSV reader")
	//default:
	//	return nil, fmt.Errorf("unsupported file type")
	//}
	return &ParquetIteratorMultipleFiles{}, fmt.Errorf("unimplemented")

}
func (fs *HDFSFileStore) Exists(key string) (bool, error) {
	_, err := fs.Client.Stat(fs.addPrefix(key))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (fs *HDFSFileStore) Delete(key string) error {
	return fs.Client.Remove(fs.addPrefix(key))
}

func (fs *HDFSFileStore) DeleteAll(dir string) error {
	return fmt.Errorf("unimplemented")
}

func (fs *HDFSFileStore) NewestFileOfType(prefix string, fileType FileType) (string, error) {
	return "", fmt.Errorf("unimplemented")
}
func (fs *HDFSFileStore) PathWithPrefix(path string, remote bool) string {
	return ""
}
func (fs *HDFSFileStore) NumRows(key string) (int64, error) {
	return 0, fmt.Errorf("unimplemented")
}
func (fs *HDFSFileStore) Close() error {
	return fs.Client.Close()
}
func (fs *HDFSFileStore) Upload(sourcePath string, destPath string) error {
	return fmt.Errorf("unimplemented")
}
func (fs *HDFSFileStore) Download(sourcePath string, destPath string) error {
	return fmt.Errorf("unimplemented")
}
func (fs *HDFSFileStore) AsAzureStore() *AzureFileStore {
	return nil
}
