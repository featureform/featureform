// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv2config "github.com/aws/aws-sdk-go-v2/config"
	awsv2Creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/types"
	"github.com/pkg/errors"
)

type SparkFileStore interface {
	Type() SparkFileStoreType
	SparkConfigs() sparkConfigs
	FileStore
}

type SparkFileStoreType string

func (t SparkFileStoreType) String() string {
	return string(t)
}

const (
	SFS_S3         SparkFileStoreType = "s3"
	SFS_AZURE_BLOB SparkFileStoreType = "azure_blob_store"
	SFS_GCS        SparkFileStoreType = "google_cloud_storage"
	SFS_HDFS       SparkFileStoreType = "hdfs"
	SFS_LOCAL      SparkFileStoreType = "local"
)

type SparkFileStoreV2 interface {
	CreateFilePath(key string, isDirectory bool) (filestore.Filepath, error)
	Write(key filestore.Filepath, data []byte) error
	Read(key filestore.Filepath) ([]byte, error)
	Delete(key filestore.Filepath) error
	Close() error
	SparkConfigs() sparkConfigs

	Exists(location pl.Location) (bool, error)
	// Seems like one of these could be deprecated or we need clarity on the diff
	Type() SparkFileStoreType
	FilestoreType() filestore.FileStoreType

	// TODO deprecate these three and use SparkConfigs() instead
	// SparkConfig() []string
	// CredentialsConfig() []string
	// Packages() []string
}

type SparkFileStoreFactory func(config Config) (SparkFileStore, error)

var sparkFileStoreMap = map[filestore.FileStoreType]SparkFileStoreFactory{
	filestore.FileSystem: NewSparkLocalFileStore,
	filestore.Azure:      NewSparkAzureFileStore,
	filestore.S3:         NewSparkS3FileStore,
	filestore.Glue:       NewSparkGlueS3FileStore,
	filestore.GCS:        NewSparkGCSFileStore,
	filestore.HDFS:       NewSparkHDFSFileStore,
}

func CreateSparkFileStore(fsType filestore.FileStoreType, glueConfig *pc.GlueConfig, config Config) (SparkFileStore, error) {
	factory, exists := sparkFileStoreMap[fsType]
	if !exists {
		return nil, fferr.NewInternalError(fmt.Errorf("factory does not exist: %s", fsType))
	}
	fileStore, err := factory(config)
	if err != nil {
		return nil, err
	}

	// TODO This is temporary, the right way is to have the factory do this but because the config only contains
	// filestore details and not the glue details, we're doing this here
	glueStore, ok := fileStore.(*SparkGlueS3FileStore)
	if ok {
		fileStore, err = glueStore.InitGlueClient(glueConfig)
		if err != nil {
			return nil, err
		}
	}

	return fileStore, nil
}

type SparkS3FileStore struct {
	*S3FileStore
}

func NewSparkS3FileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewS3FileStore(config)
	if err != nil {
		return nil, err
	}
	s3, ok := fileStore.(*S3FileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *S3FileStore"))
	}

	return &SparkS3FileStore{s3}, nil
}

func (s3 SparkS3FileStore) SparkConfigs() sparkConfigs {
	accessKey, secretKey := "", ""
	if staticCreds, ok := s3.Credentials.(pc.AWSStaticCredentials); ok {
		accessKey = staticCreds.AccessKeyId
		secretKey = staticCreds.SecretKey
	}
	return sparkConfigs{
		sparkS3Flags{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Region:    s3.BucketRegion,
			Bucket:    s3.Bucket,
		},
	}
}

func (s3 SparkS3FileStore) Type() SparkFileStoreType {
	return SFS_S3
}

// SparkGlueS3FileStore is a SparkFileStore that uses Glue as the catalog for Iceberg tables
// It serves as a wrapper around the SparkS3FileStore and adds the necessary configurations for Glue
// Note: we only support catalogs with s3 right now
type SparkGlueS3FileStore struct {
	GlueConfig *pc.GlueConfig
	GlueClient *glue.Client
	Logger     logging.Logger
	SparkS3FileStore
}

func NewSparkGlueS3FileStore(config Config) (SparkFileStore, error) {
	logr := logging.NewLogger("spark_glue_s3_file_store")
	logr.Debugw("Creating SparkGlueS3FileStore")
	sparkS3FileStore, err := NewSparkS3FileStore(config)
	if err != nil {
		return nil, err
	}
	s3FileStore, ok := sparkS3FileStore.(*SparkS3FileStore)
	if !ok {
		return nil, fferr.NewInternalErrorf("could not cast file store to *SparkS3FileStore")
	}

	return &SparkGlueS3FileStore{
		SparkS3FileStore: *s3FileStore,
		GlueClient:       nil,
		Logger:           logr,
	}, nil
}

func (glueS3 *SparkGlueS3FileStore) InitGlueClient(glueConfig *pc.GlueConfig) (*SparkGlueS3FileStore, error) {
	// If the user is using a service account, we don't need to provide credentials
	// as the AWS SDK will use the IAM role of the K8s pod to authenticate.
	var opts []func(*awsv2config.LoadOptions) error
	if staticCreds, ok := glueS3.S3FileStore.Credentials.(pc.AWSStaticCredentials); ok {
		credsProvider := aws.NewCredentialsCache(
			awsv2Creds.NewStaticCredentialsProvider(staticCreds.AccessKeyId, staticCreds.SecretKey, ""),
		)
		opts = append(opts, awsv2config.WithCredentialsProvider(credsProvider))
	}
	opts = append(opts, awsv2config.WithRegion(glueConfig.Region))

	// Load the configuration using the provided options.
	cfg, err := awsv2config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, fferr.NewInternalErrorf("could not load AWS config for glue client: %v", err)
	}

	if glueConfig.AssumeRoleArn != "" {
		stsClient := sts.NewFromConfig(cfg)
		credsProvider := stscreds.NewAssumeRoleProvider(stsClient, glueConfig.AssumeRoleArn)
		cfg.Credentials = aws.NewCredentialsCache(credsProvider)
	}

	return &SparkGlueS3FileStore{
		GlueConfig:       glueConfig,
		GlueClient:       glue.NewFromConfig(cfg),
		Logger:           glueS3.Logger,
		SparkS3FileStore: glueS3.SparkS3FileStore,
	}, nil
}

func (glueS3 SparkGlueS3FileStore) Exists(location pl.Location) (bool, error) {
	switch loc := location.(type) {
	case *pl.CatalogLocation:
		input := &glue.GetTableInput{
			DatabaseName: aws.String(loc.Database()),
			Name:         aws.String(loc.Table()),
		}

		glueS3.Logger.Infow("Checking if Glue table exists", "table", input.Name, "database", input.DatabaseName)
		_, err := glueS3.GlueClient.GetTable(context.TODO(), input)

		var entityNotFoundErr *types.EntityNotFoundException
		if errors.As(err, &entityNotFoundErr) {
			// If the error is an EntityNotFoundException, the table does not exist
			return false, nil
		} else if err != nil {
			// If there is another type of error, return it
			return false, fferr.NewInternalErrorf("error checking if Glue table exists: %v", err)
		}
		return true, nil
	case *pl.FileStoreLocation:
		return glueS3.SparkS3FileStore.Exists(loc)
	default:
		return false, fferr.NewInternalErrorf("location type not supported: %T", location)
	}
}

func (glueS3 SparkGlueS3FileStore) SparkConfigs() sparkConfigs {
	s3Configs := glueS3.SparkS3FileStore.SparkConfigs()
	var tableFormat pt.TableFormatType
	var tableFormatConfig sparkConfig
	// TODO unify these under pt package
	switch glueS3.GlueConfig.TableFormat {
	case pc.Iceberg:
		tableFormat = pt.IcebergType
		tableFormatConfig = sparkIcebergFlags{}
	case pc.DeltaLake:
		tableFormat = pt.DeltaType
		tableFormatConfig = sparkDeltaFlags{}
	default:
		panic("Not implemented")
	}
	glueConfig := sparkGlueFlags{
		fileStoreType:   pt.S3Type,
		tableFormatType: tableFormat,
		Region:          glueS3.GlueConfig.Region,
		Warehouse:       glueS3.GlueConfig.Warehouse,
	}
	return append(s3Configs, glueConfig, tableFormatConfig)
}

func (glueS3 SparkGlueS3FileStore) ParseFilePath(path string) (filestore.Filepath, error) {
	return glueS3.SparkS3FileStore.ParseFilePath(path)
}

func NewSparkAzureFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewAzureFileStore(config)
	if err != nil {
		return nil, err
	}

	azure, ok := fileStore.(*AzureFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *AzureFileStore"))
	}

	return &SparkAzureFileStore{azure}, nil
}

type SparkAzureFileStore struct {
	*AzureFileStore
}

func (azureStore SparkAzureFileStore) SparkConfigs() sparkConfigs {
	return sparkConfigs{
		sparkAzureFlags{
			AccountName:      azureStore.AccountName,
			AccountKey:       azureStore.AccountKey,
			ConnectionString: azureStore.connectionString(),
			ContainerName:    azureStore.containerName(),
		},
	}
}

func (azureStore SparkAzureFileStore) Type() SparkFileStoreType {
	return SFS_AZURE_BLOB
}

func NewSparkGCSFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewGCSFileStore(config)
	if err != nil {
		return nil, err
	}
	gcs, ok := fileStore.(*GCSFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *GCSFileStore"))
	}
	serializedCredentials, err := json.Marshal(gcs.Credentials.JSON)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	return &SparkGCSFileStore{SerializedCredentials: serializedCredentials, GCSFileStore: gcs}, nil
}

type SparkGCSFileStore struct {
	SerializedCredentials []byte
	*GCSFileStore
}

func (gcs SparkGCSFileStore) SparkConfigs() sparkConfigs {
	return sparkConfigs{
		sparkGCSFlags{
			ProjectID: gcs.Credentials.ProjectId,
			Bucket:    gcs.Bucket,
			JSONCreds: gcs.SerializedCredentials,
		},
	}
}

func (gcs SparkGCSFileStore) Type() SparkFileStoreType {
	return SFS_GCS
}

func NewSparkHDFSFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewHDFSFileStore(config)
	if err != nil {
		return nil, err
	}
	hdfs, ok := fileStore.(*HDFSFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *HDFSFileStore"))
	}

	return &SparkHDFSFileStore{hdfs}, nil
}

type SparkHDFSFileStore struct {
	*HDFSFileStore
}

func (hdfs SparkHDFSFileStore) SparkConfigs() sparkConfigs {
	// Not currently tested
	return sparkConfigs{}
}

func (hdfs SparkHDFSFileStore) Type() SparkFileStoreType {
	return SFS_HDFS
}

func NewSparkLocalFileStore(config Config) (SparkFileStore, error) {
	fileStore, err := NewLocalFileStore(config)
	if err != nil {
		return nil, err
	}
	local, ok := fileStore.(*LocalFileStore)
	if !ok {
		return nil, fferr.NewInternalError(fmt.Errorf("could not cast file store to *LocalFileStore"))
	}

	return &SparkLocalFileStore{local}, nil
}

type SparkLocalFileStore struct {
	*LocalFileStore
}

func (local SparkLocalFileStore) SparkConfigs() sparkConfigs {
	// Not currently tested
	return sparkConfigs{}
}

func (local SparkLocalFileStore) Type() SparkFileStoreType {
	return SFS_LOCAL
}

type SparkFileStoreConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config pc.SerializedConfig) error
	IsFileStoreConfig() bool
}

func ResourcePath(id ResourceID) string {
	return fmt.Sprintf("%s/%s/%s", id.Type, id.Name, id.Variant)
}
