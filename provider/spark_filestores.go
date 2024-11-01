// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

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
	// TODO deprecate both of these.
	SparkConfig() []string
	CredentialsConfig() []string
	Packages() []string
	Type() string // s3, azure_blob_store, google_cloud_storage, hdfs, local
	SparkConfigs() sparkConfigs
	FileStore
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

func (s3 SparkS3FileStore) SparkConfig() []string {
	sparkCfg := []string{
		"--spark_config",
		"\"spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\"",
	}

	// If we're using a service account, we don't need to provide credentials
	// or stipulate the credentials provider
	if staticCreds, ok := s3.Credentials.(pc.AWSStaticCredentials); ok {
		sparkCfg = append(sparkCfg, []string{
			"--spark_config",
			"\"fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\"",
			"--spark_config",
			fmt.Sprintf("\"fs.s3a.access.key=%s\"", staticCreds.AccessKeyId),
			"--spark_config",
			fmt.Sprintf("\"fs.s3a.secret.key=%s\"", staticCreds.SecretKey),
		}...)

		if s3.BucketRegion != "" {
			sparkCfg = append(sparkCfg, []string{
				"--spark_config",
				fmt.Sprintf("\"fs.s3a.endpoint=s3.%s.amazonaws.com\"", s3.BucketRegion),
			}...)
		} else {
			// If the region is not provided, we default to us-east-1
			// This is the default behavior of the Hadoop S3A
			// <property>
			// <name>fs.s3a.endpoint</name>
			// <description>AWS S3 endpoint to connect to. An up-to-date list is
			// 	provided in the AWS Documentation: regions and endpoints. Without this
			// 	property, the standard region (s3.amazonaws.com) is assumed.
			// </description>
			// </property>
			// For more information:
			// https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration
			sparkCfg = append(sparkCfg, []string{
				"--spark_config",
				"\"fs.s3a.endpoint=s3.amazonaws.com\"",
			}...)
		}
	}

	return sparkCfg
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

func (s3 SparkS3FileStore) CredentialsConfig() []string {
	credsCfg := []string{
		"--credential",
		fmt.Sprintf("\"aws_bucket_name=%s\"", s3.Bucket),
		"--credential",
		fmt.Sprintf("\"aws_region=%s\"", s3.BucketRegion),
	}

	if staticCreds, ok := s3.Credentials.(pc.AWSStaticCredentials); ok {
		credsCfg = append(credsCfg, []string{
			"--credential",
			fmt.Sprintf("\"aws_access_key_id=%s\"", staticCreds.AccessKeyId),
			"--credential",
			fmt.Sprintf("\"aws_secret_access_key=%s\"", staticCreds.SecretKey),
		}...)
	} else {
		credsCfg = append(credsCfg, []string{
			"--credential",
			"\"use_service_account=true\"",
		}...)
	}

	return credsCfg
}

func (s3 SparkS3FileStore) Packages() []string {
	return []string{
		"--packages",
		"org.apache.spark:spark-hadoop-cloud_2.12:3.2.0",
		"--exclude-packages",
		"com.google.guava:guava",
	}
}

func (s3 SparkS3FileStore) Type() string {
	return "s3"
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

func (glueS3 SparkGlueS3FileStore) SparkConfig() []string {
	s3Configs := glueS3.SparkS3FileStore.SparkConfig()

	// ff_catalog is the spark session name for the glue catalog
	if glueS3.GlueConfig == nil {
		glueS3.Logger.Error("Glue config is nil")
		return s3Configs
	}

	glueConfigs := make([]string, 0)

	switch glueS3.GlueConfig.TableFormat {
	case pc.Iceberg:
		glueConfigs = []string{
			"--spark_config",
			"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			"--spark_config",
			"spark.sql.catalog.ff_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
			"--spark_config",
			fmt.Sprintf("spark.sql.catalog.ff_catalog.region=%s", glueS3.GlueConfig.Region),
			"--spark_config",
			"spark.sql.catalog.ff_catalog=org.apache.iceberg.spark.SparkCatalog",
			"--spark_config",
			"spark.sql.catalog.ff_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
			"--spark_config",
			fmt.Sprintf("spark.sql.catalog.ff_catalog.warehouse=%s", glueS3.GlueConfig.Warehouse),
		}
	case pc.DeltaLake:
		glueConfigs = []string{
			"--spark_config",
			"spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
			"--spark_config",
			"spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
			"--spark_config",
			"spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
			"--spark_config",
			"spark.sql.catalogImplementation=hive",
		}
	}

	return append(s3Configs, glueConfigs...)
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

func (glueS3 SparkGlueS3FileStore) Packages() []string {
	packages := []string{"org.apache.spark:spark-hadoop-cloud_2.12:3.2.0"}

	if glueS3.GlueConfig.TableFormat == pc.Iceberg {
		packages = append(packages, "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2")
	}
	return []string{
		"--packages",
		strings.Join(packages, ","),
		"--exclude-packages",
		"com.google.guava:guava",
	}
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

func (store SparkAzureFileStore) configString() string {
	return fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net=%s", store.AccountName, store.AccountKey)
}

func (azureStore SparkAzureFileStore) SparkConfigs() sparkConfigs {
	// TODO
	panic("Not implemented")
}

func (azureStore SparkAzureFileStore) SparkConfig() []string {
	return []string{
		"--spark_config",
		fmt.Sprintf("\"%s\"", azureStore.configString()),
	}
}

func (azureStore SparkAzureFileStore) CredentialsConfig() []string {
	return []string{
		"--credential",
		fmt.Sprintf("\"azure_connection_string=%s\"", azureStore.connectionString()),
		"--credential",
		fmt.Sprintf("\"azure_container_name=%s\"", azureStore.containerName()),
	}
}

func (azureStore SparkAzureFileStore) Packages() []string {
	return []string{
		"--packages",
		"\"org.apache.hadoop:hadoop-azure:3.2.0\"",
	}
}

func (azureStore SparkAzureFileStore) Type() string {
	return "azure_blob_store"
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
	// TODO
	panic("Not implemented")
}

func (gcs SparkGCSFileStore) SparkConfig() []string {
	return []string{
		"--spark_config",
		"fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"--spark_config",
		"fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		"--spark_config",
		"fs.gs.auth.service.account.enable=true",
		"--spark_config",
		"fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"--spark_config",
		"fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE",
	}
}

func (gcs SparkGCSFileStore) CredentialsConfig() []string {
	base64Credentials := base64.StdEncoding.EncodeToString(gcs.SerializedCredentials)

	return []string{
		"--credential",
		fmt.Sprintf("\"gcp_project_id=%s\"", gcs.Credentials.ProjectId),
		"--credential",
		fmt.Sprintf("\"gcp_bucket_name=%s\"", gcs.Bucket),
		"--credential",
		fmt.Sprintf("\"gcp_credentials=%s\"", base64Credentials),
	}
}

func (gcs SparkGCSFileStore) Packages() []string {
	return []string{
		"--packages",
		"com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0",
		"--jars",
		"/app/provider/scripts/spark/jars/gcs-connector-hadoop2-2.2.11-shaded.jar",
	}
}

func (gcs SparkGCSFileStore) Type() string {
	return "google_cloud_storage"
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
	// TODO
	panic("Not implemented")
}

func (hdfs SparkHDFSFileStore) SparkConfig() []string {
	return []string{}
}

func (hdfs SparkHDFSFileStore) CredentialsConfig() []string {
	return []string{}
}

func (hdfs SparkHDFSFileStore) Packages() []string {
	return []string{}
}

func (hdfs SparkHDFSFileStore) Type() string {
	return "hdfs"
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
	// TODO
	panic("Not implemented")
}

func (local SparkLocalFileStore) SparkConfig() []string {
	return []string{}
}

func (local SparkLocalFileStore) CredentialsConfig() []string {
	return []string{}
}

func (local SparkLocalFileStore) Packages() []string {
	return []string{}
}

func (local SparkLocalFileStore) Type() string {
	return "local"
}

type SparkFileStoreConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config pc.SerializedConfig) error
	IsFileStoreConfig() bool
}

func ResourcePath(id ResourceID) string {
	return fmt.Sprintf("%s/%s/%s", id.Type, id.Name, id.Variant)
}
