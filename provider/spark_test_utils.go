package provider

import (
	"testing"

	fs "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func GetTestingBlobDatabricks(t *testing.T) *SparkOfflineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	azureConfig := &pc.AzureFileStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
		Path:          helpers.GetEnv("AZURE_CONTAINER_PATH", ""),
	}
	return getTestingDatabricks(t, azureConfig, fs.Azure)
}

func GetTestingS3Databricks(t *testing.T) *SparkOfflineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	awsCreds := pc.AWSCredentials{
		// These are not working in CI/CD at the time of this PR, but should be prefered.
		// AWSAccessKeyId: helpers.GetEnv("AWS_ACCESS_KEY_ID", ""),
		// AWSSecretKey:   helpers.GetEnv("AWS_SECRET_ACCESS_KEY", ""),
		AWSAccessKeyId: helpers.GetEnv("DYNAMO_ACCESS_KEY", ""),
		AWSSecretKey:   helpers.GetEnv("DYNAMO_SECRET_KEY", ""),
	}
	s3Config := &pc.S3FileStoreConfig{
		Credentials:  awsCreds,
		BucketRegion: helpers.GetEnv("S3_BUCKET_REGION", "us-east-2"),
		BucketPath:   helpers.GetEnv("S3_BUCKET_PATH", "featureform-spark-testing"),
		Path:         uuid.NewString(),
	}
	t.Logf("S3 CONFIG TestingS3Databricks: %+v\n", s3Config)
	return getTestingDatabricks(t, s3Config, fs.S3)
}

func getTestingDatabricks(t *testing.T, cfg SparkFileStoreConfig, fst fs.FileStoreType) *SparkOfflineStore {
	databricksConfig := pc.DatabricksConfig{
		Username: helpers.GetEnv("DATABRICKS_USERNAME", ""),
		Password: helpers.GetEnv("DATABRICKS_PASSWORD", ""),
		Host:     helpers.GetEnv("DATABRICKS_HOST", ""),
		Token:    helpers.GetEnv("DATABRICKS_TOKEN", ""),
		Cluster:  helpers.GetEnv("DATABRICKS_CLUSTER", ""),
	}
	SparkOfflineConfig := pc.SparkConfig{
		ExecutorType:   pc.Databricks,
		ExecutorConfig: &databricksConfig,
		StoreType:      fst,
		StoreConfig:    cfg,
	}
	sparkSerializedConfig, err := SparkOfflineConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize the SparkOfflineConfig")
	}

	sparkProvider, err := Get(pt.SparkOffline, sparkSerializedConfig)
	if err != nil {
		t.Fatalf("Could not create spark provider: %s", err)
	}
	sparkOfflineStore := sparkProvider.(*SparkOfflineStore)

	return sparkOfflineStore
}
