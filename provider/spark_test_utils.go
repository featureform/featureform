package provider

import (
	fs "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"testing"
)

func GetTestingBlobDatabrick(t *testing.T) *SparkOfflineStore {
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
	return getTestingDatabricks(t, azureConfig)
}

func GetTestingS3Databricks(t *testing.T) *SparkOfflineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	awsCreds := pc.AWSCredentials{
		AWSAccessKeyId: helpers.GetEnv("AWS_ACCESS_KEY_ID", ""),
		AWSSecretKey:   helpers.GetEnv("AWS_SECRET_ACCESS_KEY", ""),
	}
	s3Config := &pc.S3FileStoreConfig{
		Credentials:  awsCreds,
		BucketRegion: helpers.GetEnv("S3_BUCKET_REGION", ""),
		BucketPath:   helpers.GetEnv("S3_BUCKET_PATH", ""),
	}
	return getTestingDatabricks(t, s3Config)

}

func getTestingDatabricks(t *testing.T, cfg SparkFileStoreConfig) *SparkOfflineStore {
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
		StoreType:      fs.S3,
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
