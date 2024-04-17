package provider

import (
	fs "github.com/featureform/filestore"
	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	"github.com/joho/godotenv"
	"testing"
)

func GetTestingDatabricksOfflineStore(t *testing.T) *SparkOfflineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	databricksConfig := pc.DatabricksConfig{
		Username: helpers.GetEnv("DATABRICKS_USERNAME", ""),
		Password: helpers.GetEnv("DATABRICKS_PASSWORD", ""),
		Host:     helpers.GetEnv("DATABRICKS_HOST", ""),
		Token:    helpers.GetEnv("DATABRICKS_TOKEN", ""),
		Cluster:  helpers.GetEnv("DATABRICKS_CLUSTER", ""),
	}
	azureConfig := pc.AzureFileStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
		Path:          helpers.GetEnv("AZURE_CONTAINER_PATH", ""),
	}
	SparkOfflineConfig := pc.SparkConfig{
		ExecutorType:   pc.Databricks,
		ExecutorConfig: &databricksConfig,
		StoreType:      fs.Azure,
		StoreConfig:    &azureConfig,
	}

	sparkSerializedConfig, err := SparkOfflineConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize the SparkOfflineConfig")
	}

	sparkProvider, err := Get("SPARK_OFFLINE", sparkSerializedConfig)
	if err != nil {
		t.Fatalf("Could not create spark provider: %s", err)
	}
	sparkStore, err := sparkProvider.AsOfflineStore()
	if err != nil {
		t.Fatalf("Could not convert spark provider to offline store: %s", err)
	}
	sparkOfflineStore := sparkStore.(*SparkOfflineStore)

	return sparkOfflineStore
}
