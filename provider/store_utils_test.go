package provider

import (
	"encoding/json"
	pc "github.com/featureform/provider/provider_config"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

func getLocalFileStore(t *testing.T) FileStore {
	config := pc.LocalFileStoreConfig{
		DirPath: "file:///tmp/",
	}

	serializedConfig, err := config.Serialize()
	if err != nil {
		t.Fatalf("could not serialize local config: %s", err)
	}
	fileStore, err := NewLocalFileStore(serializedConfig)
	if err != nil {
		t.Fatalf("could not initialize local filestore: %s", err)
	}

	return fileStore
}

func getS3FileStore(t *testing.T, loadEnv bool) FileStore {
	if loadEnv {
		// We don't want to fail this in CI/CD
		_ = godotenv.Load()
	}
	s3Config := pc.S3FileStoreConfig{
		Credentials: pc.AWSCredentials{
			AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
			AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
		},
		BucketRegion: os.Getenv("S3_BUCKET_REGION"),
		BucketPath:   os.Getenv("S3_BUCKET_PATH"),
		Path:         "featureform-unit-test",
	}

	serializedConfig, err := s3Config.Serialize()
	if err != nil {
		t.Fatalf("could not serialize s3 config: %s", err)
	}
	fileStore, err := NewS3FileStore(serializedConfig)
	if err != nil {
		t.Fatalf("could not initialize s3 filestore: %s", err)
	}

	return fileStore
}

func getHDFSFileStore(t *testing.T, loadEnv bool) FileStore {
	if loadEnv {
		// We don't want to fail this in CI/CD
		_ = godotenv.Load()
	}
	hdfsConfig := pc.HDFSFileStoreConfig{
		Host:     "localhost",
		Port:     "9000",
		Username: "hduser",
		Path:     "/tmp/",
	}

	serializedConfig, err := hdfsConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize hdfs config: %s", err)
	}
	fileStore, err := NewHDFSFileStore(serializedConfig)
	if err != nil {
		t.Fatalf("could not initialize hdfs filestore: %s", err)
	}

	return fileStore
}

func getAzureFileStore(t *testing.T, loadEnv bool) FileStore {
	if loadEnv {
		// We don't want to fail this in CI/CD
		_ = godotenv.Load()
	}
	azureConfig := &pc.AzureFileStoreConfig{
		AccountName:   os.Getenv("AZURE_ACCOUNT_NAME"),
		AccountKey:    os.Getenv("AZURE_ACCOUNT_KEY"),
		ContainerName: os.Getenv("AZURE_CONTAINER_NAME"),
		Path:          "featureform-unit-test",
	}

	serializedConfig, err := azureConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize azure config: %s", err)
	}
	fileStore, err := NewAzureFileStore(serializedConfig)
	if err != nil {
		t.Fatalf("could not initialize azure filestore: %s", err)
	}

	return fileStore
}

func getGCSFileStore(t *testing.T, loadEnv bool) FileStore {
	if loadEnv {
		// We don't want to fail this in CI/CD
		_ = godotenv.Load()
	}
	credsFile := os.Getenv("GCP_CREDENTIALS_FILE")
	content, err := os.ReadFile(credsFile)
	if err != nil {
		t.Fatalf("Error when opening file: %v", err)
	}

	var creds map[string]interface{}
	err = json.Unmarshal(content, &creds)
	if err != nil {
		t.Fatalf("Error during Unmarshal() creds: %v", err)
	}

	gcsConfig := pc.GCSFileStoreConfig{
		BucketName: os.Getenv("GCS_BUCKET_NAME"),
		BucketPath: "featureform-unit-test",
		Credentials: pc.GCPCredentials{
			ProjectId: os.Getenv("GCP_PROJECT_ID"),
			JSON:      creds,
		},
	}

	serializedConfig, err := gcsConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize gcs config: %v", err)
	}
	fileStore, err := NewGCSFileStore(serializedConfig)
	if err != nil {
		t.Fatalf("could not initialize gcs filestore: %s", err)
	}

	return fileStore
}
