package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/featureform/provider"
	"gocloud.dev/blob/azureblob"
	"os"
)

type Config []byte

type AzureFileStoreConfig struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (config *AzureFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *AzureFileStoreConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

type AzureFileStore struct {
	AccountName      string
	AccountKey       string
	ConnectionString string
	ContainerName    string
	Path             string
	GenericFileStore
}

func (azure *AzureFileStore) ConfigString() string {
	return fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net=%s", azure.AccountName, azure.AccountKey)
}
func (azure *AzureFileStore) GetConnectionString() string {
	return azure.ConnectionString
}
func (azure *AzureFileStore) GetContainerName() string {
	return azure.ContainerName
}

func (azure *AzureFileStore) AddAzureVars(envVars map[string]string) map[string]string {
	envVars["AZURE_CONNECTION_STRING"] = azure.ConnectionString
	envVars["AZURE_CONTAINER_NAME"] = azure.ContainerName
	return envVars
}

func (azure AzureFileStore) AsAzureStore() *AzureFileStore {
	return &azure
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

func NewAzureFileStore(config provider.Config) (FileStore, error) {
	azureStoreConfig := AzureFileStoreConfig{}
	if err := azureStoreConfig.Deserialize(config); err != nil {
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
		return AzureFileStore{}, fmt.Errorf("Could not create azure client: %v", err)
	}

	bucket, err := azureblob.OpenBucket(context.TODO(), client, azureStoreConfig.ContainerName, nil)
	if err != nil {
		return AzureFileStore{}, fmt.Errorf("Could not open azure bucket: %v", err)
	}
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", azureStoreConfig.AccountName, azureStoreConfig.AccountKey)
	return AzureFileStore{
		AccountName:      azureStoreConfig.AccountName,
		AccountKey:       azureStoreConfig.AccountKey,
		ConnectionString: connectionString,
		ContainerName:    azureStoreConfig.ContainerName,
		Path:             azureStoreConfig.Path,
		GenericFileStore: GenericFileStore{
			bucket: bucket,
		},
	}, nil
}
