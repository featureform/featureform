package provider_config

import (
	"encoding/json"
	"fmt"
)

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem               = "LOCAL_FILESYSTEM"
	Azure                    = "AZURE"
	S3                       = "S3"
	GCS                      = "GCS"
)

type AzureFileStoreConfig struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (config *AzureFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (config *AzureFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *AzureFileStoreConfig) Deserialize(data SerializedConfig) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}
