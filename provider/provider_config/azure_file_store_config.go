package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem FileStoreType = "LOCAL_FILESYSTEM"
	Azure      FileStoreType = "AZURE"
	S3         FileStoreType = "S3"
	GCS        FileStoreType = "GCS"
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

func (pg AzureFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"AccountName": true,
		"AccountKey":  true,
	}
}

func (a AzureFileStoreConfig) DifferingFields(b AzureFileStoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
