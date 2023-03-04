package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type AzureFileStoreConfig struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (store *AzureFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (store *AzureFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(store)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (store *AzureFileStoreConfig) Deserialize(data SerializedConfig) error {
	err := json.Unmarshal(data, store)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

func (store *AzureFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"AccountKey": true,
	}
}

func (a *AzureFileStoreConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*AzureFileStoreConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
