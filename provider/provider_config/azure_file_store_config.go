package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type AzureFileStoreConfig struct {
	DefaultProviderConfig
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (store *AzureFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"AccountKey": true,
	}
}
