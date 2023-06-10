package provider_config

import ss "github.com/featureform/helpers/string_set"

type OnlineBlobConfig struct {
	DefaultProviderConfig
	Type   FileStoreType
	Config AzureFileStoreConfig
}

func (o OnlineBlobConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Config": true,
	}
}
