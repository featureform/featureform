package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type GCSFileStoreConfig struct {
	DefaultProviderConfig
	BucketName  string
	BucketPath  string
	Credentials GCPCredentials
}

func (s *GCSFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}
