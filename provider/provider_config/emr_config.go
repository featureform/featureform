package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type EMRConfig struct {
	DefaultProviderConfig
	Credentials   AWSCredentials
	ClusterRegion string
	ClusterName   string
}

func (e *EMRConfig) IsExecutorConfig() bool {
	return true
}

func (e EMRConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}
