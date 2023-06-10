package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type DatabricksConfig struct {
	DefaultProviderConfig
	Username string
	Password string
	Host     string
	Token    string
	Cluster  string
}

func (d *DatabricksConfig) IsExecutorConfig() bool {
	return true
}

func (d DatabricksConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Token":    true,
	}
}
