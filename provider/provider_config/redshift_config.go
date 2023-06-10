package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type RedshiftConfig struct {
	DefaultProviderConfig
	Endpoint string
	Port     string
	Database string
	Username string
	Password string
}

func (rs RedshiftConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
	}
}
