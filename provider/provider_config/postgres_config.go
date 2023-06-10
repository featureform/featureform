package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type PostgresConfig struct {
	DefaultProviderConfig
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
}

func (pg *PostgresConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
	}
}
