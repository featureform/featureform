package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type MongoDBConfig struct {
	DefaultProviderConfig
	Host       string
	Port       string
	Username   string
	Password   string
	Database   string
	Throughput int
}

func (m MongoDBConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":   true,
		"Password":   true,
		"Port":       true,
		"Throughput": true,
	}
}
