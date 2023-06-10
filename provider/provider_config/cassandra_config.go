package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type CassandraConfig struct {
	DefaultProviderConfig
	Keyspace    string
	Addr        string
	Username    string
	Password    string
	Consistency string
	Replication int
}

func (cass CassandraConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":    true,
		"Password":    true,
		"Consistency": true,
		"Replication": true,
	}
}
