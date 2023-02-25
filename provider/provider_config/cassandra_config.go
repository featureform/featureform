package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type CassandraConfig struct {
	Keyspace    string
	Addr        string
	Username    string
	Password    string
	Consistency string
	Replication int
}

func (r CassandraConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *CassandraConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

func (pg CassandraConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":    true,
		"Password":    true,
		"Consistency": true,
		"Replication": true,
	}
}

func (a CassandraConfig) DifferingFields(b CassandraConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
