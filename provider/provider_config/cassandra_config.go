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

func (cass CassandraConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(cass)
	if err != nil {
		panic(err)
	}
	return config
}

func (cass *CassandraConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, cass)
	if err != nil {
		return err
	}
	return nil
}

func (cass CassandraConfig) MutableFields() ss.StringSet {
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
