package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type MongoDBConfig struct {
	Host       string
	Port       string
	Username   string
	Password   string
	Database   string
	Throughput int
}

func (m MongoDBConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return config
}

func (m *MongoDBConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return err
	}
	return nil
}

func (m MongoDBConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":   true,
		"Password":   true,
		"Port":       true,
		"Throughput": true,
	}
}

func (a MongoDBConfig) DifferingFields(b MongoDBConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
