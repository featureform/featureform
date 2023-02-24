package provider_config

import "encoding/json"

type MongoDBConfig struct {
	Host       string
	Port       string
	Username   string
	Password   string
	Database   string
	Throughput int
}

func (r MongoDBConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *MongoDBConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}
