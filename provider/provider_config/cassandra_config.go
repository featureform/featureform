package provider_config

import "encoding/json"

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
