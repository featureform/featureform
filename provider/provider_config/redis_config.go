package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type RedisConfig struct {
	Prefix   string
	Addr     string
	Password string
	DB       int
}

func (r RedisConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *RedisConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

func (r RedisConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Password": true,
	}
}

func (a RedisConfig) DifferingFields(b RedisConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
