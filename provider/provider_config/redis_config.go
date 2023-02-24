package provider_config

import "encoding/json"

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
