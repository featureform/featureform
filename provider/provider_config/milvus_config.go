package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type MilvusConfig struct {
	Address     string
	Port        int32
	ServiceName string
	ApiKey      string
}

func (mc MilvusConfig) Serialize() SerializedConfig {
	config, err := json.Marshal(mc)
	if err != nil {
		panic(err)
	}
	return config
}

func (pc *MilvusConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pc)
	if err != nil {
		return err
	}
	return nil
}

func (pc MilvusConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Address":     false,
		"Port":        false,
		"ServiceName": false,
		"ApiKey":      true,
	}
}

func (a MilvusConfig) DifferingFields(b MilvusConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
