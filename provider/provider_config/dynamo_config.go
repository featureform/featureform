package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type DynamodbConfig struct {
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
}

func (d DynamodbConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return config
}

func (d *DynamodbConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, d)
	if err != nil {
		return err
	}
	return nil
}

func (d DynamodbConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Region":    true,
		"AccessKey": true,
		"SecretKey": true,
	}
}

func (a DynamodbConfig) DifferingFields(b DynamodbConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
