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

func (r DynamodbConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *DynamodbConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

func (pg DynamodbConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Region":    true,
		"AccessKey": true,
		"SecretKey": true,
	}
}

func (a DynamodbConfig) DifferingFields(b DynamodbConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
