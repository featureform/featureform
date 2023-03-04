package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type DynamodbConfig struct {
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
}

func (d DynamodbConfig) Serialize() ([]byte, error) {
	config, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (d *DynamodbConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, d)
	if err != nil {
		return err
	}
	return nil
}

func (d *DynamodbConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"AccessKey": true,
		"SecretKey": true,
	}
}

func (a *DynamodbConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*DynamodbConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
