package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type FirestoreConfig struct {
	Collection  string
	ProjectID   string
	Credentials map[string]interface{}
}

func (r FirestoreConfig) Serialize() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *FirestoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

func (pg FirestoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a FirestoreConfig) DifferingFields(b FirestoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
