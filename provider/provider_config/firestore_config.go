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

func (fs FirestoreConfig) Serialize() SerializedConfig {
	config, err := json.Marshal(fs)
	if err != nil {
		panic(err)
	}
	return config
}

func (fs *FirestoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, fs)
	if err != nil {
		return err
	}
	return nil
}

func (fs FirestoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a FirestoreConfig) DifferingFields(b FirestoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
