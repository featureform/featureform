package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type FirestoreConfig struct {
	Collection  string
	ProjectID   string
	Credentials map[string]interface{}
}

func (fs FirestoreConfig) Serialize() ([]byte, error) {
	config, err := json.Marshal(fs)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (fs *FirestoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, fs)
	if err != nil {
		return err
	}
	return nil
}

func (fs *FirestoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a *FirestoreConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*FirestoreConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
