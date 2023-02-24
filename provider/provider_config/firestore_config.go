package provider_config

import "encoding/json"

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
