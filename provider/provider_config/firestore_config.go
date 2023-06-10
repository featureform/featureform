package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type FirestoreConfig struct {
	DefaultProviderConfig
	Collection  string
	ProjectID   string
	Credentials map[string]interface{}
}

func (fs FirestoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}
