package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type BigQueryConfig struct {
	DefaultProviderConfig
	ProjectId   string
	DatasetId   string
	Credentials map[string]interface{}
}

func (bq *BigQueryConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}
