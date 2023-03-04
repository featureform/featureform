package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type BigQueryConfig struct {
	ProjectId   string
	DatasetId   string
	Credentials map[string]interface{}
}

func (bq *BigQueryConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, bq)
	if err != nil {
		return err
	}
	return nil
}

func (bq *BigQueryConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(bq)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (bq *BigQueryConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a *BigQueryConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*BigQueryConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
