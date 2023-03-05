package provider_config

import (
	"encoding/json"

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

func (bq *BigQueryConfig) Serialize() []byte {
	conf, err := json.Marshal(bq)
	if err != nil {
		panic(err)
	}
	return conf
}

func (bq BigQueryConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a BigQueryConfig) DifferingFields(b BigQueryConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
