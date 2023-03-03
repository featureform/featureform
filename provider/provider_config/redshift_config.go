package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type RedshiftConfig struct {
	Endpoint string
	Port     string
	Database string
	Username string
	Password string
}

func (rs *RedshiftConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, rs)
	if err != nil {
		return err
	}
	return nil
}

func (rs *RedshiftConfig) Serialize() []byte {
	conf, err := json.Marshal(rs)
	if err != nil {
		panic(err)
	}
	return conf
}

func (rs RedshiftConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
	}
}

func (a RedshiftConfig) DifferingFields(b RedshiftConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
