package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type MSSQLConfig struct {
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
}

func (ms *MSSQLConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, ms)
	if err != nil {
		return err
	}
	return nil
}

func (ms *MSSQLConfig) Serialize() []byte {
	conf, err := json.Marshal(ms)
	if err != nil {
		panic(err)
	}
	return conf
}

func (ms MSSQLConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Host":     true,
		"Port":     true,
		"Database": true,
	}
}

func (a MSSQLConfig) DifferingFields(b MSSQLConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
