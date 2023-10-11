package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type MySqlConfig struct {
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
}

func (my *MySqlConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, my)
	if err != nil {
		return err
	}
	return nil
}

func (my *MySqlConfig) Serialize() []byte {
	conf, err := json.Marshal(my)
	if err != nil {
		panic(err)
	}
	return conf
}

func (my MySqlConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Host":     true,
		"Port":     true,
		"Database": true,
	}
}

func (a MySqlConfig) DifferingFields(b MySqlConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
