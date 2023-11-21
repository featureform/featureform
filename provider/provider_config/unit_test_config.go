package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type UnitTestConfig struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
}

func (pg *UnitTestConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pg)
	if err != nil {
		return err
	}
	return nil
}

func (pg *UnitTestConfig) Serialize() []byte {
	conf, err := json.Marshal(pg)
	if err != nil {
		panic(err)
	}
	return conf
}

func (pg UnitTestConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
	}
}

func (a UnitTestConfig) DifferingFields(b UnitTestConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
