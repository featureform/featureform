package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type ClickHouseConfig struct {
	Host     string `json:"Host"`
	Port     uint16 `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	SSL      bool   `json:"SSL"`
}

func (ch *ClickHouseConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, ch)
	if err != nil {
		return err
	}
	return nil
}

func (pg *ClickHouseConfig) Serialize() []byte {
	conf, err := json.Marshal(pg)
	if err != nil {
		panic(err)
	}
	return conf
}

func (ch ClickHouseConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
		"SSL":      true,
	}
}

func (a ClickHouseConfig) DifferingFields(b ClickHouseConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
