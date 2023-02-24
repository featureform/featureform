package provider_config

import "encoding/json"

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
