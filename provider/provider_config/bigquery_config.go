package provider_config

import "encoding/json"

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
