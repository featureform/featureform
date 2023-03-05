package provider_config

import "encoding/json"

type OnlineBlobConfig struct {
	Type   FileStoreType
	Config AzureFileStoreConfig
}

func (online OnlineBlobConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(online)
	if err != nil {
		panic(err)
	}
	return config
}

func (online *OnlineBlobConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, online)
	if err != nil {
		return err
	}
	return nil
}
