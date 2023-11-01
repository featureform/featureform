package provider_config

import (
	"encoding/json"

	fs "github.com/featureform/filestore"
)

type OnlineBlobConfig struct {
	Type   fs.FileStoreType
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
