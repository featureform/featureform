package provider_config

import (
	"encoding/json"
	"github.com/featureform/fferr"

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
		return fferr.NewInternalError(err)
	}
	return nil
}
