package provider_config

import (
	"encoding/json"

	"github.com/featureform/fferr"

	ss "github.com/featureform/helpers/string_set"
)

type QdrantConfig struct {
	GrpcHost string
	ApiKey   string
	UseTls   bool
}

func (pc QdrantConfig) Serialize() SerializedConfig {
	config, err := json.Marshal(pc)
	if err != nil {
		panic(err)
	}
	return config
}

func (pc *QdrantConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pc)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (pc QdrantConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"ApiKey": true,
	}
}

func (a QdrantConfig) DifferingFields(b QdrantConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
