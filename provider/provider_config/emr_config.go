package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type EMRConfig struct {
	Credentials   AWSCredentials
	ClusterRegion string
	ClusterName   string
}

func (e *EMRConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, e)
	if err != nil {
		return err
	}
	return nil
}

func (e *EMRConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (e *EMRConfig) IsExecutorConfig() bool {
	return true
}

func (e *EMRConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a *EMRConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*EMRConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
