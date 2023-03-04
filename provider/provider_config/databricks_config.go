package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type DatabricksConfig struct {
	Username string
	Password string
	Host     string
	Token    string
	Cluster  string
}

func (d *DatabricksConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, d)
	if err != nil {
		return err
	}
	return nil
}

func (d *DatabricksConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (d *DatabricksConfig) IsExecutorConfig() bool {
	return true
}

func (d *DatabricksConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Token":    true,
	}
}

func (a *DatabricksConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*DatabricksConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
