package provider_config

import (
	"encoding/json"
	"fmt"

	cfg "github.com/featureform/config"
	ss "github.com/featureform/helpers/string_set"
)

type ExecutorConfig struct {
	DockerImage string `json:"docker_image"`
}

func (c *ExecutorConfig) Serialize() ([]byte, error) {
	serialized, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("could not serialize K8s Config: %w", err)
	}
	return serialized, nil
}

func (c *ExecutorConfig) Deserialize(config []byte) error {
	err := json.Unmarshal(config, &c)
	if err != nil {
		return fmt.Errorf("could not deserialize K8s Executor Config: %w", err)
	}
	return nil
}

func (c *ExecutorConfig) GetImage() string {
	if c.DockerImage == "" {
		return cfg.GetPandasRunnerImage()
	} else {
		return c.DockerImage
	}
}

func (c ExecutorConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"DockerImage": true,
	}
}

func (a ExecutorConfig) DifferingFields(b ExecutorConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
