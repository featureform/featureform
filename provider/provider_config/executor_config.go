package provider_config

import (
	cfg "github.com/featureform/config"
	ss "github.com/featureform/helpers/string_set"
)

type ExecutorConfig struct {
	DefaultProviderConfig
	DockerImage string `json:"docker_image"`
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
