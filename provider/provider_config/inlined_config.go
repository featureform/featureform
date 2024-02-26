package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
	ikv "github.com/inlinedio/ikv-store/ikv-go-client"
)

type IKVConfig struct {
	StoreName string `json:"StoreName"`

	// Account credentials
	AccountId      string `json:"AccountId"`
	AccountPasskey string `json:"AccountPasskey"`

	// Absolute path to mount point
	// for embedded database
	MountDirectory string `json:"MountDirectory"`

	// logging level. values: "error", "info", "warn", "debug", "trace"
	LogLevel string `json:"LogLevel"`

	// path to log file
	// optional, uses stdout/stderr when missing
	LogFilePath string `json:"LogFilePath"`
}

func (ic IKVConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(ic)
	if err != nil {
		panic(err)
	}
	return config
}

func (ic *IKVConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, ic)
	if err != nil {
		return err
	}

	if len(ic.LogLevel) == 0 {
		ic.LogLevel = "info"
	}

	return nil
}

func (a IKVConfig) DifferingFields(b RedisConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}

func (ic *IKVConfig) ToClientOptions() (*ikv.ClientOptions, error) {
	builder := ikv.NewClientOptionsBuilder().WithStoreName(ic.StoreName).WithAccountId(ic.AccountId).WithAccountPasskey(ic.AccountPasskey).WithMountDirectory(ic.MountDirectory)

	// logging options
	if len(ic.LogFilePath) == 0 {
		builder = builder.WithConsoleLogging(ic.LogLevel)
	} else {
		builder = builder.WithFileLogging(ic.LogFilePath, ic.LogLevel)
	}

	options, err := builder.Build()
	if err != nil {
		return nil, err
	}

	return &options, nil
}
