package provider_config

import (
	"encoding/json"
	"strings"

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
	// optional, info when missing
	LogLevel string `json:"LogLevel"`

	// path to log file
	// optional, uses stdout/stderr when missing
	LogFilePath string `json:"LogFilePath"`
}

func (ic IKVConfig) Serialized() SerializedConfig {
	ic.LogLevel = coerceValidLogLevel(ic.LogLevel)

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

	ic.LogLevel = coerceValidLogLevel(ic.LogLevel)

	return nil
}

func (ic IKVConfig) MutableFields() ss.StringSet {
	return ss.StringSet{}
}

func (a IKVConfig) DifferingFields(b IKVConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}

func (ic *IKVConfig) ToClientOptions() (*ikv.ClientOptions, error) {
	builder := ikv.NewClientOptionsBuilder().WithStoreName(ic.StoreName).WithAccountId(ic.AccountId).WithAccountPasskey(ic.AccountPasskey).WithMountDirectory(ic.MountDirectory)

	// logging options
	if len(ic.LogFilePath) == 0 {
		builder = builder.WithConsoleLogging(coerceValidLogLevel(ic.LogLevel))
	} else {
		builder = builder.WithFileLogging(ic.LogFilePath, coerceValidLogLevel(ic.LogLevel))
	}

	options, err := builder.Build()
	if err != nil {
		return nil, err
	}

	return &options, nil
}

// valid logging levels
var loglevels = map[string]interface{}{
	"error": 0,
	"warn":  0,
	"info":  0,
	"debug": 0,
	"trace": 0,
}

func coerceValidLogLevel(level string) string {
	if len(level) == 0 {
		return "info"
	}

	valid_level := strings.ToLower(level)

	if _, exists := loglevels[valid_level]; !exists {
		return "info"
	}

	return valid_level
}
