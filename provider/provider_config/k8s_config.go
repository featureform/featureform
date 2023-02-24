package provider_config

import (
	"encoding/json"
	"fmt"
)

type FileStoreConfig []byte

type ExecutorType string

type FileStoreType string

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig interface{}
	StoreType      FileStoreType
	StoreConfig    AzureFileStoreConfig
}

func (config *K8sConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (config *K8sConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize k8s config: %w", err)
	}
	if config.ExecutorConfig == "" {
		config.ExecutorConfig = ExecutorConfig{}
	} else {
		return config.executorConfigFromMap()
	}
	return nil
}

const (
	GoProc ExecutorType = "GO_PROCESS"
	K8s                 = "K8S"
)

func (config *K8sConfig) executorConfigFromMap() error {
	cfgMap, ok := config.ExecutorConfig.(map[string]interface{})
	if !ok {
		return fmt.Errorf("could not get ExecutorConfig values")
	}
	serializedExecutor, err := json.Marshal(cfgMap)
	if err != nil {
		return fmt.Errorf("could not marshal executor config: %w", err)
	}
	excConfig := ExecutorConfig{}
	err = excConfig.Deserialize(serializedExecutor)
	if err != nil {
		return fmt.Errorf("could not deserialize config into ExecutorConfig: %w", err)
	}
	config.ExecutorConfig = excConfig
	return nil
}
