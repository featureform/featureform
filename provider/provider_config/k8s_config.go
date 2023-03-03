package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig interface{}
	StoreType      FileStoreType
	StoreConfig    AzureFileStoreConfig
}

func (k8s *K8sConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(k8s)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (k8s *K8sConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, k8s)
	if err != nil {
		return fmt.Errorf("deserialize k8s config: %w", err)
	}
	if k8s.ExecutorConfig == "" {
		k8s.ExecutorConfig = ExecutorConfig{}
	} else {
		return k8s.executorConfigFromMap()
	}
	return nil
}

func (k8s K8sConfig) MutableFields() ss.StringSet {
	result := ss.StringSet{
		"ExecutorConfig": true,
	}
	storeFields := k8s.StoreConfig.MutableFields()

	for field, val := range storeFields {
		result["Store."+field] = val
	}

	return result
}

func (a K8sConfig) DifferingFields(b K8sConfig) (ss.StringSet, error) {
	result := ss.StringSet{}

	if a.StoreType != b.StoreType {
		return result, fmt.Errorf("store config mismatch: a = %v; b = %v", a.StoreType, b.StoreType)
	}

	executorFields, err := differingFields(a.ExecutorConfig, b.ExecutorConfig)
	if err != nil {
		return result, err
	}
	storeFields, err := a.StoreConfig.DifferingFields(b.StoreConfig)
	if err != nil {
		return result, err
	}

	if len(executorFields) > 0 {
		result["ExecutorConfig"] = true
	}

	for field, val := range storeFields {
		result["Store."+field] = val
	}

	return result, err
}

const (
	GoProc ExecutorType = "GO_PROCESS"
	K8s    ExecutorType = "K8S"
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
