package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
	"github.com/mitchellh/mapstructure"
)

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig interface{}
	StoreType      FileStoreType
	StoreConfig    FileStoreConfig
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
	return nil
}

func (k8s *K8sConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		ExecutorType   ExecutorType
		ExecutorConfig map[string]interface{}
		StoreType      FileStoreType
		StoreConfig    map[string]interface{}
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	k8s.ExecutorType = temp.ExecutorType
	k8s.StoreType = temp.StoreType

	err = k8s.decodeExecutor(temp.ExecutorType, temp.ExecutorConfig)
	if err != nil {
		return fmt.Errorf("could not decode executor: %w", err)
	}

	err = k8s.decodeFileStore(temp.StoreType, temp.StoreConfig)
	if err != nil {
		return fmt.Errorf("could not decode filestore: %w", err)
	}

	return nil
}

func (k8s *K8sConfig) decodeExecutor(executorType ExecutorType, configMap map[string]interface{}) error {
	serializedExecutor, err := json.Marshal(configMap)
	if err != nil {
		return fmt.Errorf("could not marshal executor config: %w", err)
	}
	excConfig := ExecutorConfig{}
	err = excConfig.Deserialize(serializedExecutor)
	if err != nil {
		return fmt.Errorf("could not deserialize config into ExecutorConfig: %w", err)
	}
	k8s.ExecutorConfig = excConfig
	return nil
}

func (k8s *K8sConfig) decodeFileStore(fileStoreType FileStoreType, configMap map[string]interface{}) error {
	var fileStoreConfig FileStoreConfig
	switch fileStoreType {
	case Azure:
		fileStoreConfig = &AzureFileStoreConfig{}
	case S3:
		fileStoreConfig = &S3FileStoreConfig{}
	default:
		return fmt.Errorf("the file store type '%s' is not supported ", fileStoreType)
	}

	err := mapstructure.Decode(configMap, fileStoreConfig)
	if err != nil {
		return fmt.Errorf("could not decode file store map: %w", err)
	}
	k8s.StoreConfig = fileStoreConfig
	return nil
}

func (k8s K8sConfig) MutableFields() ss.StringSet {
	result := ss.StringSet{
		"ExecutorConfig": true,
	}

	var storeFields ss.StringSet
	switch k8s.StoreType {
	case Azure:
		storeFields = k8s.StoreConfig.(*AzureFileStoreConfig).MutableFields()
	case S3:
		storeFields = k8s.StoreConfig.(*S3FileStoreConfig).MutableFields()
	}

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

	if len(executorFields) > 0 {
		result["ExecutorConfig"] = true
	}

	var storeFields ss.StringSet
	switch a.StoreType {
	case Azure:
		storeFields, err = a.StoreConfig.(*AzureFileStoreConfig).DifferingFields(*b.StoreConfig.(*AzureFileStoreConfig))
	case S3:
		storeFields, err = a.StoreConfig.(*S3FileStoreConfig).DifferingFields(*b.StoreConfig.(*S3FileStoreConfig))
	default:
		return nil, fmt.Errorf("unsupported store type: %v", a.StoreType)
	}

	if err != nil {
		return result, err
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
