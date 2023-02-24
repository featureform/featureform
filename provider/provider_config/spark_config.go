package provider_config

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

type SparkExecutorType string

const (
	EMR          SparkExecutorType = "EMR"
	Databricks   SparkExecutorType = "DATABRICKS"
	SparkGeneric SparkExecutorType = "SPARK"
)

type AWSCredentials struct {
	AWSAccessKeyId string
	AWSSecretKey   string
}

type SparkExecutorConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config SerializedConfig) error
	IsExecutorConfig() bool
}

type SparkFileStoreConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config SerializedConfig) error
	IsFileStoreConfig() bool
}

type SparkConfig struct {
	ExecutorType   SparkExecutorType
	ExecutorConfig SparkExecutorConfig
	StoreType      FileStoreType
	StoreConfig    SparkFileStoreConfig
}

func (s *SparkConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *SparkConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *SparkConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		ExecutorType   SparkExecutorType
		ExecutorConfig map[string]interface{}
		StoreType      FileStoreType
		StoreConfig    map[string]interface{}
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	s.ExecutorType = temp.ExecutorType
	s.StoreType = temp.StoreType

	err = s.decodeExecutor(temp.ExecutorType, temp.ExecutorConfig)
	if err != nil {
		return fmt.Errorf("could not decode executor: %w", err)
	}

	err = s.decodeFileStore(temp.StoreType, temp.StoreConfig)
	if err != nil {
		return fmt.Errorf("could not decode filestore: %w", err)
	}

	return nil
}

func (s *SparkConfig) decodeExecutor(executorType SparkExecutorType, configMap map[string]interface{}) error {
	var executorConfig SparkExecutorConfig
	switch executorType {
	case EMR:
		executorConfig = &EMRConfig{}
	case Databricks:
		executorConfig = &DatabricksConfig{}
	default:
		return fmt.Errorf("the executor type '%s' is not supported ", executorType)
	}

	err := mapstructure.Decode(configMap, executorConfig)
	if err != nil {
		return fmt.Errorf("could not decode executor map: %w", err)
	}
	s.ExecutorConfig = executorConfig
	return nil
}

func (s *SparkConfig) decodeFileStore(fileStoreType FileStoreType, configMap map[string]interface{}) error {
	var fileStoreConfig SparkFileStoreConfig
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
	s.StoreConfig = fileStoreConfig
	return nil
}

type SparkGenericConfig struct {
	Master        string
	DeployMode    string
	PythonVersion string
}

func (sc *SparkGenericConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, sc)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SparkGenericConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(sc)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (sc *SparkGenericConfig) IsExecutorConfig() bool {
	return true
}
