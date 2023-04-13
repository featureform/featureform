package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
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

type GCPCredentials struct {
	ProjectId string
	JSON      map[string]interface{}
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

func (s SparkConfig) MutableFields() ss.StringSet {
	result := ss.StringSet{}
	var executorFields ss.StringSet
	var storeFields ss.StringSet

	switch s.ExecutorType {
	case EMR:
		executorFields = s.ExecutorConfig.(*EMRConfig).MutableFields()
	case Databricks:
		executorFields = s.ExecutorConfig.(*DatabricksConfig).MutableFields()
	case SparkGeneric:
		executorFields = s.ExecutorConfig.(*SparkGenericConfig).MutableFields()
	default:
		executorFields = ss.StringSet{}
	}

	switch s.StoreType {
	case Azure:
		storeFields = s.StoreConfig.(*AzureFileStoreConfig).MutableFields()
	case S3:
		storeFields = s.StoreConfig.(*S3FileStoreConfig).MutableFields()
	case GCS:
		storeFields = s.StoreConfig.(*GCSFileStoreConfig).MutableFields()
	default:
		storeFields = ss.StringSet{}
	}

	for field, val := range executorFields {
		result["Executor."+field] = val
	}

	for field, val := range storeFields {
		result["Store."+field] = val
	}

	return result
}

func (a SparkConfig) DifferingFields(b SparkConfig) (ss.StringSet, error) {
	result := ss.StringSet{}
	var executorFields ss.StringSet
	var storeFields ss.StringSet
	var err error

	if a.ExecutorType != b.ExecutorType {
		return result, fmt.Errorf("executor config mismatch: a = %v; b = %v", a.ExecutorType, b.ExecutorType)
	}

	if a.StoreType != b.StoreType {
		return result, fmt.Errorf("store config mismatch: a = %v; b = %v", a.StoreType, b.StoreType)
	}

	switch a.ExecutorType {
	case EMR:
		executorFields, err = a.ExecutorConfig.(*EMRConfig).DifferingFields(*b.ExecutorConfig.(*EMRConfig))
	case Databricks:
		executorFields, err = a.ExecutorConfig.(*DatabricksConfig).DifferingFields(*b.ExecutorConfig.(*DatabricksConfig))
	default:
		return nil, fmt.Errorf("unknown executor type: %v", a.ExecutorType)
	}

	if err != nil {
		return result, err
	}

	switch a.StoreType {
	case Azure:
		storeFields, err = a.StoreConfig.(*AzureFileStoreConfig).DifferingFields(*b.StoreConfig.(*AzureFileStoreConfig))
	case S3:
		storeFields, err = a.StoreConfig.(*S3FileStoreConfig).DifferingFields(*b.StoreConfig.(*S3FileStoreConfig))
	case GCS:
		storeFields, err = a.StoreConfig.(*GCSFileStoreConfig).DifferingFields(*b.StoreConfig.(*GCSFileStoreConfig))
	default:
		return nil, fmt.Errorf("unknown store type: %v", a.StoreType)
	}

	if err != nil {
		return result, err
	}

	for field, val := range executorFields {
		result["Executor."+field] = val
	}

	for field, val := range storeFields {
		result["Store."+field] = val
	}

	return result, err
}

func (s *SparkConfig) decodeExecutor(executorType SparkExecutorType, configMap map[string]interface{}) error {
	var executorConfig SparkExecutorConfig
	switch executorType {
	case EMR:
		executorConfig = &EMRConfig{}
	case Databricks:
		executorConfig = &DatabricksConfig{}
	case SparkGeneric:
		executorConfig = &SparkGenericConfig{}
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
	case HDFS:
		fileStoreConfig = &HDFSFileStoreConfig{}
	case GCS:
		fileStoreConfig = &GCSFileStoreConfig{}
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
	CoreSite      string
	YarnSite      string
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

func (sc SparkGenericConfig) MutableFields() ss.StringSet {
	// Generic Spark config is not open to update once registered
	return ss.StringSet{}
}

func (a SparkGenericConfig) DifferingFields(b SparkGenericConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
