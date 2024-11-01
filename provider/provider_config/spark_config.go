// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"encoding/json"
	"fmt"

	"github.com/featureform/fferr"

	fs "github.com/featureform/filestore"
	ss "github.com/featureform/helpers/stringset"
	"github.com/mitchellh/mapstructure"
)

type SparkExecutorType string
type TableFormat string

const (
	EMR          SparkExecutorType = "EMR"
	Databricks   SparkExecutorType = "DATABRICKS"
	SparkGeneric SparkExecutorType = "SPARK"
	Iceberg      TableFormat       = "iceberg"
	DeltaLake    TableFormat       = "delta"
)

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

type GlueConfig struct {
	Database      string
	Warehouse     string
	Region        string
	AssumeRoleArn string
	TableFormat   TableFormat
}

type SparkFlags struct {
	SparkParams     map[string]string `json:"SparkParams"`
	WriteOptions    map[string]string `json:"WriteOptions"`
	TableProperties map[string]string `json:"TableProperties"`
}

type SparkConfig struct {
	ExecutorType   SparkExecutorType
	ExecutorConfig SparkExecutorConfig
	StoreType      fs.FileStoreType
	StoreConfig    SparkFileStoreConfig
	GlueConfig     *GlueConfig // GlueConfig is optional
}

type sparkConfigTemp struct {
	ExecutorType   SparkExecutorType
	ExecutorConfig json.RawMessage
	StoreType      fs.FileStoreType
	StoreConfig    json.RawMessage
	GlueConfig     *GlueConfig
}

func (s *SparkConfig) Deserialize(config SerializedConfig) error {
	temp := sparkConfigTemp{}
	err := json.Unmarshal(config, &temp)
	if err != nil {
		return err
	}

	s.ExecutorType = temp.ExecutorType
	s.StoreType = temp.StoreType
	s.GlueConfig = temp.GlueConfig

	execData, err := json.Marshal(temp.ExecutorConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	switch s.ExecutorType {
	case EMR:
		s.ExecutorConfig = &EMRConfig{}
	case Databricks:
		s.ExecutorConfig = &DatabricksConfig{}
	case SparkGeneric:
		s.ExecutorConfig = &SparkGenericConfig{}
	default:
	}

	if err := s.ExecutorConfig.Deserialize(execData); err != nil {
		return err
	}

	storeData, err := json.Marshal(temp.StoreConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	switch s.StoreType {
	case fs.Azure:
		s.StoreConfig = &AzureFileStoreConfig{}
	case fs.S3:
		s.StoreConfig = &S3FileStoreConfig{}
	case fs.GCS:
		s.StoreConfig = &GCSFileStoreConfig{}
	case fs.HDFS:
		s.StoreConfig = &HDFSFileStoreConfig{}
	default:
	}

	if err := s.StoreConfig.Deserialize(storeData); err != nil {
		return err
	}

	return nil
}

func (s *SparkConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (s *SparkConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		ExecutorType   SparkExecutorType
		ExecutorConfig map[string]interface{}
		StoreType      fs.FileStoreType
		StoreConfig    map[string]interface{}
		GlueConfig     *GlueConfig
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	s.ExecutorType = temp.ExecutorType
	s.StoreType = temp.StoreType
	s.GlueConfig = temp.GlueConfig

	err = s.decodeExecutor(temp.ExecutorType, temp.ExecutorConfig)
	if err != nil {
		return err
	}

	err = s.decodeFileStore(temp.StoreType, temp.StoreConfig)
	if err != nil {
		return err
	}

	if temp.GlueConfig != nil {
		err = s.decodeGlueConfig(temp.GlueConfig)
		if err != nil {
			return err
		}
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
	case fs.Azure:
		storeFields = s.StoreConfig.(*AzureFileStoreConfig).MutableFields()
	case fs.S3:
		storeFields = s.StoreConfig.(*S3FileStoreConfig).MutableFields()
	case fs.GCS:
		storeFields = s.StoreConfig.(*GCSFileStoreConfig).MutableFields()
	case fs.HDFS:
		storeFields = s.StoreConfig.(*HDFSFileStoreConfig).MutableFields()
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
		return result, fferr.NewInternalError(
			fmt.Errorf(
				"executor config mismatch: a = %v; b = %v",
				a.ExecutorType,
				b.ExecutorType,
			),
		)
	}

	if a.StoreType != b.StoreType {
		return result, fferr.NewInternalError(
			fmt.Errorf(
				"store config mismatch: a = %v; b = %v",
				a.StoreType,
				b.StoreType,
			),
		)
	}

	switch a.ExecutorType {
	case EMR:
		executorFields, err = a.ExecutorConfig.(*EMRConfig).DifferingFields(*b.ExecutorConfig.(*EMRConfig))
	case Databricks:
		executorFields, err = a.ExecutorConfig.(*DatabricksConfig).DifferingFields(*b.ExecutorConfig.(*DatabricksConfig))
	case SparkGeneric:
		executorFields, err = a.ExecutorConfig.(*SparkGenericConfig).DifferingFields(*b.ExecutorConfig.(*SparkGenericConfig))
	default:
		return nil, fferr.NewProviderConfigError("Spark", fmt.Errorf("unknown executor type: %v", a.ExecutorType))
	}

	if err != nil {
		return result, err
	}

	switch a.StoreType {
	case fs.Azure:
		storeFields, err = a.StoreConfig.(*AzureFileStoreConfig).DifferingFields(*b.StoreConfig.(*AzureFileStoreConfig))
	case fs.S3:
		storeFields, err = a.StoreConfig.(*S3FileStoreConfig).DifferingFields(*b.StoreConfig.(*S3FileStoreConfig))
	case fs.GCS:
		storeFields, err = a.StoreConfig.(*GCSFileStoreConfig).DifferingFields(*b.StoreConfig.(*GCSFileStoreConfig))
	case fs.HDFS:
		storeFields, err = a.StoreConfig.(*HDFSFileStoreConfig).DifferingFields(*b.StoreConfig.(*HDFSFileStoreConfig))
	default:
		return nil, fferr.NewProviderConfigError("Spark", fmt.Errorf("unknown store type: %v", a.StoreType))
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

func (a SparkConfig) UsesCatalog() bool {
	return a.GlueConfig != nil
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
		return fferr.NewProviderConfigError(
			"Spark",
			fmt.Errorf("the executor type '%s' is not supported ", executorType),
		)
	}

	err := mapstructure.Decode(configMap, executorConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	s.ExecutorConfig = executorConfig
	return nil
}

func (s *SparkConfig) decodeFileStore(fileStoreType fs.FileStoreType, configMap map[string]interface{}) error {
	var fileStoreConfig SparkFileStoreConfig
	switch fileStoreType {
	case fs.Azure:
		fileStoreConfig = &AzureFileStoreConfig{}
	case fs.S3:
		fileStoreConfig = &S3FileStoreConfig{}
	case fs.HDFS:
		fileStoreConfig = &HDFSFileStoreConfig{}
	case fs.GCS:
		fileStoreConfig = &GCSFileStoreConfig{}
	default:
		return fferr.NewProviderConfigError(
			"Spark",
			fmt.Errorf("the file store type '%s' is not supported ", fileStoreType),
		)
	}

	err := mapstructure.Decode(configMap, fileStoreConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	s.StoreConfig = fileStoreConfig
	return nil
}

func (s *SparkConfig) decodeGlueConfig(configMap *GlueConfig) error {
	var glueConfig GlueConfig
	err := mapstructure.Decode(configMap, &glueConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	s.GlueConfig = &glueConfig
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
