package provider_config

import (
	"encoding/json"
	ss "github.com/featureform/helpers/string_set"
	si "github.com/featureform/helpers/struct_iterator"
	sm "github.com/featureform/helpers/struct_map"
)

type ProviderConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config SerializedConfig) error
	DifferingFields(other ProviderConfig) (ss.StringSet, error)
	MutableFields() ss.StringSet
}

type ExecutorType string

type FileStoreType string

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem FileStoreType = "LOCAL_FILESYSTEM"
	Azure      FileStoreType = "AZURE"
	S3         FileStoreType = "S3"
	GCS        FileStoreType = "GCS"
	DB         FileStoreType = "db"
	HDFS       FileStoreType = "HDFS"
)

type SerializedConfig []byte

type DefaultProviderConfig struct{}

func (config *DefaultProviderConfig) Serialize() ([]byte, error) {
	return json.Marshal(config)
}

func (config *DefaultProviderConfig) Deserialize(serialized SerializedConfig) error {
	err := json.Unmarshal(serialized, config)
	if err != nil {
		return err
	}
	return nil
}

func (config *DefaultProviderConfig) DifferingFields(other ProviderConfig) (ss.StringSet, error) {
	diff := ss.StringSet{}
	thisIter, err := si.NewStructIterator(config)
	if err != nil {
		return nil, err
	}

	otherMap, err := sm.NewStructMap(other)

	if err != nil {
		return nil, err
	}

	for thisIter.Next() {
		key := thisIter.Key()
		aVal := thisIter.Value()
		if !otherMap.Has(key, aVal) {
			diff[key] = true
		}
	}

	return diff, nil
}
