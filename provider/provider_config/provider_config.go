package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
	si "github.com/featureform/helpers/struct_iterator"
	sm "github.com/featureform/helpers/struct_map"
)

type FileStoreConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config SerializedConfig) error
	IsFileStoreConfig() bool
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

func differingFields(a, b interface{}) (ss.StringSet, error) {
	diff := ss.StringSet{}
	aIter, err := si.NewStructIterator(a)
	if err != nil {
		return nil, err
	}

	bMap, err := sm.NewStructMap(b)

	if err != nil {
		return nil, err
	}

	for aIter.Next() {
		key := aIter.Key()
		aVal := aIter.Value()
		if !bMap.Has(key, aVal) {
			diff[key] = true
		}
	}

	return diff, nil
}
