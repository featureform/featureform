package provider_config

import (
	"encoding/json"

	ss "github.com/featureform/helpers/string_set"
)

type S3FileStoreConfig struct {
	Credentials  AWSCredentials
	BucketRegion string
	BucketPath   string
	Path         string
}

func (s *S3FileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3FileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *S3FileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (s S3FileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a S3FileStoreConfig) DifferingFields(b S3FileStoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}

type HDFSFileStoreConfig struct {
	Host     string
	Port     string
	Path     string
	Username string
}

func (s *HDFSFileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *HDFSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *HDFSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}
