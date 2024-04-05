package provider_config

import (
	"encoding/json"
	"github.com/featureform/fferr"

	ss "github.com/featureform/helpers/string_set"
)

type S3FileStoreConfig struct {
	Credentials  AWSCredentials
	BucketRegion string
	// BucketPath is the bucket name, no s3://
	BucketPath   string
	// Path is the subpath in the bucket to work in
	Path         string
	// Endpoint is used when using a S3 compatible service outside of AWS like localstack
	Endpoint     string
}

func (s *S3FileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (s *S3FileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
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
		return fferr.NewInternalError(err)
	}
	return nil
}

func (s *HDFSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (s *HDFSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (s HDFSFileStoreConfig) DifferingFields(b HDFSFileStoreConfig) (ss.StringSet, error) {
	return differingFields(s, b)
}
