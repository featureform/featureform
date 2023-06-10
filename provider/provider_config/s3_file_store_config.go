package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type S3FileStoreConfig struct {
	DefaultProviderConfig
	Credentials  AWSCredentials
	BucketRegion string
	BucketPath   string
	Path         string
}

func (s *S3FileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

type HDFSFileStoreConfig struct {
	DefaultProviderConfig
	Host     string
	Port     string
	Path     string
	Username string
}

func (s *HDFSFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Host": true,
	}
}
