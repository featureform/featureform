package provider_config

import (
	"encoding/json"
	"fmt"

	ss "github.com/featureform/helpers/string_set"
)

type GCSFileStoreConfig struct {
	BucketName  string
	BucketPath  string
	Credentials GCPCredentials
}

func (s *GCSFileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *GCSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *GCSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (s *GCSFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a *GCSFileStoreConfig) DifferingFields(b ProviderConfig) (ss.StringSet, error) {
	if _, ok := b.(*GCSFileStoreConfig); !ok {
		return nil, fmt.Errorf("cannot compare different config types")
	}
	return differingFields(a, b)
}
