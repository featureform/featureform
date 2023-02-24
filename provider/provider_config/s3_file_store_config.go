package provider_config

import "encoding/json"

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
