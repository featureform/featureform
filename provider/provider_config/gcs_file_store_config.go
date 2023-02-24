package provider_config

import "encoding/json"

type GCSFileStoreConfig struct {
	BucketName  string
	BucketPath  string
	Credentials []byte
}

func (s *GCSFileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *GCSFileStoreConfig) Serialize() []byte {
	conf, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return conf
}
