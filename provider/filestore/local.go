package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/featureform/provider"
	"gocloud.dev/blob"
)

type LocalFileStoreConfig struct {
	DirPath string
}

func (config *LocalFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (config *LocalFileStoreConfig) Deserialize(data []byte) error {
	err := json.Unmarshal(data, config)
	if err != nil {
		return fmt.Errorf("deserialize file blob store config: %w", err)
	}
	return nil
}

type LocalFileStore struct {
	DirPath string
	GenericFileStore
}

func NewLocalFileStore(config provider.Config) (FileStore, error) {
	fileStoreConfig := LocalFileStoreConfig{}
	if err := fileStoreConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("could not deserialize file store config: %v", err)
	}
	bucket, err := blob.OpenBucket(context.TODO(), fileStoreConfig.DirPath)
	if err != nil {
		return nil, err
	}
	return LocalFileStore{
		DirPath: fileStoreConfig.DirPath[len("file:///"):],
		GenericFileStore: GenericFileStore{
			bucket: bucket,
			path:   fileStoreConfig.DirPath,
		},
	}, nil
}
