package backup

import (
	"fmt"
	help "github.com/featureform/helpers"
	"github.com/featureform/provider"
)

type Provider interface {
	Init() error
	Upload(name, dest string) error
	Restore() error
}

type Azure struct {
	store provider.FileStore
}

func (az *Azure) Init() error {
	filestoreConfig := provider.AzureFileStoreConfig{
		AccountName:   help.GetEnv("AZURE_STORAGE_ACCOUNT", ""),
		AccountKey:    help.GetEnv("AZURE_STORAGE_KEY", ""),
		ContainerName: help.GetEnv("AZURE_CONTAINER_NAME", ""),
		Path:          help.GetEnv("AZURE_STORAGE_PATH", ""),
	}
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize the AzureFileStoreConfig: %v", err)
	}

	filestore, err := provider.NewAzureFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create Azure Filestore: %v", err)
	}
	az.store = filestore
	return nil
}

func (az *Azure) Upload(name, dest string) error {
	return az.store.Upload(name, dest)
}

func (az *Azure) Restore() error {
	return nil
}

type Local struct {
	store provider.FileStore
}

func (fs *Local) Init() error {
	filestoreConfig := provider.LocalFileStoreConfig{
		DirPath: help.GetEnv("LOCAL_FILESTORE_PATH", "file://./"),
	}
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize the AzureFileStoreConfig: %v", err)
	}

	filestore, err := provider.NewLocalFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create Local Filestore: %v", err)
	}
	fs.store = filestore
	return nil
}

func (fs *Local) Upload(name, dest string) error {
	return fs.store.Upload(name, dest)
}

func (fs *Local) Restore() error {
	return nil
}
