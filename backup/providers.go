package backup

import (
	"fmt"
	"github.com/featureform/provider"
)

type Provider interface {
	Init() error
	Upload(name, dest string) error
	Restore() error
}

type Azure struct {
	AzureStorageAccount string
	AzureStorageKey     string
	AzureContainerName  string
	AzureStoragePath    string
	store               provider.FileStore
}

func (az *Azure) Init() error {
	filestoreConfig := provider.AzureFileStoreConfig{
		AccountName:   az.AzureStorageAccount,
		AccountKey:    az.AzureStorageKey,
		ContainerName: az.AzureContainerName,
		Path:          az.AzureStoragePath,
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
	Path  string
	store provider.FileStore
}

func (fs *Local) Init() error {
	filestoreConfig := provider.FileFileStoreConfig{
		DirPath: fs.Path,
	}
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize the AzureFileStoreConfig: %v", err)
	}

	filestore, err := provider.NewFileFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create Azure Filestore: %v", err)
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
