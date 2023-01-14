package backup

import (
	"fmt"
	"github.com/featureform/provider"
)

type Provider interface {
	Init() error
	Upload(name, dest string) error
	Download(src, dest string) error
	LatestBackupName() (string, error)
}

type Azure struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
	store         provider.FileStore
}

func (az *Azure) Init() error {
	filestoreConfig := provider.AzureFileStoreConfig{
		AccountName:   az.AccountName,
		AccountKey:    az.AccountKey,
		ContainerName: az.ContainerName,
		Path:          az.Path,
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

func (az *Azure) Download(src, dest string) error {
	return az.store.Download(src, dest)
}

func (az *Azure) LatestBackupName() (string, error) {
	return az.store.NewestFile("")
}

type Local struct {
	Path  string
	store provider.FileStore
}

func (fs *Local) Init() error {
	filestoreConfig := provider.LocalFileStoreConfig{
		DirPath: fs.Path,
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

func (fs *Local) Download(src, dest string) error {
	return fs.store.Download(src, dest)
}

func (fs *Local) LatestBackupName() (string, error) {
	return fs.store.NewestFile("")
}
