package backup

import (
	"bytes"
	"context"
	"fmt"
	"github.com/featureform/provider"
	"gocloud.dev/gcp"
)

type Provider interface {
	Init() error
	Upload(name, dest string) error
	Download(src, dest string) error
	LatestBackupName(prefix string) (string, error)
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

func (az *Azure) LatestBackupName(prefix string) (string, error) {
	return az.store.NewestFile(prefix)
}

type S3 struct {
	AWSAccessKeyId string
	AWSSecretKey   string
	BucketRegion   string
	BucketName     string
	BucketPath     string
	store          provider.FileStore
}

func (s3 *S3) Init() error {
	filestoreConfig := provider.S3FileStoreConfig{
		AWSAccessKeyId: s3.AWSAccessKeyId,
		AWSSecretKey:   s3.AWSSecretKey,
		BucketRegion:   s3.BucketRegion,
		BucketPath:     s3.BucketName,
		Path:           s3.BucketPath,
	}
  
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize S3 Config: %v", err)
	}

	filestore, err := provider.NewS3FileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create S3 Filestore: %v", err)
	}
	s3.store = filestore
	return nil
}

func (s3 *S3) Upload(name, dest string) error {
	return s3.store.Upload(name, dest)
}

func (s3 *S3) Download(src, dest string) error {
	return s3.store.Download(src, dest)
}

func (s3 *S3) LatestBackupName(prefix string) (string, error) {
	return s3.store.NewestFile(prefix)
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
		return fmt.Errorf("cannot serialize the LocalFileStoreConfig: %v", err)
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

func (fs *Local) LatestBackupName(prefix string) (string, error) {
	return fs.store.NewestFile(prefix)
}

type GCS struct {
	BucketName             string
	BucketPath             string
	CredentialFileLocation string
	Credentials            []byte
	store                  provider.FileStore
}

func (g *GCS) getDefaultCredentials() ([]byte, error) {
	if creds, err := gcp.DefaultCredentials(context.Background()); err != nil {
		return nil, err
	} else {
		return creds.JSON, nil
	}
}

func (g *GCS) checkCredentials() ([]byte, error) {
	if bytes.Equal(g.Credentials, []byte("")) {
		return g.getDefaultCredentials()
	} else {
		return g.Credentials, nil
	}
}

func (g *GCS) Init() error {
	credentials, err := g.checkCredentials()
	if err != nil {
		return fmt.Errorf("failed to check credentials: %v", err)
	}

	filestoreConfig := provider.GCSFileStoreConfig{
		BucketName:  g.BucketName,
		BucketPath:  g.BucketPath,
		Credentials: credentials,
	}
	config := filestoreConfig.Serialize()

	filestore, err := provider.NewGCSFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create GCS Filestore: %v", err)
	}
	g.store = filestore
	return nil
}

func (g *GCS) Upload(name, dest string) error {
	return g.store.Upload(name, dest)
}

func (g *GCS) Download(src, dest string) error {
	return g.store.Download(src, dest)
}

func (g *GCS) LatestBackupName(prefix string) (string, error) {
	return g.store.NewestFile(prefix)
}

