package backup

import (
	"context"
	"fmt"
	help "github.com/featureform/helpers"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"time"
)

type ProviderType string

const (
	AZURE ProviderType = "AZURE"
	LOCAL              = "LOCAL"
)

type Backup struct {
	ETCDConfig   clientv3.Config
	ProviderType ProviderType
}

func (b *Backup) Save(name string) error {
	sp := snapshot.NewV3(nil)
	err := sp.Save(context.TODO(), b.ETCDConfig, name)
	if err != nil {
		return fmt.Errorf("cannot save snapshot: %v", err)
	}

	backupProvider, err := b.getBackupProvider()
	err = backupProvider.Upload(name, name)
	if err != nil {
		return fmt.Errorf("cannot upload snapshot to filestore: %v", err)
	}
	return nil
}

func (b *Backup) Restore() error {
	return nil
}

func (b *Backup) getBackupProvider() (Provider, error) {
	var backupProvider Provider
	switch b.ProviderType {
	case AZURE:
		backupProvider = &Azure{
			AzureStorageAccount: help.GetEnv("AZURE_STORAGE_ACCOUNT", ""),
			AzureStorageKey:     help.GetEnv("AZURE_STORAGE_KEY", ""),
			AzureContainerName:  help.GetEnv("AZURE_CONTAINER_NAME", ""),
			AzureStoragePath:    help.GetEnv("AZURE_STORAGE_PATH", ""),
		}
	case LOCAL:
		backupProvider = &Local{
			Path: "file://./",
		}
	default:
		return nil, fmt.Errorf("the cloud provider '%s' is not supported", b.ProviderType)
	}

	return backupProvider, nil
}

func GenerateSnapshotName(currentTime time.Time) string {
	prefix := "featureform_etcd_snapshot"
	formattedTime := currentTime.Format("2006-01-02 15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}
