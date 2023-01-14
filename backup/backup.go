package backup

import (
	"context"
	"encoding/json"
	"fmt"
	help "github.com/featureform/helpers"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"time"
)

type ProviderType string

const (
	AZURE ProviderType = "AZURE"
	LOCAL              = "LOCAL"
)

// Client Allows ETCD to be tested with mock values
type Client interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

type Backup struct {
	ETCDClient   Client
	ProviderType ProviderType
}

func (b *Backup) Save(filename string) error {
	err := b.takeSnapshot(filename)
	if err != nil {
		return fmt.Errorf("could not take snapshot: %v", err)
	}

	backupProvider, err := b.getBackupProvider()
	if err != nil {
		return err
	}
	err = backupProvider.Init()
	if err != nil {
		return fmt.Errorf("could not initialize provider: %v", err)
	}
	err = backupProvider.Upload(filename, filename)
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

type BackupFile struct {
	Key   []byte
	Value []byte
}

func (b *Backup) takeSnapshot(filename string) error {
	resp, err := b.ETCDClient.Get(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("could get snapshot values: %v", err)
	}

	values := b.convertValues(resp.Kvs)
	err = b.writeValues(filename, values)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backup) convertValues(resp []*mvccpb.KeyValue) []BackupFile {
	values := []BackupFile{}
	for _, row := range resp {
		values = append(values, BackupFile{
			Key:   row.Key,
			Value: row.Value,
		})
	}
	return values
}

func (b *Backup) writeValues(filename string, values []BackupFile) error {
	file, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("could not marshal snapshot: %v", err)
	}
	if err = ioutil.WriteFile(filename, file, 0644); err != nil {
		return fmt.Errorf("could not write snapshot to file: %v", err)
	}
	return nil
}

func GenerateSnapshotName(currentTime time.Time) string {
	prefix := "featureform_etcd_snapshot"
	formattedTime := currentTime.Format("2006-01-02 15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}
