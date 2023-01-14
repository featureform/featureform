package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/featureform/provider"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"time"
)

// Client Allows ETCD to be tested with mock values
type Client interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

type BackupManager struct {
	ETCDClient Client
	Provider   provider.FileStoreType
}

func (b *BackupManager) Save() error {
	currentTimestamp := time.Now()
	snapshotName := GenerateSnapshotName(currentTimestamp)
	return b.SaveTo(snapshotName)
}

func (b *BackupManager) SaveTo(filename string) error {
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

func (b *BackupManager) Restore() error {
	panic(fmt.Errorf("Restore() unimplemented"))
	return nil
}

func (b *BackupManager) getBackupProvider() (Provider, error) {
	var backupProvider Provider
	switch b.Provider {
	case provider.Azure:
		backupProvider = &Azure{}
	case provider.FileSystem:
		backupProvider = &Local{}
	default:
		return nil, fmt.Errorf("the cloud provider '%s' is not supported", b.Provider)
	}

	return backupProvider, nil
}

type backupRow struct {
	Key   []byte
	Value []byte
}

type backup []backupRow

func (b *BackupManager) takeSnapshot(filename string) error {
	resp, err := b.ETCDClient.Get(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("could get snapshot values: %v", err)
	}

	err = b.convertEtcdToBackup(resp.Kvs).writeTo(filename)
	if err != nil {
		return err
	}
	return nil
}

func (b *BackupManager) convertEtcdToBackup(resp []*mvccpb.KeyValue) backup {
	values := make(backup, len(resp))
	for i, row := range resp {
		values[i] = backupRow{
			Key:   row.Key,
			Value: row.Value,
		}
	}
	return values
}

func (f backup) writeTo(filename string) error {
	file, err := json.Marshal(f)
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
	formattedTime := currentTime.Format("2006-01-02_15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}
