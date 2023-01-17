package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"time"
)

const SnapshotFilename = "snapshot.json"
const SnapshotPrefix = "featureform_etcd_snapshot"

// Client Allows ETCD to be tested with mock values
type Client interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
}

type BackupManager struct {
	ETCDClient Client
	Provider   Provider
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

	err = b.Provider.Init()
	if err != nil {
		return fmt.Errorf("could not initialize provider: %v", err)
	}
	err = b.Provider.Upload(filename, filename)
	if err != nil {
		return fmt.Errorf("cannot upload snapshot to filestore: %v", err)
	}
	return nil
}

func (b *BackupManager) Restore(prefix string) error {
	err := b.Provider.Init()
	if err != nil {
		return fmt.Errorf("could not initialize provider: %v", err)
	}

	filename, err := b.Provider.LatestBackupName(prefix)
	if err != nil {
		return fmt.Errorf("could not get latest backup: %v", err)
	}

	err = b.RestoreFrom(filename)
	if err != nil {
		return fmt.Errorf("could not restore file %s: %v", filename, err)
	}
	return nil
}

func (b *BackupManager) RestoreFrom(filename string) error {
	err := b.Provider.Init()
	if err != nil {
		return fmt.Errorf("could not initialize provider: %v", err)
	}

	err = b.Provider.Download(filename, SnapshotFilename)
	if err != nil {
		return fmt.Errorf("could not download snapshot file %s: %v", filename, err)
	}

	err = b.loadSnapshot(SnapshotFilename)
	if err != nil {
		return fmt.Errorf("could not load snapshot: %v", err)
	}
	return nil
}

type backupRow struct {
	Key   []byte
	Value []byte
}

type backup []backupRow

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

func (f *backup) readFrom(filename string) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("could not read file %s: %v", filename, err)
	}
	err = json.Unmarshal(file, &f)
	if err != nil {
		return fmt.Errorf("could not unmarshal file %s: %v", filename, err)
	}
	return nil
}

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

func (b *BackupManager) loadSnapshot(filename string) error {
	backupData := backup{}
	err := backupData.readFrom(filename)
	if err != nil {
		return fmt.Errorf("could not read from file %s: %v", filename, err)
	}
	err = b.writeToEtcd(backupData)
	if err != nil {
		return fmt.Errorf("could not write to ETCD: %v", err)
	}
	return nil
}

func (b *BackupManager) writeToEtcd(data backup) error {
	for _, row := range data {
		_, err := b.ETCDClient.Put(context.Background(), string(row.Key), string(row.Value))
		if err != nil {
			return fmt.Errorf("could not Put K/V (%s:%s): %v", row.Key, row.Value, err)
		}
	}
	return nil
}

func GenerateSnapshotName(currentTime time.Time) string {
	prefix := SnapshotPrefix
	formattedTime := currentTime.Format("2006-01-02_15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}
