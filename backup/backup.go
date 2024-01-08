package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/featureform/filestore"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const SnapshotFilename = "snapshot.json"
const SnapshotPrefix = "featureform_snapshot"

// Client Allows ETCD to be tested with mock values
type Client interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
}

type BackupManager struct {
	ETCDClient Client
	Provider   Provider
	Logger     *zap.SugaredLogger
}

func (b *BackupManager) Save() error {
	currentTimestamp := time.Now()
	snapshotName := GenerateSnapshotName(currentTimestamp)
	return b.SaveTo(snapshotName)
}

func (b *BackupManager) SaveTo(filename string) error {
	err := b.takeSnapshot(filename)
	if err != nil {
		return err
	}

	err = b.Provider.Init()
	if err != nil {
		return err
	}
	err = b.Provider.Upload(filename, filename)
	if err != nil {
		return err
	}
	return nil
}

func (b *BackupManager) Restore(filenamePrefix string) error {
	b.Logger.Info("Starting Restore")
	err := b.Provider.Init()
	if err != nil {
		return err
	}

	b.Logger.Infof("Getting Latest Backup With Prefix `%s`", filenamePrefix)
	filename, err := b.Provider.LatestBackupName(filenamePrefix)
	if err != nil {
		return err
	}

	b.Logger.Infof("Restoring with file: %s", filename.ToURI())
	err = b.RestoreFrom(filename)
	if err != nil {
		return err
	}
	return nil
}

func (b *BackupManager) RestoreFrom(source filestore.Filepath) error {
	err := b.Provider.Init()
	if err != nil {
		return err
	}
	b.Logger.Info("Downloading Restore File")
	destination := &filestore.LocalFilepath{}
	if err := destination.SetKey(SnapshotFilename); err != nil {
		return err
	}

	err = b.Provider.Download(source, destination)
	if err != nil {
		return err
	}

	b.Logger.Info("Loading Snapshot")
	err = b.loadSnapshot(SnapshotFilename)
	if err != nil {
		return err
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
		return err
	}
	if err = ioutil.WriteFile(filename, file, 0644); err != nil {
		return err
	}
	return nil
}

func (f *backup) readFrom(filename string) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = json.Unmarshal(file, &f)
	if err != nil {
		return err
	}
	return nil
}

func (b *BackupManager) takeSnapshot(filename string) error {
	resp, err := b.ETCDClient.Get(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		return err
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
	b.Logger.Info("Reading From Snapshot File")
	err := backupData.readFrom(filename)
	if err != nil {
		return err
	}
	b.Logger.Info("Clearing ETCD")
	err = b.clearEtcd()
	if err != nil {
		return err
	}
	b.Logger.Info("Writing Snapshot to ETCD")
	if err := b.writeToEtcd(backupData); err != nil {
		b.Logger.Error("BACKUP FAILED: Backup was unable to complete. Partial snapshot has been restored")
		return err
	}

	return nil
}

func (b *BackupManager) writeToEtcd(data backup) error {
	for _, row := range data {
		_, err := b.ETCDClient.Put(context.Background(), string(row.Key), string(row.Value))
		if err != nil {
			b.Logger.Error("could not Put K/V (%s:%s): %v", row.Key, row.Value, err)
			return err
		}
	}
	return nil
}

func (b *BackupManager) clearEtcd() error {
	_, err := b.ETCDClient.Delete(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	return nil
}

func GenerateSnapshotName(currentTime time.Time) string {
	prefix := SnapshotPrefix
	formattedTime := currentTime.Format("2006-01-02_15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}
