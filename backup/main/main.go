package main

import (
	"fmt"
	"github.com/featureform/provider"
	"time"

	"github.com/featureform/backup"
	help "github.com/featureform/helpers"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	etcdHost := help.GetEnv("ETCD_HOSTNAME", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	etcdUsername := help.GetEnv("ETCD_USERNAME", "")
	etcdPassword := help.GetEnv("ETCD_PASSWORD", "")

	address := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	etcdConfig := clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 10,
		Username:    etcdUsername,
		Password:    etcdPassword,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		panic(err)
	}

	backupExecutor := backup.BackupManager{
		ETCDClient: client,
		Provider:   provider.FileStoreType(help.GetEnv("CLOUD_PROVIDER", "LOCAL")),
	}

	currentTimestamp := time.Now()
	snapshotName := backup.GenerateSnapshotName(currentTimestamp)

	err = backupExecutor.Save(snapshotName)
	if err != nil {
		panic(err)
	}

}
