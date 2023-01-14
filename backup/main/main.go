package main

import (
	"fmt"
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
		DialTimeout: time.Second * 1,
		Username:    etcdUsername,
		Password:    etcdPassword,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		panic(err)
	}

	backupExecutor := backup.Backup{
		ETCDClient:   client,
		ProviderType: backup.ProviderType(help.GetEnv("CLOUD_PROVIDER", "LOCAL")),
	}

	currentTimestamp := time.Now()
	snapshotName := backup.GenerateSnapshotName(currentTimestamp)

	err = backupExecutor.Save(snapshotName)
	if err != nil {
		panic(err)
	}

}
