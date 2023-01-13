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

	backupExecutor := backup.Backup{
		ETCDConfig:   etcdConfig,
		ProviderType: backup.ProviderType(help.GetEnv("CLOUD_PROVIDER", "LOCAL")),
	}

	currentTimestamp := time.Now()
	snapshotName := generateSnapshotName(currentTimestamp)

	err := backupExecutor.Save(snapshotName)
	if err != nil {
		panic(err)
	}

}

func generateSnapshotName(currentTime time.Time) string {
	prefix := "featureform_etcd_snapshot"
	formattedTime := currentTime.Format("2006-01-02 15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}
