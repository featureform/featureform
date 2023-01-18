package main

import (
	"fmt"
	"github.com/featureform/backup"
	help "github.com/featureform/helpers"
	"github.com/featureform/provider"
	"github.com/joho/godotenv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

func main() {
	godotenv.Load(".env")
	snapshotName := help.GetEnv("SNAPSHOT_NAME", backup.SnapshotPrefix)
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

	p := provider.FileStoreType(help.GetEnv("CLOUD_PROVIDER", "FILE_SYSTEM"))

	var backupProvider backup.Provider
	switch p {
	case provider.Azure:
		backupProvider = &backup.Azure{
			AccountName:   help.GetEnv("AZURE_STORAGE_ACCOUNT", ""),
			AccountKey:    help.GetEnv("AZURE_STORAGE_KEY", ""),
			ContainerName: help.GetEnv("AZURE_CONTAINER_NAME", ""),
			Path:          help.GetEnv("AZURE_STORAGE_PATH", ""),
		}
	case provider.S3:
		backupProvider = &backup.S3{
			AWSAccessKeyId: help.GetEnv("AWS_ACCESS_KEY", ""),
			AWSSecretKey:   help.GetEnv("AWS_SECRET_KEY", ""),
			BucketRegion:   help.GetEnv("AWS_BUCKET_REGION", ""),
			BucketName:     help.GetEnv("AWS_BUCKET_NAME", ""),
			BucketPath:     help.GetEnv("AWS_BUCKET_PATH", ""),
		}
	case provider.FileSystem:
		backupProvider = &backup.Local{
			Path: help.GetEnv("LOCAL_FILESTORE_PATH", "file://./"),
		}
	default:
		panic(fmt.Errorf("the cloud provider '%s' is not supported", p))
	}

	backupExecutor := backup.BackupManager{
		ETCDClient: client,
		Provider:   backupProvider,
	}

	if snapshotName == "" {
		snapshotName = backup.SnapshotPrefix
	}

	err = backupExecutor.Restore(snapshotName)
	if err != nil {
		panic(err)
	}
}
