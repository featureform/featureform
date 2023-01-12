package main

import (
	"context"
	"fmt"
	"time"

	help "github.com/featureform/helpers"
	"github.com/featureform/provider"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"
)

func main() {
	lg := zap.NewExample()
	defer lg.Sync()

	sp := snapshot.NewV3(lg)

	etcdHost := help.GetEnv("ETCD_HOSTNAME", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	etcdUsername := help.GetEnv("ETCD_USERNAME", "")
	etcdPassword := help.GetEnv("ETCD_PASSWORD", "")

	currentTimestamp := time.Now()
	snapshotName := generateSnapshotName(currentTimestamp)

	address := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	etcdConfig := clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 1,
		Username:    etcdUsername,
		Password:    etcdPassword,
	}

	err := sp.Save(context.TODO(), etcdConfig, snapshotName)
	if err != nil {
		msg := fmt.Sprintf("cannot save snapshot: %v", err)
		panic(msg)
	}

	filestore, err := getFilestore()
	if err != nil {
		msg := fmt.Sprintf("cannot get filestore: %v", err)
		panic(msg)
	}

	err = filestore.Upload(snapshotName, snapshotName)
	if err != nil {
		msg := fmt.Sprintf("cannot upload snapshot to filestore: %v", err)
		panic(msg)
	}
}

func generateSnapshotName(currentTime time.Time) string {
	prefix := "featureform_etcd_snapshot"
	formattedTime := currentTime.Format("2006-01-02 15:04:05")

	return fmt.Sprintf("%s__%s.db", prefix, formattedTime)
}

func getFilestore() (provider.FileStore, error) {
	cloudProvider := help.GetEnv("CLOUD_PROVIDER", "LOCAL")
	if cloudProvider == "AZURE" {
		storageAccount := help.GetEnv("AZURE_STORAGE_ACCOUNT", "")
		storageToken := help.GetEnv("AZURE_STORAGE_TOKEN", "")
		container := help.GetEnv("AZURE_CONTAINER_NAME", "")
		storagePath := help.GetEnv("AZURE_STORAGE_PATH", "")

		filestoreConfig := provider.AzureFileStoreConfig{
			AccountName:   storageAccount,
			AccountKey:    storageToken,
			ContainerName: container,
			Path:          storagePath,
		}
		config, err := filestoreConfig.Serialize()
		if err != nil {
			return nil, fmt.Errorf("cannot serialize the AzureFileStoreConfig: %v", err)
		}

		filestore, err := provider.NewAzureFileStore(config)
		if err != nil {
			return nil, fmt.Errorf("cannot create Azure Filestore: %v", err)
		}
		return filestore, nil
	} else {
		return nil, fmt.Errorf("the cloud provider '%s' is not supported", cloudProvider)
	}
}
