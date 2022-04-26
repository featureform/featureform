package main

import (
	"fmt"
	"github.com/featureform/serving/coordinator"
	"github.com/featureform/serving/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"os"
)

func main() {
	etcdHost := os.Getenv("ETCD_HOST")
	etcdPort := os.Getenv("ETCD_PORT")
	etcdUrl := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	metadataHost := os.Getenv("METADATA_HOST")
	metadataPort := os.Getenv("METADATA_HOST")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	fmt.Println("connecting to etcd: %s\n", etcdUrl)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdUrl},
		Username:  "root",
		Password:  "secretpassword",
	})
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient(metadataUrl, logger)
	if err != nil {
		logger.Errorw("Failed to connect: %v", err)
	}
	coord, err := coordinator.NewCoordinator(client, logger, cli, &coordinator.KubernetesJobSpawner{})
	if err != nil {
		logger.Errorw("Failed to set up coordinator: %v", err)
	}

	if err := coord.WatchForNewJobs(); err != nil {
		panic(err)
		return
	}
}
