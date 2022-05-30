package main

import (
	"fmt"
	"github.com/featureform/coordinator"
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"os"
	"time"
)

func main() {
	etcdHost := os.Getenv("ETCD_HOST")
	etcdPort := os.Getenv("ETCD_PORT")
	etcdUrl := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	metadataHost := os.Getenv("METADATA_HOST")
	metadataPort := os.Getenv("METADATA_PORT")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	fmt.Printf("connecting to etcd: %s\n", etcdUrl)
	fmt.Printf("connecting to metadata: %s\n", metadataUrl)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdUrl},
		Username:    "root",
		Password:    "secretpassword",
		DialTimeout: time.Second * 1,
	})
	if err := runner.RegisterFactory(string(runner.COPY_TO_ONLINE), runner.MaterializedChunkRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register training set runner factory: %w", err))
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("connected to etcd")
	logger := zap.NewExample().Sugar()
	defer logger.Sync()
	logger.Debug("Connected to ETCD")
	client, err := metadata.NewClient(metadataUrl, logger)
	if err != nil {
		logger.Errorw("Failed to connect: %v", err)
		panic(err)
	}
	logger.Debug("Connected to Metadata")
	coord, err := coordinator.NewCoordinator(client, logger, cli, &coordinator.KubernetesJobSpawner{})
	if err != nil {
		logger.Errorw("Failed to set up coordinator: %v", err)
		panic(err)
	}
	logger.Debug("Begin Job Watch")
	if err := coord.WatchForNewJobs(); err != nil {
		logger.Errorw(err.Error())
		panic(err)
		return
	}
}
