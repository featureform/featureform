package main

import (
	"fmt"
	"github.com/featureform/coordinator"
	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"time"
)

func main() {
	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_HOST", "6379")
	etcdUrl := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	fmt.Printf("connecting to etcd: %s\n", etcdUrl)
	fmt.Printf("connecting to metadata: %s\n", metadataUrl)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdUrl},
		Username:    help.GetEnv("ETCD_USERNAME", "root"),
		Password:    help.GetEnv("ETCD_PASSWORD", "secretpassword"),
		DialTimeout: time.Second * 1,
	})
	if err := runner.RegisterFactory(string(runner.COPY_TO_ONLINE), runner.MaterializedChunkRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register training set runner factory: %w", err))
	}
	if err := runner.RegisterFactory(string(runner.MATERIALIZE), runner.MaterializeRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register training set runner factory: %w", err))
	}
	if err := runner.RegisterFactory(string(runner.CREATE_TRANSFORMATION), runner.CreateTransformationRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register training set runner factory: %w", err))
	}
	if err := runner.RegisterFactory(string(runner.CREATE_TRAINING_SET), runner.TrainingSetRunnerFactory); err != nil {
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
	coord, err := coordinator.NewCoordinator(client, logger, cli, &coordinator.MemoryJobSpawner{})
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
