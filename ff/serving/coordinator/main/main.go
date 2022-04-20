package main

import (
	"net"

	"github.com/featureform/serving/coordinator"
	"github.com/featureform/serving/metrics"
	"github.com/featureform/serving/newserving"

	pb "github.com/featureform/serving/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		logger.Errorw("Failed to connect: %v", err)
	}
	coord, err := NewCoordinator(client, logger, cli)
	if err != nil {
		logger.Errorw("Failed to set up coordinator: %v", err)
	}
	go func() {
		if err := coord.StartJobWatcher(); err != nil {
			return err
		}
	}()
	go func() {
		if err := coord.startTimedJobWatcher(); err != nil {
			return err
		}
	}()
	
}
