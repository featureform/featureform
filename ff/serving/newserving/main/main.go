package main

import (
	"net"

	"github.com/featureform/serving/metadata"
	"github.com/featureform/serving/metrics"
	"github.com/featureform/serving/newserving"

	pb "github.com/featureform/serving/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	logger := zap.NewExample().Sugar()

	port := ":8080"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}

	promMetrics := metrics.NewMetrics("test")
	metrics_port := ":2112"

	meta, err := metadata.NewClient("localhost:8081", logger)
	if err != nil {
		logger.Panicw("Failed to connect to metadata", "Err", err)
	}

	serv, err := newserving.NewFeatureServer(meta, promMetrics, logger)

	grpcServer := grpc.NewServer()
	if err != nil {
		logger.Panicw("Failed to create training server", "Err", err)
	}
	pb.RegisterFeatureServer(grpcServer, serv)
	logger.Infow("Serving metrics", "Port", metrics_port)
	go promMetrics.ExposePort(metrics_port)
	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}

}
