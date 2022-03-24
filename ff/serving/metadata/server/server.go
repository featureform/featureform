package main

import (
	"net"

	"github.com/featureform/serving/metadata"
	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	logger := zap.NewExample().Sugar()
	port := ":8080"
	lis, err := net.Listen("tcp", port)
	config := &metadata.Config{
		Logger: logger,
		//TypeSenseParams: &search.TypeSenseParams{
		//	Port:   "8108",
		//	Host:   "localhost",
		//	ApiKey: "xyz",
		//},
		StorageProvider: metadata.ETCD,
		ETCD: metadata.EtcdConfig{
			Host: "localhost",
			Port: "2379",
		},
	}
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}
	grpcServer := grpc.NewServer()
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	pb.RegisterMetadataServer(grpcServer, server)

	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}
}
