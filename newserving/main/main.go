// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"net"
	"os"

	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	"github.com/featureform/newserving"

	pb "github.com/featureform/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	logger := zap.NewExample().Sugar()

	port := os.Getenv("SERVING_PORT")
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}

	promMetrics := metrics.NewMetrics("test")
	metricsPort := os.Getenv("METRICS_PORT")

	metadataHost := os.Getenv("METADATA_HOST")
	metadataPort := os.Getenv("METADATA_PORT")
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)

	meta, err := metadata.NewClient(metadataConn, logger)
	if err != nil {
		logger.Panicw("Failed to connect to metadata", "Err", err)
	}

	serv, err := newserving.NewFeatureServer(meta, promMetrics, logger)

	grpcServer := grpc.NewServer()
	if err != nil {
		logger.Panicw("Failed to create training server", "Err", err)
	}
	pb.RegisterFeatureServer(grpcServer, serv)
	logger.Infow("Serving metrics", "Port", metricsPort)
	go promMetrics.ExposePort(metricsPort)
	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}

}
