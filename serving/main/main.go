// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	"github.com/featureform/serving"
	"log"
	"net"
	"net/http"

	pb "github.com/featureform/proto"
	"google.golang.org/grpc"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	logger := logging.NewLogger("serving")

	host := help.GetEnv("SERVING_HOST", "0.0.0.0")
	port := help.GetEnv("SERVING_PORT", "8080")
	address := fmt.Sprintf("%s:%s", host, port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}

	promMetrics := metrics.NewMetrics("test")
	metricsPort := help.GetEnv("METRICS_PORT", ":9090")

	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)

	meta, err := metadata.NewClient(metadataConn, logger)
	if err != nil {
		logger.Panicw("Failed to connect to metadata", "Err", err)
	}

	serv, err := serving.NewFeatureServer(meta, promMetrics, logger)
	if err != nil {
		logger.Panicw("Failed to create training server", "Err", err)
	}
	grpcServer := grpc.NewServer()

	pb.RegisterFeatureServer(grpcServer, serv)
	logger.Infow("Serving metrics", "Port", metricsPort)
	go promMetrics.ExposePort(metricsPort)
	logger.Infow("Server starting", "Port", address)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}

}
