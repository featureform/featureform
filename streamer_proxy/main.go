package main

import (
	"log"
	"net"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
)

func main() {
	//get the gRPC server and stream back to the clients. todo: set the env variables from the container
	serverAddress := "0.0.0.0:8086"
	streamerAddress := "grpc://iceberg-streamer:0.0.0.0:8085"

	flightServer := &GoProxyServer{
		streamerAddress: streamerAddress,
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Failed to bind addres to %s: %v", serverAddress, err)
	}

	log.Printf("Starting Go Proxy Flight server on %s...", flightServer.streamerAddress)
	flight.RegisterFlightServiceServer(grpcServer, &flightServer.BaseFlightServer)
	servErr := grpcServer.Serve(listener)
	if servErr != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

}
