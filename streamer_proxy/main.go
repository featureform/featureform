package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/featureform/helpers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GoProxyServer struct {
	flight.BaseFlightServer
	streamerAddress string
}

func (gps *GoProxyServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	log.Printf("Received request, forwarding to iceberg-streamer at: %v", gps.streamerAddress)
	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(gps.streamerAddress, nil, nil, insecureOption)
	if err != nil {
		log.Fatalf("Failed to connect to the iceberg-streamer: %v", err)
		return err
	}
	defer client.Close()

	// fetch and pass stream back to the caller
	flightStream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		log.Printf("Error fetching the data from the iceberg-streamer: %v", err)
		return err
	}

	for {
		flightData, err := flightStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("Reached the end of the stream")
				break
			}
			return err
		}

		//send back the flight data as-is
		sendErr := stream.Send(flightData)
		if sendErr != nil {
			return err
		}
	}
	return nil
}

func main() {
	serverAddress := "0.0.0.0:8086"
	streamerAddress := helpers.GetEnv("ICEBERG_STREAMER_ADDRESS", "")
	if streamerAddress == "" {
		log.Fatalf("Missing ICEBERG_STREAMER_ADDRESS env variable: %v", streamerAddress)
	}

	proxyFlightServer := &GoProxyServer{
		streamerAddress: streamerAddress,
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Failed to bind addres to %s: %v", serverAddress, err)
	}

	log.Printf("Starting Go Proxy Flight server on %s...", serverAddress)
	flight.RegisterFlightServiceServer(grpcServer, proxyFlightServer)
	servErr := grpcServer.Serve(listener)
	if servErr != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
}
