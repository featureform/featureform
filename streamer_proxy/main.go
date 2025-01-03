// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GoProxyServer struct {
	flight.BaseFlightServer
	streamerAddress string
	logger          logging.Logger
}

func hydrateTicket(ticket *flight.Ticket) *flight.Ticket {
	//copy the ticket values,
	//get the provider credentials from location.go
	/*
		ticket_data = {
			"catalog": catalog,
			"namespace": namespace,
			"table": table,
			"client.access-key-id": access_key_id,
			"client.secret-access-key": secret_access_key,
		}
	*/
	//send back the ticket
	fmt.Println(ticket.Ticket)
	return ticket
}

func (gps *GoProxyServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	gps.logger.Infof("Received request, forwarding to iceberg-streamer at: %v", gps.streamerAddress)
	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(gps.streamerAddress, nil, nil, insecureOption)
	if err != nil {
		gps.logger.Errorf("Failed to connect to the iceberg-streamer: %v", err)
		return err
	}
	defer client.Close()

	filledTicket := hydrateTicket(ticket)

	// fetch and pass stream back to the caller
	flightStream, err := client.DoGet(context.Background(), filledTicket)
	if err != nil {
		gps.logger.Errorf("Error fetching the data from the iceberg-streamer: %v", err)
		return err
	}

	for {
		flightData, err := flightStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				gps.logger.Infof("Reached the end of the stream")
				break
			}
			gps.logger.Errorf("An error occurred receiving the flight data from stream: %v", err)
			return err
		}

		//send back the flight data as-is
		sendErr := stream.Send(flightData)
		if sendErr != nil {
			gps.logger.Errorf("An error occurred passing the flight data to client: %v", err)
			return err
		}
	}
	return nil
}

func main() {
	baseLogger := logging.NewLogger("iceberg-proxy")
	serverAddress := "0.0.0.0:8086"
	streamerAddress := helpers.GetEnv("ICEBERG_STREAMER_ADDRESS", "")
	if streamerAddress == "" {
		baseLogger.Fatalf("Missing ICEBERG_STREAMER_ADDRESS env variable: %v", streamerAddress)
	}

	proxyFlightServer := &GoProxyServer{
		streamerAddress: streamerAddress,
		logger:          baseLogger,
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		proxyFlightServer.logger.Fatalf("Failed to bind address to %s: %v", serverAddress, err)
	}

	proxyFlightServer.logger.Infof("Starting Go Proxy Flight server on %s...", serverAddress)
	flight.RegisterFlightServiceServer(grpcServer, proxyFlightServer)
	servErr := grpcServer.Serve(listener)
	if servErr != nil {
		proxyFlightServer.logger.Fatalf("Failed to start gRPC server: %v", err)
	}
}
