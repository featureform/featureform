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
	"strings"

	"encoding/json"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const one_million_record_limit = 1_000_000

type GoProxyServer struct {
	flight.BaseFlightServer
	streamerAddress string
	logger          logging.Logger
}

type TicketData struct {
	Location        string `json:"location"`
	Region          string `json:"client.region"`
	AccessKeyId     string `json:"client.access-key-id"`
	SecretAccessKey string `json:"client.secret-access-key"`
	Limit           int    `json:"limit"`
}

// pulls the client ticket's location and hydrates with additional entries
func (gps *GoProxyServer) hydrateTicket(ticket *flight.Ticket) (*flight.Ticket, error) {
	var ticketData TicketData
	err := json.Unmarshal(ticket.Ticket, &ticketData)
	if err != nil {
		masrhalErr := fmt.Errorf("failed to parse ticket JSON: %w", err)
		gps.logger.Error(masrhalErr)
		return nil, masrhalErr
	}

	// handle location
	if ticketData.Location == "" {
		locationErr := fmt.Errorf("missing 'location' in ticket data")
		gps.logger.Error(locationErr)
		return nil, locationErr
	}

	parts := strings.Split(ticketData.Location, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		splitErr := fmt.Errorf("invalid location format, expected 'namespace.table' but got: %s", ticketData.Location)
		gps.logger.Error(splitErr)
		return nil, splitErr
	}
	namespace := parts[0]
	table := parts[1]

	// validate region
	if ticketData.Region == "" {
		regionErr := fmt.Errorf("missing 'client.region' in ticket data")
		gps.logger.Error(regionErr)
		return nil, regionErr
	}

	//  validate keyId
	if ticketData.AccessKeyId == "" {
		accessKeyIdErr := fmt.Errorf("missing 'client.access-key-id' in ticket data")
		gps.logger.Error(accessKeyIdErr)
		return nil, accessKeyIdErr
	}

	// validate secretKey
	if ticketData.SecretAccessKey == "" {
		secretErr := fmt.Errorf("missing 'client.secret-access-key' in ticket data")
		gps.logger.Error(secretErr)
		return nil, secretErr
	}

	// validate limit
	if ticketData.Limit == 0 {
		ticketData.Limit = one_million_record_limit
	}

	hydratedTicketData := map[string]any{
		"catalog":                  "default",
		"namespace":                namespace,
		"table":                    table,
		"client.access-key-id":     ticketData.AccessKeyId,
		"client.secret-access-key": ticketData.SecretAccessKey,
		"client.region":            ticketData.Region,
		"limit":                    ticketData.Limit,
	}

	//re-package the ticket
	hydratedTicketBytes, err := json.Marshal(hydratedTicketData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal hydrated ticket JSON: %w", err)
	}

	return &flight.Ticket{Ticket: hydratedTicketBytes}, nil
}

func (gps *GoProxyServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	gps.logger.Infof("Received request, forwarding to iceberg-streamer at: %v", gps.streamerAddress)
	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	sizeOption := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 * 1024 * 1024)) //20 MB

	client, err := flight.NewClientWithMiddleware(gps.streamerAddress, nil, nil, insecureOption, sizeOption)
	if err != nil {
		gps.logger.Errorf("Failed to connect to the iceberg-streamer: %v", err)
		return err
	}
	defer client.Close()

	filledTicket, err := gps.hydrateTicket(ticket)
	if err != nil {
		gps.logger.Errorf("Failed to hydrate ticket: %v", err)
		return err
	}

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
	streamerAddress := helpers.GetEnv("ICEBERG_STREAMER_PORT", "")
	if streamerAddress == "" {
		baseLogger.Fatalf("Missing ICEBERG_STREAMER_PORT env variable: %v", streamerAddress)
	}

	proxyFlightServer := &GoProxyServer{
		streamerAddress: "iceberg-streamer:" + streamerAddress,
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
