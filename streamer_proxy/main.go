// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	grpc_health "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/featureform/fferr"
	fs "github.com/featureform/filestore"
	"github.com/featureform/health"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pc "github.com/featureform/provider/provider_config"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const two_million_record_limit = 2_000_000

type GoProxyServer struct {
	flight.BaseFlightServer
	streamerAddress string
	logger          logging.Logger
	metadata        *metadata.Client
}

type TicketData struct {
	Source       string `json:"source"`
	Variant      string `json:"variant"`
	ResourceType string `json:"resourceType"`
	Limit        int    `json:"limit"`
}

// pulls the client ticket's location and hydrates with additional entries
func (gps *GoProxyServer) hydrateTicket(ticket *flight.Ticket) (*flight.Ticket, error) {
	var ticketData TicketData
	err := json.Unmarshal(ticket.Ticket, &ticketData)
	if err != nil {
		marshalErr := fferr.NewInternalErrorf("failed to parse ticket JSON: %w", err)
		gps.logger.Error(marshalErr)
		return nil, marshalErr
	}

	// handle location
	if ticketData.Source == "" {
		sourceErr := fferr.NewInternalErrorf("missing 'source' in ticket data")
		gps.logger.Error(sourceErr)
		return nil, sourceErr
	}

	if ticketData.Variant == "" {
		variantErr := fferr.NewInternalErrorf("missing 'variant' in ticket data")
		gps.logger.Error(variantErr)
		return nil, variantErr
	}

	if ticketData.ResourceType == "" {
		resourceTypeErr := fferr.NewInternalErrorf("missing 'resourceType' in ticket data")
		gps.logger.Error(resourceTypeErr)
		return nil, resourceTypeErr
	}

	sourceVariant, getSourceErr := gps.metadata.GetSourceVariant(context.TODO(), metadata.NameVariant{Name: ticketData.Source, Variant: ticketData.Variant})
	if getSourceErr != nil {
		gps.logger.Error("error when invoking metadata.GetSourceVariant()", "error", getSourceErr)
		return nil, getSourceErr
	}

	gps.logger.Infof("Fetching location with source variant: %s-%s", sourceVariant.Name(), sourceVariant.Variant())
	location, locationErr := sourceVariant.GetLocation(context.TODO())

	if locationErr != nil {
		gps.logger.Error("error when invoking sourceVariant.GetPrimaryLocation()", "error", locationErr)
		return nil, locationErr
	}

	gps.logger.Debugf("location found: %s", location.Location())
	gps.logger.Debugf("location type: %s", location.Type())

	if location == nil {
		err := fferr.NewInternalErrorf("location is nil after GetPrimaryLocation")
		gps.logger.Error(err)
		return nil, err
	}

	parts := strings.Split(location.Location(), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		splitErr := fferr.NewInternalErrorf("invalid location format, expected 'namespace.table' but got: %s", location.Location())
		gps.logger.Error(splitErr)
		return nil, splitErr
	}
	namespace := parts[0]
	table := parts[1]

	// validate limit
	if ticketData.Limit == 0 {
		ticketData.Limit = two_million_record_limit
	}

	//pull the provider
	provider, providerErr := gps.metadata.GetProvider(context.TODO(), sourceVariant.Provider())
	if providerErr != nil {
		gps.logger.Error("error when invoking metadata.GetProvider(%s)", sourceVariant.Provider())
		return nil, providerErr
	}

	config := &pc.SparkConfig{}
	if err := config.Deserialize(provider.SerializedConfig()); err != nil {
		gps.logger.Error("could not deserialize the provider config", "error", err)
		return nil, err
	}
	if config.GlueConfig == nil {
		glueErr := fferr.NewInternalErrorf("streamer only supports GlueCatalog")
		gps.logger.Error(glueErr)
		return nil, glueErr
	}
	roleArn := config.GlueConfig.AssumeRoleArn
	if config.StoreType != fs.S3 {
		storeErr := fferr.NewInternalErrorf("Store type not supported by streamer: %s", config.StoreType)
		gps.logger.Error(storeErr)
		return nil, storeErr
	}
	var creds pc.AWSStaticCredentials
	if roleArn == "" {
		// Grab creds from store
		s3Config, ok := config.StoreConfig.(*pc.S3FileStoreConfig)
		if !ok {
			credsErr := fferr.NewInternalErrorf(
				"Invalid Spark Config. StoreType is %s but StoreConfig is %T",
				config.StoreType, config.StoreConfig,
			)
			gps.logger.Error(credsErr)
			return nil, credsErr
		}
		staticCreds, ok := s3Config.Credentials.(pc.AWSStaticCredentials)
		if !ok {
			credsErr := fferr.NewInternalErrorf(
				"If Glue is not using AssumeRoleArn then S3 must have static creds but has %T",
				s3Config.Credentials,
			)
			gps.logger.Error(credsErr)
			return nil, credsErr
		}
		creds = staticCreds
	}
	hydratedTicketData := map[string]any{
		"catalog":                  "default",
		"namespace":                namespace,
		"table":                    table,
		"client.region":            config.GlueConfig.Region,
		"client.access-key-id":     creds.AccessKeyId,
		"client.secret-access-key": creds.SecretKey,
		"client.role-arn":          roleArn,
		"limit":                    ticketData.Limit,
	}

	//re-package the ticket
	hydratedTicketBytes, err := json.Marshal(hydratedTicketData)
	if err != nil {
		return nil, fferr.NewInternalErrorf("failed to marshal hydrated ticket JSON: %w", err)
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
		flightData, recvErr := flightStream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				gps.logger.Infof("Reached the end of the stream")
				break
			}
			gps.logger.Errorf("An error occurred receiving the flight data from stream: %v", recvErr)
			return recvErr
		}

		//send back the flight data as-is
		sendErr := stream.Send(flightData)
		if sendErr != nil {
			gps.logger.Errorf("An error occurred passing the flight data to client: %v", sendErr)
			return sendErr
		}
	}

	gps.logger.Info("Proxy Get Complete")
	return nil
}

func main() {
	baseLogger := logging.NewLogger("iceberg-proxy")
	serverAddress := "0.0.0.0:8086"
	streamerPort := helpers.GetEnv("ICEBERG_STREAMER_PORT", "8085")
	streamerHost := helpers.GetEnv("ICEBERG_STREAMER_HOST", "localhost")
	if streamerPort == "" {
		baseLogger.Fatalf("Missing ICEBERG_STREAMER_PORT env variable: %v", streamerPort)
	}

	if streamerHost == "" {
		baseLogger.Fatalf("Missing ICEBERG_STREAMER_HOST env variable: %v", streamerHost)
	}

	proxyFlightServer := &GoProxyServer{
		streamerAddress: fmt.Sprintf("%s:%s", streamerHost, streamerPort),
		logger:          baseLogger,
	}

	proxyFlightServer.logger.Infof("Go proxy using streamer address %s", proxyFlightServer.streamerAddress)

	// connect to metadata
	metadataHost := helpers.GetEnv("METADATA_HOST", "localhost")
	metadataPort := helpers.GetEnv("METADATA_PORT", "8080")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)

	client, err := metadata.NewClient(metadataUrl, baseLogger)
	if err != nil {
		proxyFlightServer.logger.Fatalf("Failed to connect to metadata service: %v", err)
	}
	proxyFlightServer.logger.Infof("Connected to Metadata at %s", metadataUrl)
	proxyFlightServer.metadata = client

	apiStatusPort := helpers.GetEnv("API_STATUS_PORT", "8443")
	baseLogger.Infow("Retrieved API status port from ENV", "port", apiStatusPort)
	if err = health.StartHttpServer(baseLogger, apiStatusPort); err != nil {
		baseLogger.Errorw("Failed to start health check", "err", err)
		panic(err)
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		proxyFlightServer.logger.Fatalf("Failed to bind address to %s: %v", serverAddress, err)
	}

	healthServer := grpc_health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// start the proxy flight server
	proxyFlightServer.logger.Infof("Starting Go Proxy Flight server on %s...", serverAddress)
	proxyFlightServer.logger.Infof("Go proxy using streamer address %s", proxyFlightServer.streamerAddress)
	flight.RegisterFlightServiceServer(grpcServer, proxyFlightServer)
	servErr := grpcServer.Serve(listener)
	if servErr != nil {
		proxyFlightServer.logger.Fatalf("Failed to start gRPC server: %v", servErr)
	}
}
