// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	fs "github.com/featureform/filestore"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	pc "github.com/featureform/provider/provider_config"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type SourceClient struct {
	grpc.ClientStream
}

var sourceCalls = 0

func (c SourceClient) Send(nv *pb.NameVariantRequest) error {
	return nil
}

func (c SourceClient) Recv() (*pb.SourceVariant, error) {
	if sourceCalls == 0 {
		sourceCalls++
		return &pb.SourceVariant{
			Name:    "test_name",
			Variant: "test_variant",
			Definition: &pb.SourceVariant_PrimaryData{PrimaryData: &pb.PrimaryData{
				Location: &pb.PrimaryData_Catalog{
					Catalog: &pb.CatalogTable{
						Database:    expectedDatabase,
						Table:       expectedTable,
						TableFormat: "tableFormat",
					},
				},
			}},
			Provider: "test_provider",
		}, nil
	}
	return nil, io.EOF
}

func (c SourceClient) CloseSend() error {
	return nil
}

const (
	expectedAccessKey = "access-key-test"
	expectedSecretKey = "secret-key-test"
	expectedRegion    = "regionTest"
	expectedWarehouse = "warehouse"
	expectedDatabase  = "database"
	expectedTable     = "table"
)

type ProviderClient struct {
	grpc.ClientStream
}

var providerCalls = 0

func (c ProviderClient) Send(nv *pb.NameRequest) error {
	return nil
}

func (c ProviderClient) Recv() (*pb.Provider, error) {
	creds := pc.AWSStaticCredentials{
		AccessKeyId: expectedAccessKey,
		SecretKey:   expectedSecretKey,
	}
	sparkConfig := &pc.SparkConfig{
		ExecutorType:   pc.Databricks,
		ExecutorConfig: &pc.DatabricksConfig{},
		StoreType:      fs.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials: creds,
		},
		GlueConfig: &pc.GlueConfig{
			Warehouse: expectedWarehouse,
			Region:    expectedRegion,
		},
	}
	serializedConfig, err := sparkConfig.Serialize()
	if err != nil {
		panic(err)
	}
	if providerCalls == 0 {
		providerCalls++
		return &pb.Provider{
			Name:             "sample_test_provider",
			Type:             "GLUE",
			Software:         "AWS",
			SerializedConfig: serializedConfig,
			Tags:             &pb.Tags{Tag: []string{"sample_tag"}},
			Properties:       &pb.Properties{},
			Status:           &pb.ResourceStatus{Status: pb.ResourceStatus_READY},
			Sources:          []*pb.NameVariant{{Name: "test_name", Variant: "test_variant"}},
		}, nil
	}
	return nil, io.EOF
}

func (c ProviderClient) CloseSend() error {
	return nil
}

type MockGrpcConn struct {
	pb.MetadataClient
}

type MockMetadataClient struct {
	GrpcConn MockGrpcConn
}

func (m MockGrpcConn) GetSourceVariants(context.Context, ...grpc.CallOption) (pb.Metadata_GetSourceVariantsClient, error) {
	return SourceClient{}, nil
}

func (m MockGrpcConn) GetProviders(context.Context, ...grpc.CallOption) (pb.Metadata_GetProvidersClient, error) {
	return ProviderClient{}, nil
}

func TestValidTicket(t *testing.T) {
	sourceCalls = 0
	providerCalls = 0

	proxyFlightServer := &GoProxyServer{
		streamerAddress: "test-address:8080",
		logger:          logging.NewLogger("iceberg-proxy-test"),
		metadata: &metadata.Client{
			GrpcConn: MockGrpcConn{},
		},
	}

	var proxyBytes = []byte(`{"source": "test_name", 
		"variant": "test_variant",
		"resourceType":"someResource",
		"limit": 5}`)

	ticket := flight.Ticket{
		Ticket: proxyBytes,
	}

	hydratedTicket, err := proxyFlightServer.hydrateTicket(&ticket)

	assert.NoError(t, err, "hydrateTicket returned an error")
	assert.NotEmpty(t, hydratedTicket.Ticket)

	var ticketData = map[string]any{}

	jsonErr := json.Unmarshal(hydratedTicket.Ticket, &ticketData)
	if jsonErr != nil {
		assert.FailNow(t, "The returned config data did not marshal correctly", jsonErr)
	}

	assert.Equal(t, "default", ticketData["catalog"])
	assert.Equal(t, expectedDatabase, ticketData["namespace"])
	assert.Equal(t, expectedTable, ticketData["table"])
	assert.Equal(t, expectedRegion, ticketData["client.region"])
	assert.Equal(t, expectedAccessKey, ticketData["client.access-key-id"])
	assert.Equal(t, expectedSecretKey, ticketData["client.secret-access-key"])
	assert.Equal(t, float64(5), ticketData["limit"])

}

func TestInvalidTicket(t *testing.T) {

	proxyFlightServer := &GoProxyServer{
		streamerAddress: "test-address:8080",
		logger:          logging.NewLogger("iceberg-proxy-test"),
	}

	tests := []struct {
		name        string
		ticketMap   any
		expectedMsg string
	}{
		{
			name: "malformed or missing source key",
			ticketMap: map[string]any{
				"sour":         "someSource",
				"variant":      "someVariant",
				"resourceType": "PRIMARY",
				"limit":        5,
			},
			expectedMsg: "missing 'source' in ticket data",
		},
		{
			name: "malformed or missing variant value",
			ticketMap: map[string]any{
				"source":       "someSource",
				"variant":      "",
				"resourceType": "PRIMARY",
				"limit":        5,
			},
			expectedMsg: "missing 'variant' in ticket data",
		},
		{
			name: "malformed or missing resource type key",
			ticketMap: map[string]any{
				"source":        "someSource",
				"variant":       "someVariant",
				"resourceTypes": "PRIMARY",
				"limit":         5,
			},
			expectedMsg: "missing 'resourceType' in ticket data",
		},
		{
			name:        "malformed json",
			ticketMap:   []byte(`{i'm not valid!}`),
			expectedMsg: "failed to parse ticket JSON: json: cannot unmarshal string into Go value of type main.TicketData",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticketBytes, err := json.Marshal(tt.ticketMap)
			if err != nil {
				assert.FailNow(t, "Failed to marshal the ticketMap", "error", ticketBytes)
			}

			ticket := flight.Ticket{
				Ticket: ticketBytes,
			}

			hydratedTicket, err := proxyFlightServer.hydrateTicket(&ticket)
			assert.EqualError(t, err, tt.expectedMsg)
			assert.Nil(t, hydratedTicket, "the ticket should be 'nil'")
		})
	}

}
