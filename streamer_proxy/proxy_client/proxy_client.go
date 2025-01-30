package proxy_client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/featureform/fferr"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StreamProxyClient struct {
	client       flight.Client
	flightStream flight.FlightService_DoGetClient
	recordReader *flight.Reader
	currentBatch arrow.Record
	columns      []string
	logger       logging.Logger
}

func GetStreamProxyClient(ctx context.Context, source, variant string, limit int) (*StreamProxyClient, error) {
	proxyHost := help.GetEnv("ICEBERG_PROXY_HOST", "localhost")
	proxyPort := help.GetEnv("ICEBERG_PROXY_PORT", "8086")

	baseLogger := logging.NewLogger("stream-iterator")

	if proxyHost == "" {
		envErr := fmt.Errorf("missing ICEBERG_PROXY_HOST env variable")
		baseLogger.Error(envErr.Error())
		return nil, fferr.NewInternalError(envErr)
	}

	if proxyPort == "" {
		envErr := fmt.Errorf("missing ICEBERG_PROXY_PORT env variable")
		baseLogger.Error(envErr.Error())
		return nil, fferr.NewInternalError(envErr)
	}

	if source == "" {
		sourceErr := fmt.Errorf("missing 'source' param value")
		baseLogger.Error(sourceErr.Error())
		return nil, fferr.NewInternalError(sourceErr)
	}

	if variant == "" {
		variantErr := fmt.Errorf("missing 'variant' param value")
		baseLogger.Error(variantErr.Error())
		return nil, fferr.NewInternalError(variantErr)
	}

	if limit < 0 {
		limitErr := fmt.Errorf("limit value (%d) is less than 0", limit)
		baseLogger.Error(limitErr.Error())
		return nil, fferr.NewInternalError(limitErr)
	}

	proxyAddress := fmt.Sprintf("%s:%s", proxyHost, proxyPort)
	baseLogger.Infof("Received stream request, forwarding to iceberg-proxy at: %s", proxyAddress)

	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	sizeOption := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 * 1024 * 1024)) //20 MB

	client, clientErr := flight.NewClientWithMiddleware(proxyAddress, nil, nil, insecureOption, sizeOption)
	if clientErr != nil {
		baseLogger.Errorf("Failed to connect to the iceberg-proxy: %v", clientErr)
		return nil, clientErr
	}

	baseLogger.Info("Connection established! Preparing the ticket for the proxy...")
	ticketData := map[string]interface{}{
		"source":       source,
		"variant":      variant,
		"resourceType": "-",
		"limit":        limit,
	}

	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		return nil, fmt.Errorf("failed to create ticket: %w", err)
	}

	ticket := &flight.Ticket{Ticket: ticketBytes}

	baseLogger.Info("Fetching the data stream...")
	flightStream, err := client.DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data for source (%s) and variant (%s) from proxy: %w", source, variant, err)
	}

	baseLogger.Info("Creating the record reader...")
	recordReader, err := flight.NewRecordReader(flightStream)
	if err == io.EOF {
		// initial connection ok, no data
		return nil, fmt.Errorf("connection established, but no data available for source (%s) and variant (%s)", source, variant)
	} else if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	// pull the column names
	var columns []string
	if recordReader.Schema() != nil {
		for _, field := range recordReader.Schema().Fields() {
			columns = append(columns, field.Name)
		}
	}

	return &StreamProxyClient{
		client:       client,
		flightStream: flightStream,
		recordReader: recordReader,
		columns:      columns,
		logger:       baseLogger,
	}, nil
}

func (si *StreamProxyClient) Next() bool {
	hasNext := si.recordReader.Next()
	si.currentBatch = si.recordReader.Record()
	if !hasNext {
		si.currentBatch = nil
	}
	return hasNext
}

func (si StreamProxyClient) Values() provider.GenericRecord {
	if si.currentBatch == nil {
		return nil
	}
	rowMatrix := make(provider.GenericRecord, si.currentBatch.NumRows())

	for i := 0; i < int(si.currentBatch.NumCols()); i++ {
		currentCol := si.currentBatch.Column(i)

		for cr := 0; cr < currentCol.Len(); cr++ {
			cellString := currentCol.ValueStr(cr)

			if rowMatrix[cr] == nil {
				rowMatrix[cr] = []string{}
			}
			rowMatrix[cr] = append(rowMatrix[cr].([]string), cellString)
		}
	}
	return rowMatrix
}

func (si StreamProxyClient) Columns() []string {
	return si.columns
}

func (si StreamProxyClient) Err() error {
	return si.recordReader.Err()
}

func (si StreamProxyClient) Close() error {
	if si.flightStream != nil {
		closeErr := si.flightStream.CloseSend()
		if closeErr != nil {
			si.logger.Errorf("The flight stream CloseSend() returned an error: %v", closeErr)
		}
	}
	return si.client.Close()
}
