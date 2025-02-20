package proxy_client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"net/url"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/featureform/fferr"
	"github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StreamProxyClient struct {
	client          flight.Client
	flightStream    flight.FlightService_DoGetClient
	recordReader    *flight.Reader
	schema          *arrow.Schema
	currentBatch    arrow.Record
	columns         []string
	logger          logging.Logger
	featureformRows [][]fftypes.FeatureformValue // todox: standin for the new type pkg
}

type ProxyQuery struct {
	Source  string
	Variant string
	Limit   int
}

type ProxyConfig struct {
	Host string
	Port string
}

type ProxyRequest struct {
	Query  ProxyQuery
	Config ProxyConfig
}

func GetStreamProxyClient(ctx context.Context, proxyReq ProxyRequest) (*StreamProxyClient, error) {

	baseLogger := logging.GetLoggerFromContext(ctx)

	if proxyReq.Config.Host == "" {
		envErr := fmt.Errorf("missing 'host' param value")
		baseLogger.Error(envErr.Error())
		return nil, fferr.NewInternalError(envErr)
	}

	if proxyReq.Config.Port == "" {
		envErr := fmt.Errorf("missing 'port' param value")
		baseLogger.Error(envErr.Error())
		return nil, fferr.NewInternalError(envErr)
	}

	if proxyReq.Query.Source == "" {
		sourceErr := fmt.Errorf("missing 'source' param value")
		baseLogger.Error(sourceErr.Error())
		return nil, fferr.NewInternalError(sourceErr)
	}

	if proxyReq.Query.Variant == "" {
		variantErr := fmt.Errorf("missing 'variant' param value")
		baseLogger.Error(variantErr.Error())
		return nil, fferr.NewInternalError(variantErr)
	}

	if proxyReq.Query.Limit < 0 {
		limitErr := fmt.Errorf("limit value (%d) is less than 0", proxyReq.Query.Limit)
		baseLogger.Error(limitErr.Error())
		return nil, fferr.NewInternalError(limitErr)
	}

	parsedUrl, parseErr := url.Parse(fmt.Sprintf("%s:%s", proxyReq.Config.Host, proxyReq.Config.Port))
	if parseErr != nil {
		baseLogger.Errorw("could not parse proxy URL", "host", proxyReq.Config.Host, "port", proxyReq.Config.Port)
		return nil, fferr.NewInternalError(parseErr)
	}
	proxyAddress := parsedUrl.String()

	baseLogger.Infow("Forwarding to iceberg-proxy", "proxy_address", proxyAddress)
	baseLogger.Debugw("Forwarding parameters", "source", proxyReq.Query.Source, "variant", proxyReq.Query.Variant, "limit", proxyReq.Query.Limit)

	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	sizeOption := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 * 1024 * 1024)) //20 MB

	client, clientErr := flight.NewClientWithMiddleware(proxyAddress, nil, nil, insecureOption, sizeOption)
	if clientErr != nil {
		baseLogger.Errorf("Failed to connect to the iceberg-proxy: %v", clientErr)
		return nil, clientErr
	}

	baseLogger.Info("Connection established! Preparing the ticket for the proxy...")
	ticketData := map[string]interface{}{
		"source":       proxyReq.Query.Source,
		"variant":      proxyReq.Query.Variant,
		"resourceType": "-", // todo: we can remove this prop at this step later on.
		"limit":        proxyReq.Query.Limit,
	}

	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		ticketErr := fmt.Errorf("failed to create ticket: %w", err)
		baseLogger.Error(ticketErr.Error())
		return nil, fferr.NewInternalError(ticketErr)
	}

	ticket := &flight.Ticket{Ticket: ticketBytes}

	baseLogger.Info("Fetching the data stream...")
	flightStream, err := client.DoGet(ctx, ticket)
	if err != nil {
		doGetErr := fmt.Errorf("failed to fetch data for source (%s) and variant (%s) from proxy: %w", proxyReq.Query.Source, proxyReq.Query.Variant, err)
		baseLogger.Error(doGetErr.Error())
		return nil, fferr.NewInternalError(doGetErr)
	}

	baseLogger.Info("Creating the record reader...")
	recordReader, err := flight.NewRecordReader(flightStream)
	if err == io.EOF {
		// initial connection ok, no data
		readErr := fmt.Errorf("connection established, but no data available for source (%s) and variant (%s)", proxyReq.Query.Source, proxyReq.Query.Variant)
		baseLogger.Error(readErr.Error())
		return nil, fferr.NewInternalError(readErr)
	} else if err != nil {
		readErr := fmt.Errorf("failed to create record reader: %w", err)
		baseLogger.Error(readErr.Error())
		return nil, fferr.NewInternalError(readErr)
	}

	// pull the schema and column names
	var schema = recordReader.Schema()
	var columns []string
	if schema != nil {
		for _, field := range recordReader.Schema().Fields() {
			columns = append(columns, field.Name)
		}

	}

	return &StreamProxyClient{
		client:       client,
		flightStream: flightStream,
		recordReader: recordReader,
		columns:      columns,
		schema:       schema,
		logger:       baseLogger,
	}, nil
}

func (si *StreamProxyClient) Next() bool {
	hasNext := si.recordReader.Next()
	si.currentBatch = si.recordReader.Record()
	si.featureformRows = nil

	if !hasNext {
		si.logger.Debug("recordReader.Next() returned false (no more records), setting currentBatch to nil")
		si.currentBatch = nil
		return false
	}

	// todox: Ali make a "FeatureformRow" OF FeatureformValues
	rowMatrix := make([][]fftypes.FeatureformValue, si.currentBatch.NumRows())

	for i := 0; i < int(si.currentBatch.NumCols()); i++ {
		currentCol := si.currentBatch.Column(i)
		columnType := si.schema.Fields()[i].Type.String()

		for cr := 0; cr < currentCol.Len(); cr++ {
			if rowMatrix[cr] == nil {
				rowMatrix[cr] = []fftypes.FeatureformValue{}
			}
			ffValue := fftypes.NewFeatureformValue("Iceberg", columnType, currentCol.ValueStr(cr))
			rowMatrix[cr] = append(rowMatrix[cr], ffValue)
		}
	}
	si.featureformRows = rowMatrix
	return true
}

// note: arrow uses columnar data
func (si *StreamProxyClient) Values() [][]fftypes.FeatureformValue {
	if si.featureformRows == nil {
		si.logger.Warn("Record reader current batch is nil; returning nil")
		return nil
	}
	return si.featureformRows
}

func (si *StreamProxyClient) Columns() []string {
	if si.columns == nil {
		si.logger.Warn("columns is nil; returning an empty string slice")
		return []string{}
	}
	return si.columns
}

func (si *StreamProxyClient) Schema() arrow.Schema {
	if si.schema == nil {
		si.logger.Warn("Schema is nil; returning an empty schema")
		return arrow.Schema{}
	}
	return *si.schema
}

func (si *StreamProxyClient) Err() error {
	return si.recordReader.Err()
}

func (si *StreamProxyClient) Close() error {
	if si.flightStream != nil {
		closeErr := si.flightStream.CloseSend()
		if closeErr != nil {
			si.logger.Errorf("The flight stream CloseSend() returned an error: %v", closeErr)
		}
	}
	return si.client.Close()
}
