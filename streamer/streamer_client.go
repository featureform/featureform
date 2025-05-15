// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.

package streamer

import (
	"encoding/json"
	"fmt"

	"github.com/featureform/core"
	"github.com/featureform/fferr"
	fs "github.com/featureform/filestore"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GlueRequestWithAssumeRole represents a request using AWS AssumeRole credentials.
type glueRequestWithAssumeRole struct {
	Location *location.CatalogLocation
	Region   string
	RoleArn  string
}

func (req glueRequestWithAssumeRole) validate() error {
	if req.Region == "" {
		return fferr.NewInternalErrorf("region is required")
	}
	if req.RoleArn == "" {
		return fferr.NewInternalErrorf("role ARN is required")
	}
	return nil
}

func (req glueRequestWithAssumeRole) ToRequest() (Request, error) {
	locReqMap, err := catalogLocationToRequest(req.Location)
	if err != nil {
		return nil, err
	}
	if err := req.validate(); err != nil {
		return nil, err
	}
	glueReqMap := Request{
		"catalog_type":    "glue",
		"client.region":   req.Region,
		"client.role-arn": req.RoleArn,
	}
	return locReqMap.Add(glueReqMap)
}

// GlueRequestWithStaticCreds represents a request using static AWS credentials.
type glueRequestWithStaticCreds struct {
	Location *location.CatalogLocation
	Region   string
	Creds    pc.AWSStaticCredentials
}

func (req glueRequestWithStaticCreds) validate() error {
	if req.Region == "" {
		return fferr.NewInternalErrorf("region is required")
	}
	if req.Creds.AccessKeyId == "" {
		return fferr.NewInternalErrorf("access key is required")
	}
	if req.Creds.SecretKey == "" {
		return fferr.NewInternalErrorf("secret key is required")
	}
	return nil
}

func (req glueRequestWithStaticCreds) ToRequest() (Request, error) {
	locReqMap, err := catalogLocationToRequest(req.Location)
	if err != nil {
		return nil, err
	}
	if err := req.validate(); err != nil {
		return nil, err
	}
	glueReqMap := Request{
		"catalog_type":             "glue",
		"client.region":            req.Region,
		"client.access-key-id":     req.Creds.AccessKeyId,
		"client.secret-access-key": req.Creds.SecretKey,
	}
	return locReqMap.Add(glueReqMap)
}

func catalogLocationToRequest(loc *location.CatalogLocation) (Request, error) {
	if loc == nil {
		return nil, fferr.NewInternalErrorf("CatalogLocation is nil")
	}
	if loc.TableFormat() != string(pc.Iceberg) {
		return nil, fferr.NewInternalErrorf("TableFormat %s not supported", loc.TableFormat())
	}
	if loc.Database() == "" {
		return nil, fferr.NewInternalErrorf("Database name is required in CatalogLocation")
	}
	if loc.Table() == "" {
		return nil, fferr.NewInternalErrorf("Table name is required in CatalogLocation")
	}
	return Request{
		"catalog":   "default",
		"namespace": loc.Database(),
		"table":     loc.Table(),
	}, nil
}

// DatasetOptions represents options for retrieving a dataset.
type DatasetOptions struct {
	Limit int64
}

func (opts DatasetOptions) ToRequest() (Request, error) {
	req := make(Request)
	limitSet := opts.Limit != 0
	if opts.Limit < 0 {
		return nil, fferr.NewInternalErrorf("Invalid Limit Value: %d", opts.Limit)
	} else if limitSet {
		req["limit"] = opts.Limit
	}
	return req, nil
}

// Request is a map for request parameters.
type Request map[string]any

type requestConfig interface {
	ToRequest() (Request, error)
}

// Add merges another Request into the current one.
// It returns an error if there are duplicate keys.
func (rm Request) Add(other Request) (Request, error) {
	clone := make(Request, len(rm))
	for k, v := range rm {
		clone[k] = v
	}
	for k, v := range other {
		if _, exists := clone[k]; exists {
			return nil, fmt.Errorf("duplicate key in request map: %s", k)
		}
		clone[k] = v
	}
	return clone, nil
}

func getS3StaticCreds(cfg pc.SparkConfig) (pc.AWSStaticCredentials, error) {
	s3Config, ok := cfg.StoreConfig.(*pc.S3FileStoreConfig)
	if !ok {
		err := fferr.NewInternalErrorf(
			"Invalid Spark Config. StoreType is %s but StoreConfig is %T",
			cfg.StoreType, cfg.StoreConfig,
		)
		return pc.AWSStaticCredentials{}, err
	}
	creds, ok := s3Config.Credentials.(pc.AWSStaticCredentials)
	if !ok {
		err := fferr.NewInternalErrorf(
			"If Glue is not using AssumeRoleArn then S3 must have static creds but has %T",
			s3Config.Credentials,
		)
		return pc.AWSStaticCredentials{}, err
	}
	return creds, nil
}

// RequestFromSparkConfig creates a Request from a SparkConfig.
func RequestFromSparkConfig(ctx *core.Context, config pc.SparkConfig, loc location.Location) (Request, error) {
	if config.GlueConfig == nil {
		err := fferr.NewInternalErrorf("streamer only supports GlueCatalog")
		ctx.Errorw("Glue config missing", "error", err)
		return nil, err
	}
	if config.StoreType != fs.S3 {
		err := fferr.NewInternalErrorf("Store type not supported by streamer: %s", config.StoreType)
		ctx.Errorw("Unsupported store type", "error", err)
		return nil, err
	}
	catalogLoc, ok := loc.(*location.CatalogLocation)
	if !ok {
		err := fmt.Errorf("Unsupported location type: %T", loc)
		ctx.Errorf("%w", err)
		return nil, fferr.NewInternalError(err)
	}
	var reqCfg requestConfig
	region := config.GlueConfig.Region
	roleArn := config.GlueConfig.AssumeRoleArn
	hasRoleArn := roleArn != ""
	if hasRoleArn {
		reqCfg = glueRequestWithAssumeRole{
			Location: catalogLoc,
			Region:   region,
			RoleArn:  roleArn,
		}
	} else {
		creds, err := getS3StaticCreds(config)
		if err != nil {
			err := fmt.Errorf(
				"Unable to create streamer, spark config doesn't have roleArn or static creds: %s", err,
			)
			ctx.Errorf("%w", err)
			return nil, fferr.NewInternalError(err)
		}
		reqCfg = glueRequestWithStaticCreds{
			Location: catalogLoc,
			Region:   region,
			Creds:    creds,
		}
	}
	return reqCfg.ToRequest()
}

func RequestFromSerializedSparkConfig(ctx *core.Context, serConf pc.SerializedConfig, loc location.Location) (Request, error) {
	config := &pc.SparkConfig{}
	if err := config.Deserialize(serConf); err != nil {
		ctx.Errorw("could not deserialize the spark config", "error", err)
		return nil, fferr.NewInternalErrorf("failed to deserialize config: %w", err)
	}
	return RequestFromSparkConfig(ctx, *config, loc)
}

type Client struct {
	rpcClient flight.Client
}

func NewClient(ctx *core.Context, addr string) (*Client, error) {
	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	sizeOption := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 * 1024 * 1024)) // 20 MB
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, insecureOption, sizeOption)
	if err != nil {
		ctx.Errorw("failed to create flight client", "error", err)
		return nil, fferr.NewInternalErrorf("failed to create flight client: %w", err)
	}
	return &Client{rpcClient: client}, nil
}

func (c *Client) Close() error {
	if err := c.rpcClient.Close(); err != nil {
		return fferr.NewInternalErrorf("failed to close flight client: %w", err)
	}
	return nil
}

func (c *Client) GetSchema(ctx *core.Context, req Request) (*arrowlib.Schema, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		ctx.Errorw("failed to marshal request", "error", err)
		return nil, fferr.NewInternalErrorf("failed to marshal request: %w", err)
	}
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  reqBytes,
	}
	schemaResult, err := c.rpcClient.GetSchema(ctx, desc)
	if err != nil {
		ctx.Errorw("error fetching schema", "error", err)
		return nil, fferr.NewInternalErrorf("error fetching schema: %w", err)
	}
	schema, err := flight.DeserializeSchema(schemaResult.GetSchema(), memory.DefaultAllocator)
	if err != nil {
		ctx.Errorw("failed to deserialize schema", "error", err)
		return nil, fferr.NewInternalErrorf("failed to deserialize schema: %w", err)
	}
	return schema, nil
}

func (c *Client) GetReader(ctx *core.Context, req Request, opts DatasetOptions) (*flight.Reader, error) {
	reqBytes, err := c.marshalRequest(ctx, req, opts)
	if err != nil {
		ctx.Errorw("failed to marshal request", "error", err)
		return nil, fferr.NewInternalErrorf("failed to marshal request: %w", err)
	}
	ticket := &flight.Ticket{Ticket: reqBytes}
	stream, err := c.rpcClient.DoGet(ctx, ticket)
	if err != nil {
		ctx.Errorw("error fetching dataset", "error", err)
		return nil, fferr.NewInternalErrorf("error fetching dataset: %w", err)
	}
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		ctx.Errorw("failed to create record reader", "error", err)
		return nil, fferr.NewInternalErrorf("failed to create record reader: %w", err)
	}
	return reader, nil
}

func (c *Client) marshalRequest(ctx *core.Context, req Request, opts DatasetOptions) ([]byte, error) {
	optsReq, err := opts.ToRequest()
	if err != nil {
		err := fferr.NewInternalErrorf("Failed to create request: %s", err)
		ctx.Errorf("%w", err)
		return nil, err
	}
	joinedReq, err := req.Add(optsReq)
	if err != nil {
		err := fferr.NewInternalErrorf("Failed to create request: %s", err)
		ctx.Errorf("%w", err)
		return nil, err
	}
	return json.Marshal(joinedReq)
}
