package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	grpc_status "google.golang.org/grpc/status"

	"github.com/joho/godotenv"

	"google.golang.org/grpc/credentials/insecure"

	health "github.com/featureform/health"
	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	srv "github.com/featureform/proto"
	pt "github.com/featureform/provider/provider_type"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ApiServer struct {
	Logger     *zap.SugaredLogger
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	metadata   MetadataServer
	online     OnlineServer
}

type MetadataServer struct {
	address string
	Logger  *zap.SugaredLogger
	meta    pb.MetadataClient
	client  *metadata.Client
	pb.UnimplementedApiServer
	health *health.Health
}

type OnlineServer struct {
	Logger  *zap.SugaredLogger
	address string
	client  srv.FeatureClient
	srv.UnimplementedFeatureServer
}

func NewApiServer(logger *zap.SugaredLogger, address string, metaAddr string, srvAddr string) (*ApiServer, error) {
	return &ApiServer{
		Logger:  logger,
		address: address,
		metadata: MetadataServer{
			address: metaAddr,
			Logger:  logger,
		},
		online: OnlineServer{
			Logger:  logger,
			address: srvAddr,
		},
	}, nil
}

func (serv *MetadataServer) CreateUser(ctx context.Context, user *pb.User) (*pb.Empty, error) {
	serv.Logger.Infow("Creating User", "user", user.Name)
	return serv.meta.CreateUser(ctx, user)
}

func (serv *MetadataServer) GetUsers(stream pb.Api_GetUsersServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetUsers(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetFeatures(stream pb.Api_GetFeaturesServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetFeatures(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Api_GetFeatureVariantsServer) error {
	for {
		nameVariant, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetFeatureVariants(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabels(stream pb.Api_GetLabelsServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetLabels(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Api_GetLabelVariantsServer) error {
	for {
		nameVariant, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetLabelVariants(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSources(stream pb.Api_GetSourcesServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetSources(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Api_GetSourceVariantsServer) error {
	for {
		nameVariant, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetSourceVariants(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Api_GetTrainingSetsServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetTrainingSets(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Api_GetTrainingSetVariantsServer) error {
	for {
		nameVariant, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetTrainingSetVariants(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetProviders(stream pb.Api_GetProvidersServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetProviders(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEntities(stream pb.Api_GetEntitiesServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetEntities(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetModels(stream pb.Api_GetModelsServer) error {
	for {
		name, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetModels(stream.Context())
		if err != nil {
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEquivalent(ctx context.Context, req *pb.ResourceVariant) (*pb.ResourceVariant, error) {
	return serv.meta.GetEquivalent(ctx, req)
}

func (serv *MetadataServer) ListUsers(in *pb.Empty, stream pb.Api_ListUsersServer) error {
	proxyStream, err := serv.meta.ListUsers(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListFeatures(in *pb.Empty, stream pb.Api_ListFeaturesServer) error {
	proxyStream, err := serv.meta.ListFeatures(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListLabels(in *pb.Empty, stream pb.Api_ListLabelsServer) error {
	proxyStream, err := serv.meta.ListLabels(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListSources(in *pb.Empty, stream pb.Api_ListSourcesServer) error {
	proxyStream, err := serv.meta.ListSources(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListTrainingSets(in *pb.Empty, stream pb.Api_ListTrainingSetsServer) error {
	proxyStream, err := serv.meta.ListTrainingSets(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListModels(in *pb.Empty, stream pb.Api_ListModelsServer) error {
	proxyStream, err := serv.meta.ListModels(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListEntities(in *pb.Empty, stream pb.Api_ListEntitiesServer) error {
	proxyStream, err := serv.meta.ListEntities(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListProviders(in *pb.Empty, stream pb.Api_ListProvidersServer) error {
	proxyStream, err := serv.meta.ListProviders(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
	}
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
	// The existence of a provider is part of the determination for checking provider health, hence why it
	// needs to happen prior to the call to CreateProvider, which is an upsert operation.
	shouldCheckProviderHealth, err := serv.shouldCheckProviderHealth(ctx, provider)
	if err != nil {
		return nil, err
	}
	serv.Logger.Infow("Creating Provider", "name", provider.Name)
	_, err = serv.meta.CreateProvider(ctx, provider)
	if err != nil && grpc_status.Code(err) != codes.AlreadyExists {
		serv.Logger.Errorw("Failed to create provider", "error", err)
		return nil, err
	}
	if !serv.health.IsSupportedProvider(pt.Type(provider.Type)) {
		serv.Logger.Infow("Provider type is currently not supported for health check", "type", provider.Type)
		return &pb.Empty{}, nil
	}
	if shouldCheckProviderHealth {
		serv.Logger.Infow("Checking provider health", "name", provider.Name)
		err := serv.checkProviderHealth(ctx, provider.Name)
		if err != nil {
			serv.Logger.Errorw("Failed to set provider status", "error", err, "health check error", err)
			return nil, err
		}
	}
	return &pb.Empty{}, err
}

func (serv *MetadataServer) shouldCheckProviderHealth(ctx context.Context, provider *pb.Provider) (bool, error) {
	var existingProvider *pb.Provider
	for {
		stream, err := serv.meta.GetProviders(ctx)
		if err != nil {
			return false, err
		}
		if err := stream.Send(&pb.Name{Name: provider.Name}); err != nil {
			return false, err
		}
		res, err := stream.Recv()
		if grpc_status.Code(err) == codes.NotFound {
			break
		}
		if err != nil {
			return false, err
		}
		if res.Name == provider.Name && res.Type == provider.Type {
			existingProvider = res
			break
		}
	}
	// We should check provider health if:
	// 1. The provider does not exist
	// 2. The provider exists but the config has changed
	// 3. The provider exists but the previous health check failed
	return (existingProvider == nil ||
			!bytes.Equal(existingProvider.SerializedConfig, provider.SerializedConfig) ||
			(existingProvider.Status != nil && existingProvider.Status.Status == pb.ResourceStatus_FAILED)),
		nil
}

func (serv *MetadataServer) checkProviderHealth(ctx context.Context, providerName string) error {
	var status *pb.ResourceStatus
	isHealthy, err := serv.health.CheckProvider(providerName)
	if err != nil || !isHealthy {
		serv.Logger.Errorw("Provider health check failed", "error", err)

		errorStatus, ok := grpc_status.FromError(err)
		errorProto := errorStatus.Proto()
		var errorStatusProto *pb.ErrorStatus
		if ok {
			errorStatusProto = &pb.ErrorStatus{Code: errorProto.Code, Message: errorProto.Message, Details: errorProto.Details}
		} else {
			errorStatusProto = nil
		}

		status = &pb.ResourceStatus{
			Status:       pb.ResourceStatus_FAILED,
			ErrorMessage: err.Error(),
			ErrorStatus:  errorStatusProto,
		}
	} else {
		serv.Logger.Infow("Provider health check passed", "name", providerName)
		status = &pb.ResourceStatus{
			Status: pb.ResourceStatus_READY,
		}
	}
	statusReq := &pb.SetStatusRequest{
		ResourceId: &pb.ResourceID{
			Resource: &pb.NameVariant{
				Name: providerName,
			},
			ResourceType: pb.ResourceType_PROVIDER,
		},
		Status: status,
	}
	_, statusErr := serv.meta.SetResourceStatus(ctx, statusReq)
	return statusErr
}

func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, source *pb.SourceVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Source Variant", "name", source.Name, "variant", source.Variant)
	switch casted := source.Definition.(type) {
	case *pb.SourceVariant_Transformation:
		switch transformationType := casted.Transformation.Type.(type) {
		case *pb.Transformation_SQLTransformation:
			serv.Logger.Infow("Retreiving the sources from SQL Transformation", transformationType)
			transformation := casted.Transformation.Type.(*pb.Transformation_SQLTransformation).SQLTransformation
			qry := transformation.Query
			numEscapes := strings.Count(qry, "{{")
			sources := make([]*pb.NameVariant, numEscapes)
			for i := 0; i < numEscapes; i++ {
				split := strings.SplitN(qry, "{{", 2)
				afterSplit := strings.SplitN(split[1], "}}", 2)
				key := strings.TrimSpace(afterSplit[0])
				nameVariant := strings.SplitN(key, ".", 2)
				sources[i] = &pb.NameVariant{Name: nameVariant[0], Variant: nameVariant[1]}
				qry = afterSplit[1]
			}
			source.Definition.(*pb.SourceVariant_Transformation).Transformation.Type.(*pb.Transformation_SQLTransformation).SQLTransformation.Source = sources
		}
	}
	return serv.meta.CreateSourceVariant(ctx, source)
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entity *pb.Entity) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Entity", "entity", entity.Name)
	return serv.meta.CreateEntity(ctx, entity)
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Requesting Schedule Change", "resource", req.ResourceId, "new schedule", req.Schedule)
	return serv.meta.RequestScheduleChange(ctx, req)
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, feature *pb.FeatureVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Feature Variant", "name", feature.Name, "variant", feature.Variant)
	return serv.meta.CreateFeatureVariant(ctx, feature)
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, label *pb.LabelVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Label Variant", "name", label.Name, "variant", label.Variant)
	protoSource := label.Source
	serv.Logger.Debugw("Finding label source", "name", protoSource.Name, "variant", protoSource.Variant)
	source, err := serv.client.GetSourceVariant(ctx, metadata.NameVariant{Name: protoSource.Name, Variant: protoSource.Variant})
	if err != nil {
		serv.Logger.Errorw("Could not create label source variant", "error", err)
		return nil, err
	}
	label.Provider = source.Provider()
	resp, err := serv.meta.CreateLabelVariant(ctx, label)
	serv.Logger.Debugw("Created label variant", "response", resp)
	if err != nil {
		serv.Logger.Errorw("Could not create label variant", "response", resp, "error", err)
	}
	return resp, err
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, train *pb.TrainingSetVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Training Set Variant", "name", train.Name, "variant", train.Variant)
	protoLabel := train.Label
	label, err := serv.client.GetLabelVariant(ctx, metadata.NameVariant{Name: protoLabel.Name, Variant: protoLabel.Variant})
	if err != nil {
		return nil, err
	}
	for _, protoFeature := range train.Features {
		_, err := serv.client.GetFeatureVariant(ctx, metadata.NameVariant{Name: protoFeature.Name, Variant: protoFeature.Variant})
		if err != nil {
			return nil, err
		}
	}
	train.Provider = label.Provider()
	return serv.meta.CreateTrainingSetVariant(ctx, train)
}

func (serv *MetadataServer) CreateModel(ctx context.Context, model *pb.Model) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Model", "model", model.Name)
	return serv.meta.CreateModel(ctx, model)
}

func (serv *MetadataServer) CreateTrigger(ctx context.Context, feature *pb.Trigger) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Trigger", "trigger", feature.String())
	return serv.meta.CreateTrigger(ctx, feature)
}

func (serv *MetadataServer) AddTrigger(ctx context.Context, trigger *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Adding Trigger", "trigger", trigger.String())
	return serv.meta.AddTrigger(ctx, trigger)
}

func (serv *MetadataServer) RemoveTrigger(ctx context.Context, trigger *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Removing Trigger", "trigger", trigger.String())
	return serv.meta.RemoveTrigger(ctx, trigger)
}

func (serv *MetadataServer) UpdateTrigger(ctx context.Context, trigger *pb.Trigger) (*pb.Empty, error) {
	serv.Logger.Infow("Updating Trigger", "trigger", trigger.String())
	return serv.meta.UpdateTrigger(ctx, trigger)
}

func (serv *MetadataServer) DeleteTrigger(ctx context.Context, trigger *pb.Trigger) (*pb.Empty, error) {
	serv.Logger.Infow("Deleting Trigger", "trigger", trigger.String())
	return serv.meta.DeleteTrigger(ctx, trigger)
}

func (serv *OnlineServer) FeatureServe(ctx context.Context, req *srv.FeatureServeRequest) (*srv.FeatureRow, error) {
	serv.Logger.Infow("Serving Features", "request", req.String())
	return serv.client.FeatureServe(ctx, req)
}

func (serv *OnlineServer) BatchFeatureServe(req *srv.BatchFeatureServeRequest, stream srv.Feature_BatchFeatureServeServer) error {
	serv.Logger.Infow("Serving Batch Features", "request", req.String())
	client, err := serv.client.BatchFeatureServe(context.Background(), req)
	if err != nil {
		return fmt.Errorf("could not serve batch features: %w", err)
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.Send(row); err != nil {
			serv.Logger.Errorw("Failed to write to stream", "Error", err)
			return err
		}
	}

}

func (serv *OnlineServer) TrainingData(req *srv.TrainingDataRequest, stream srv.Feature_TrainingDataServer) error {
	serv.Logger.Infow("Serving Training Data", "id", req.Id.String())
	client, err := serv.client.TrainingData(context.Background(), req)
	if err != nil {
		return err
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.Send(row); err != nil {
			serv.Logger.Errorw("Failed to write to stream", "Error", err)
			return err
		}
	}
}

func (serv *OnlineServer) TrainingTestSplit(stream srv.Feature_TrainingTestSplitServer) error {
	serv.Logger.Infow("Starting Training Test Split Stream")
	clientStream, err := serv.client.TrainingTestSplit(context.Background())
	if err != nil {
		return fmt.Errorf("could not serve training test split: %w", err)
	}

	// Use a channel to communicate errors from goroutines
	//errChan := make(chan error, 2) // Buffer to hold at most two errors (send and receive)

	// Goroutine for forwarding requests to the downstream service
	//go func() {
	//	defer close(errChan)
	for {
		req, err := stream.Recv()
		fmt.Println("this is the request", req.String())
		if err == io.EOF {
			// Client has closed the stream, close the downstream stream
			serv.Logger.Infow("Client has closed the stream")
			if err := clientStream.CloseSend(); err != nil {
				return fmt.Errorf("failed to close send direction to downstream service: %w", err)
			}
			return nil
		}
		if err != nil {
			serv.Logger.Errorw("Error receiving from client stream", "error", err)
			return err
		}

		// Forward the request to the downstream service
		if err := clientStream.Send(req); err != nil {
			serv.Logger.Errorw("Failed to send request to downstream service", "error", err)
			return err
		}

		resp, err := clientStream.Recv()
		fmt.Println("this is the response", resp.String(), err)
		if err == io.EOF {
			// End of stream from downstream service
			serv.Logger.Infow("Downstream service has closed the stream")
			return nil
		}
		if err != nil {
			serv.Logger.Errorw("Error receiving from downstream service", "error", err)
			return err
		}

		// Send the response back to the client
		if err := stream.Send(resp); err != nil {
			serv.Logger.Errorw("Failed to send response to client", "error", err)
			return err
		}
	}
	//}()

	//// Goroutine for receiving responses from the downstream service and forwarding them to the client
	//go func() {
	//	for {
	//		resp, err := clientStream.Recv()
	//		fmt.Println("this is the response", resp.String(), err)
	//		if err == io.EOF {
	//			// End of stream from downstream service
	//			serv.Logger.Infow("Downstream service has closed the stream")
	//			return
	//		}
	//		if err != nil {
	//			serv.Logger.Errorw("Error receiving from downstream service", "error", err)
	//			errChan <- err
	//			return
	//		}
	//
	//		// Send the response back to the client
	//		if err := stream.Send(resp); err != nil {
	//			serv.Logger.Errorw("Failed to send response to client", "error", err)
	//			errChan <- err
	//			return
	//		}
	//	}
	//}()
}

func (serv *OnlineServer) TrainingDataColumns(ctx context.Context, req *srv.TrainingDataColumnsRequest) (*srv.TrainingColumns, error) {
	serv.Logger.Infow("Serving Training Set Columns", "id", req.Id.String())
	return serv.client.TrainingDataColumns(ctx, req)
}

func (serv *OnlineServer) SourceData(req *srv.SourceDataRequest, stream srv.Feature_SourceDataServer) error {
	serv.Logger.Infow("Serving Source Data", "id", req.Id.String())
	if req.Limit == 0 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("limit must be greater than 0"))
	}
	client, err := serv.client.SourceData(context.Background(), req)
	if err != nil {
		return err
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.Send(row); err != nil {
			serv.Logger.Errorf("failed to write to source data stream: %w", err)
			return err
		}
	}
}

func (serv *OnlineServer) SourceColumns(ctx context.Context, req *srv.SourceColumnRequest) (*srv.SourceDataColumns, error) {
	serv.Logger.Infow("Serving Source Columns", "id", req.Id.String())
	return serv.client.SourceColumns(ctx, req)
}

func (serv *OnlineServer) Nearest(ctx context.Context, req *srv.NearestRequest) (*srv.NearestResponse, error) {
	serv.Logger.Infow("Serving Nearest", "id", req.Id.String())
	return serv.client.Nearest(ctx, req)
}

func (serv *OnlineServer) GetResourceLocation(ctx context.Context, req *srv.ResourceIdRequest) (*srv.ResourceLocation, error) {
	serv.Logger.Infow("Serving Resource Location", "resource", req.String())
	return serv.client.GetResourceLocation(ctx, req)
}

func (serv *ApiServer) Serve() error {

	if serv.grpcServer != nil {
		return fferr.NewInternalError(fmt.Errorf("server already running"))
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// grpc.WithUnaryInterceptor(fferr.UnaryClientInterceptor()),
		// grpc.WithStreamInterceptor(fferr.StreamClientInterceptor()),
	}
	metaConn, err := grpc.Dial(serv.metadata.address, opts...)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	servConn, err := grpc.Dial(serv.online.address, opts...)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	serv.metadata.meta = pb.NewMetadataClient(metaConn)
	client, err := metadata.NewClient(serv.metadata.address, serv.Logger)
	if err != nil {
		return err
	}
	serv.metadata.client = client
	serv.online.client = srv.NewFeatureClient(servConn)
	serv.metadata.health = health.NewHealth(client)
	return serv.ServeOnListener(lis)
}

func (serv *ApiServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	var (
		logrusLogger = logrus.New()
		customFunc   = func(code codes.Code) logrus.Level {
			if code == codes.OK {
				return logrus.DebugLevel
			}
			return logrus.DebugLevel
		}
	)
	logrusEntry := logrus.NewEntry(logrusLogger)
	lorgusOpts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(customFunc),
	}
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)
	minTimeStr := helpers.GetEnv("FEATUREFORM_KEEPALIVE_MINTIME", "1")
	minTime, err := strconv.ParseInt(minTimeStr, 0, 0)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	kaep := keepalive.EnforcementPolicy{
		MinTime: time.Duration(minTime) * time.Minute, // minimum amount of time a client should wait before sending a keepalive ping
	}
	kaTimeout := helpers.GetEnv("FEATUREFORM_KEEPALIVE_TIMEOUT", "5")
	timeout, err := strconv.ParseInt(kaTimeout, 0, 0)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	kasp := keepalive.ServerParameters{
		Timeout: time.Duration(timeout) * time.Minute, // time after which the connection is closed if no activity
	}
	opt := []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(
			grpc_logrus.UnaryServerInterceptor(logrusEntry, lorgusOpts...),
		),
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	}
	grpcServer := grpc.NewServer(opt...)
	reflection.Register(grpcServer)
	pb.RegisterApiServer(grpcServer, &serv.metadata)
	srv.RegisterFeatureServer(grpcServer, &serv.online)
	serv.grpcServer = grpcServer
	serv.Logger.Infow("Server starting", "Address", serv.listener.Addr().String())
	return grpcServer.Serve(lis)
}

func (serv *ApiServer) GracefulStop() error {
	if serv.grpcServer == nil {
		return fferr.NewInternalError(fmt.Errorf("server not running"))
	}
	serv.grpcServer.GracefulStop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.WriteHeader(http.StatusOK)

	_, err := io.WriteString(w, "OK")
	if err != nil {
		fmt.Printf("health check write response error: %+v", err)
	}

}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	_, err := io.WriteString(w, `<html><body>Welcome to featureform</body></html>`)
	if err != nil {
		fmt.Printf("index / write response error: %+v", err)
	}

}

func startHttpsServer(port string) error {
	mux := &http.ServeMux{}

	// Health check endpoint will handle all /_ah/* requests
	// e.g. /_ah/live, /_ah/ready and /_ah/lb
	// Create separate routes for specific health requests as needed.
	mux.HandleFunc("/_ah/", handleHealthCheck)
	mux.HandleFunc("/", handleIndex)
	// Add more routes as needed.

	// Set timeouts so that a slow or malicious client doesn't hold resources forever.
	httpsSrv := &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
		Handler:      mux,
		Addr:         port,
	}

	fmt.Printf("starting HTTP server on port %s", port)

	return httpsSrv.ListenAndServe()
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	apiPort := help.GetEnv("API_PORT", "7878")
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	servingHost := help.GetEnv("SERVING_HOST", "localhost")
	servingPort := help.GetEnv("SERVING_PORT", "8080")
	apiConn := fmt.Sprintf("0.0.0.0:%s", apiPort)
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	servingConn := fmt.Sprintf("%s:%s", servingHost, servingPort)
	logger := logging.NewLogger("api")
	go func() {
		err := startHttpsServer(":8443")
		if err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("health check HTTP server failed: %+v", err))
		}
	}()
	serv, err := NewApiServer(logger, apiConn, metadataConn, servingConn)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(serv.Serve())
}
