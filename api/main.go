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
	"google.golang.org/grpc"
)

type ApiServer struct {
	Logger     logging.Logger
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	metadata   MetadataServer
	online     OnlineServer
}

type MetadataServer struct {
	address string
	Logger  logging.Logger
	meta    pb.MetadataClient
	client  *metadata.Client
	pb.UnimplementedApiServer
	health *health.Health
}

type OnlineServer struct {
	Logger  logging.Logger
	address string
	client  srv.FeatureClient
	srv.UnimplementedFeatureServer
}

func NewApiServer(logger logging.Logger, address string, metaAddr string, srvAddr string) (*ApiServer, error) {
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

func (serv *MetadataServer) CreateUser(ctx context.Context, userRequest *pb.UserRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("user", userRequest.User.Name, "")
	logger.Infow("Creating User")
	userRequest.RequestId = requestID
	return serv.meta.CreateUser(ctx, userRequest)
}

func (serv *MetadataServer) GetUsers(stream pb.Api_GetUsersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get user %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetUsers(ctx)
		if err != nil {
			logger.Errorf("Failed to get users from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive users from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send users to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetFeatures(stream pb.Api_GetFeaturesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get feature %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetFeatures(ctx)
		if err != nil {
			logger.Errorf("Failed to get features from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive features from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send features to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Api_GetFeatureVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		nameVariant, err := stream.Recv()
		logger.Debugw("Get feature variant %v from stream", nameVariant)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetFeatureVariants(ctx)
		if err != nil {
			logger.Errorf("Failed to get feature variants from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", nameVariant, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive feature variants from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send feature variants to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabels(stream pb.Api_GetLabelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get label %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetLabels(ctx)
		if err != nil {
			logger.Errorf("Failed to get labels from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive labels from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send labels to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Api_GetLabelVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		nameVariant, err := stream.Recv()
		logger.Debugw("Get label variant %v from stream", nameVariant)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetLabelVariants(ctx)
		if err != nil {
			logger.Errorf("Failed to get label variants from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", nameVariant, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive label variants from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send label variants to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSources(stream pb.Api_GetSourcesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get source %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetSources(ctx)
		if err != nil {
			logger.Errorf("Failed to get sources from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive sources from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send sources to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Api_GetSourceVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		nameVariant, err := stream.Recv()
		logger.Debugw("Get source variant %v from stream", nameVariant)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetSourceVariants(ctx)
		if err != nil {
			logger.Errorf("Failed to get source variants from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", nameVariant, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive source variants from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send source variants to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Api_GetTrainingSetsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get training set %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetTrainingSets(ctx)
		if err != nil {
			logger.Errorf("Failed to get training sets from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive training sets from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send training sets to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Api_GetTrainingSetVariantsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		nameVariant, err := stream.Recv()
		logger.Debugw("Get training set variant %v from stream", nameVariant)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetTrainingSetVariants(ctx)
		if err != nil {
			logger.Errorf("Failed to get training set variants from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(nameVariant)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", nameVariant, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive training set variants from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send training set variants to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetProviders(stream pb.Api_GetProvidersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get provider %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetProviders(ctx)
		if err != nil {
			logger.Errorf("Failed to get providers from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive providers from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send providers to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEntities(stream pb.Api_GetEntitiesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get entity %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetEntities(ctx)
		if err != nil {
			logger.Errorf("Failed to get entities from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive entities from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send entities to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetModels(stream pb.Api_GetModelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	for {
		name, err := stream.Recv()
		logger.Debugw("Get model %v from stream", name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			serv.Logger.Errorf("Failed to read client request: %v", err)
			return err
		}
		proxyStream, err := serv.meta.GetModels(ctx)
		if err != nil {
			logger.Errorf("Failed to get models from server: %v", err)
			return err
		}
		sErr := proxyStream.Send(name)
		if sErr != nil {
			logger.Errorf("Failed to send %v to the server: %v", name, err)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			logger.Errorf("Failed to receive models from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send models to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEquivalent(ctx context.Context, req *pb.ResourceVariant) (*pb.ResourceVariant, error) {
	return serv.meta.GetEquivalent(ctx, req)
}

func (serv *MetadataServer) ListUsers(in *pb.Empty, stream pb.Api_ListUsersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Users")
	proxyStream, err := serv.meta.ListUsers(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list users: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive user from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send user to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListFeatures(in *pb.Empty, stream pb.Api_ListFeaturesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Features")
	proxyStream, err := serv.meta.ListFeatures(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list features: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive feature from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send feature to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListLabels(in *pb.Empty, stream pb.Api_ListLabelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Labels")
	proxyStream, err := serv.meta.ListLabels(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list labels: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive label from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send label to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListSources(in *pb.Empty, stream pb.Api_ListSourcesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Sources")
	proxyStream, err := serv.meta.ListSources(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list sources: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive source from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send source to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListTrainingSets(in *pb.Empty, stream pb.Api_ListTrainingSetsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Training Sets")
	proxyStream, err := serv.meta.ListTrainingSets(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list training sets: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive training set from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send training set to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListModels(in *pb.Empty, stream pb.Api_ListModelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Models")
	proxyStream, err := serv.meta.ListModels(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list models: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive model from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send model to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListEntities(in *pb.Empty, stream pb.Api_ListEntitiesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Entities")
	proxyStream, err := serv.meta.ListEntities(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list entities: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive entity from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send entity to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListProviders(in *pb.Empty, stream pb.Api_ListProvidersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Providers")
	proxyStream, err := serv.meta.ListProviders(ctx, in)
	if err != nil {
		logger.Errorf("Failed to list providers: %v", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		logger.Debugw("Getting %v from stream", res.Name)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Errorf("Failed to receive provider from server: %v", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			logger.Errorf("Failed to send provider to client: %v", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, providerRequest *pb.ProviderRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("provider", providerRequest.Provider.Name, "").WithProvider(providerRequest.Provider.Type, providerRequest.Provider.Name)
	provider := providerRequest.Provider
	logger.Infow("Creating Provider")
	providerRequest.RequestId = requestID

	// The existence of a provider is part of the determination for checking provider health, hence why it
	// needs to happen prior to the call to CreateProvider, which is an upsert operation.
	shouldCheckProviderHealth, err := serv.shouldCheckProviderHealth(ctx, provider)
	if err != nil {
		return nil, err
	}
	_, err = serv.meta.CreateProvider(ctx, providerRequest)
	if err != nil && grpc_status.Code(err) != codes.AlreadyExists {
		logger.Errorw("Failed to create provider", "error", err)
		return nil, err
	}
	if !serv.health.IsSupportedProvider(pt.Type(provider.Type)) {
		logger.Infow("Provider type is currently not supported for health check", "type", provider.Type)
		return &pb.Empty{}, nil
	}
	if shouldCheckProviderHealth {
		logger.Infow("Checking provider health", "name", provider.Name)
		err := serv.checkProviderHealth(ctx, provider.Name)
		if err != nil {
			logger.Errorw("Failed to set provider status", "error", err, "health check error", err)
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

func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, sourceRequest *pb.SourceVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("source", sourceRequest.SourceVariant.Name, sourceRequest.SourceVariant.Variant).WithProvider("", sourceRequest.SourceVariant.Provider)
	source := sourceRequest.SourceVariant
	logger.Infow("Creating Source Variant")
	sourceRequest.RequestId = requestID

	switch casted := source.Definition.(type) {
	case *pb.SourceVariant_Transformation:
		switch transformationType := casted.Transformation.Type.(type) {
		case *pb.Transformation_SQLTransformation:
			logger.Infow("Retreiving the sources from SQL Transformation", transformationType)
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
	return serv.meta.CreateSourceVariant(ctx, sourceRequest)
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entityRequest *pb.EntityRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("entity", entityRequest.Entity.Name, "")
	logger.Infow("Creating Entity")
	entityRequest.RequestId = requestID

	return serv.meta.CreateEntity(ctx, entityRequest)
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Requesting Schedule Change", "resource", req.ResourceId, "new schedule", req.Schedule)
	return serv.meta.RequestScheduleChange(ctx, req)
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, featureRequest *pb.FeatureVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("feature_variant", featureRequest.FeatureVariant.Name, featureRequest.FeatureVariant.Variant).WithProvider("", featureRequest.FeatureVariant.Provider)
	logger.Infow("Creating Feature Variant")
	featureRequest.RequestId = requestID

	return serv.meta.CreateFeatureVariant(ctx, featureRequest)
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, labelRequest *pb.LabelVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("label_variant", labelRequest.LabelVariant.Name, labelRequest.LabelVariant.Variant).WithProvider("", labelRequest.LabelVariant.Provider)
	label := labelRequest.LabelVariant
	logger.Infow("Creating Label Variant")
	labelRequest.RequestId = requestID

	protoSource := label.Source
	logger.Debugw("Finding label source", "name", protoSource.Name, "variant", protoSource.Variant)
	source, err := serv.client.GetSourceVariant(ctx, metadata.NameVariant{Name: protoSource.Name, Variant: protoSource.Variant})
	if err != nil {
		serv.Logger.Errorw("Could not create label source variant", "error", err)
		return nil, err
	}
	label.Provider = source.Provider()
	resp, err := serv.meta.CreateLabelVariant(ctx, labelRequest)
	serv.Logger.Debugw("Created label variant", "response", resp)
	if err != nil {
		serv.Logger.Errorw("Could not create label variant", "response", resp, "error", err)
	}
	return resp, err
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, trainRequest *pb.TrainingSetVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("training_set_variant", trainRequest.TrainingSetVariant.Name, trainRequest.TrainingSetVariant.Variant).WithProvider("", trainRequest.TrainingSetVariant.Provider)
	train := trainRequest.TrainingSetVariant
	logger.Infow("Creating Training Set Variant")
	trainRequest.RequestId = requestID

	protoLabel := train.Label
	label, err := serv.client.GetLabelVariant(ctx, metadata.NameVariant{Name: protoLabel.Name, Variant: protoLabel.Variant})
	if err != nil {
		return nil, err
	}
	for _, protoFeature := range train.Features {
		logger.Debugw("Get feature variant", "name", protoFeature.Name, "variant", protoFeature.Variant)
		_, err := serv.client.GetFeatureVariant(ctx, metadata.NameVariant{Name: protoFeature.Name, Variant: protoFeature.Variant})
		if err != nil {
			return nil, err
		}
	}
	train.Provider = label.Provider()
	return serv.meta.CreateTrainingSetVariant(ctx, trainRequest)
}

func (serv *MetadataServer) CreateModel(ctx context.Context, modelRequest *pb.ModelRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("model", modelRequest.Model.Name, "")
	logger.Infow("Creating Model")
	modelRequest.RequestId = requestID

	return serv.meta.CreateModel(ctx, modelRequest)
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

func (serv *OnlineServer) TrainTestSplit(stream srv.Feature_TrainTestSplitServer) error {
	serv.Logger.Infow("Starting Training Test Split Stream")
	clientStream, err := serv.client.TrainTestSplit(context.Background())
	if err != nil {
		return fmt.Errorf("could not serve training test split: %w", err)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
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

		if err := clientStream.Send(req); err != nil {
			serv.Logger.Errorw("Failed to send request to downstream service", "error", err)
			return err
		}

		resp, err := clientStream.Recv()
		if err == io.EOF {
			serv.Logger.Infow("Downstream service has closed the stream")
			return nil
		}
		if err != nil {
			serv.Logger.Errorw("Error receiving from downstream service", "error", err)
			return err
		}

		if err := stream.Send(resp); err != nil {
			serv.Logger.Errorw("Failed to send response to client", "error", err)
			return err
		}

	}
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
	logger := logging.NewLogger("api-gateway")
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
