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
	logger = logger.WithResource(logging.User, userRequest.User.Name, logging.NoVariant)
	logger.Infow("Creating User")
	userRequest.RequestId = requestID
	return serv.meta.CreateUser(ctx, userRequest)
}

func (serv *MetadataServer) GetUsers(stream pb.Api_GetUsersServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Users")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.User, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting user from stream")
		nameRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetUsers(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get users from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send user to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive users from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send users to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetFeatures(stream pb.Api_GetFeaturesServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Features")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Feature, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting feature from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetFeatures(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get features from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send feature to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive features from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send features to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Api_GetFeatureVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Feature Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Feature, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting feature variant from stream")
		nameVariantRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetFeatureVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get feature variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send feature variant to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive feature variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send feature variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabels(stream pb.Api_GetLabelsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Labels")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Label, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting label from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetLabels(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get labels from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send labels to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive labels from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send labels to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Api_GetLabelVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Label Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Label, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting label variant from stream")
		nameVariantRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetLabelVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get label variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send label variants to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive label variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send label variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSources(stream pb.Api_GetSourcesServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Sources")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Source, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting source from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetSources(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get sources from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send source to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive sources from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send sources to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Api_GetSourceVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Source Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Source, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting source variant from stream")
		nameVariantRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetSourceVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get source variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send source to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive source variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send source variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Api_GetTrainingSetsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Training Sets")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.TrainingSet, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting training set from stream")
		nameRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetTrainingSets(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get training sets from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send training set to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive training sets from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send training sets to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Api_GetTrainingSetVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Training Set Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.TrainingSet, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting training set variant from stream")
		nameVariantRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetTrainingSetVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get training set variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send training set to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive training set variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send training set variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetProviders(stream pb.Api_GetProvidersServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Providers")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Provider, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting provider from stream")
		nameRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetProviders(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get providers from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send provider to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive providers from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send providers to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEntities(stream pb.Api_GetEntitiesServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Entities")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Entity, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting entity from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetEntities(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get entities from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send entity to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive entities from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send entities to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetModels(stream pb.Api_GetModelsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Models")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Model, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting model from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetModels(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get models from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send model to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive models from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send models to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEquivalent(ctx context.Context, req *pb.ResourceVariantRequest) (*pb.ResourceVariant, error) {
	ctx = logging.AttachRequestID(req.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Getting equivalent resource")
	return serv.meta.GetEquivalent(ctx, req)
}

func (serv *MetadataServer) ListUsers(listRequest *pb.ListRequest, stream pb.Api_ListUsersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Users")
	proxyStream, err := serv.meta.ListUsers(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list users", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive user from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.User, res.Name, logging.NoVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send user to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListFeatures(listRequest *pb.ListRequest, stream pb.Api_ListFeaturesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Features")
	proxyStream, err := serv.meta.ListFeatures(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list features", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive feature from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Feature, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send feature to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListLabels(listRequest *pb.ListRequest, stream pb.Api_ListLabelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Labels")
	proxyStream, err := serv.meta.ListLabels(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list labels", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive label from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Label, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send label to client", "error", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListSources(listRequest *pb.ListRequest, stream pb.Api_ListSourcesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Sources")
	proxyStream, err := serv.meta.ListSources(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list sources", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive source from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Source, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send source to client", "error", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListTrainingSets(listRequest *pb.ListRequest, stream pb.Api_ListTrainingSetsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Training Sets")
	proxyStream, err := serv.meta.ListTrainingSets(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list training sets", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive training set from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.TrainingSet, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send training set to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListModels(listRequest *pb.ListRequest, stream pb.Api_ListModelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Models")
	proxyStream, err := serv.meta.ListModels(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list models", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive model from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Model, res.Name, logging.NoVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send model to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListEntities(listRequest *pb.ListRequest, stream pb.Api_ListEntitiesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Entities")
	proxyStream, err := serv.meta.ListEntities(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list entities", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive entity from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Entity, res.Name, logging.NoVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send entity to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListProviders(listRequest *pb.ListRequest, stream pb.Api_ListProvidersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Providers")
	proxyStream, err := serv.meta.ListProviders(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list providers", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive provider from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Provider, res.Name, logging.NoVariant).WithProvider(res.Type, res.Name)
		loggerWithResource.Infow("Sending resource on stream")
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send provider to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, providerRequest *pb.ProviderRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("provider", providerRequest.Provider.Name, logging.NoVariant).WithProvider(providerRequest.Provider.Type, providerRequest.Provider.Name)
	provider := providerRequest.Provider
	logger.Infow("Creating Provider")
	providerRequest.RequestId = requestID
	// The existence of a provider is part of the determination for checking provider health, hence why it
	// needs to happen prior to the call to CreateProvider, which is an upsert operation.
	shouldCheckProviderHealth, err := serv.shouldCheckProviderHealth(ctx, provider)
	if err != nil {
		logger.Errorw("Failed to check provider health", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	for {
		stream, err := serv.meta.GetProviders(ctx)
		if err != nil {
			logger.Errorw("Failed to get providers from metadata server", "error", err)
			return false, err
		}
		if err := stream.Send(&pb.NameRequest{Name: &pb.Name{Name: provider.Name}, RequestId: logger.GetRequestID().String()}); err != nil {
			logger.Errorw("Failed to send provider to server", "error", err)
			return false, err
		}
		res, err := stream.Recv()
		if grpc_status.Code(err) == codes.NotFound {
			break
		}
		if err != nil {
			logger.Errorw("Failed to receive provider from server", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Checking provider health")
	isHealthy, err := serv.health.CheckProvider(providerName)
	if err != nil || !isHealthy {
		logger.Errorw("Provider health check failed", "error", err)

		errorStatus, ok := grpc_status.FromError(err)
		if !ok {
			logger.Infow("Unknown codes", "error status", errorStatus, "error", err)
			return err
		}
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
		logger.Infow("Provider health check passed", "name", providerName)
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
	logger = logger.WithResource(logging.Source, sourceRequest.SourceVariant.Name, sourceRequest.SourceVariant.Variant).WithProvider(logging.SkipProviderType, sourceRequest.SourceVariant.Provider)
	source := sourceRequest.SourceVariant
	logger.Infow("Creating Source Variant")
	sourceRequest.RequestId = requestID

	switch casted := source.Definition.(type) {
	case *pb.SourceVariant_Transformation:
		switch transformationType := casted.Transformation.Type.(type) {
		case *pb.Transformation_SQLTransformation:
			logger.Debugw("Retreiving the sources from SQL Transformation", "transformation type", transformationType)
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
			logger.Debugw("Setting the source in the SQL Transformation", "sources", sources)
			source.Definition.(*pb.SourceVariant_Transformation).Transformation.Type.(*pb.Transformation_SQLTransformation).SQLTransformation.Source = sources
		}
	}
	return serv.meta.CreateSourceVariant(ctx, sourceRequest)
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entityRequest *pb.EntityRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.Entity, entityRequest.Entity.Name, logging.NoVariant)
	logger.Infow("Creating Entity")
	entityRequest.RequestId = requestID

	return serv.meta.CreateEntity(ctx, entityRequest)
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Requesting Schedule Change", "resource", req.ResourceId, "new schedule", req.Schedule)
	return serv.meta.RequestScheduleChange(ctx, req)
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, featureRequest *pb.FeatureVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("feature_variant", featureRequest.FeatureVariant.Name, featureRequest.FeatureVariant.Variant).WithProvider(logging.SkipProviderType, featureRequest.FeatureVariant.Provider)
	logger.Infow("Creating Feature Variant")
	featureRequest.RequestId = requestID

	return serv.meta.CreateFeatureVariant(ctx, featureRequest)
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, labelRequest *pb.LabelVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.LabelVariant, labelRequest.LabelVariant.Name, labelRequest.LabelVariant.Variant).WithProvider(logging.SkipProviderType, labelRequest.LabelVariant.Provider)
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
	logger.Debugw("Created label variant", "response", resp)
	if err != nil {
		serv.Logger.Errorw("Could not create label variant", "response", resp, "error", err)
	}
	return resp, err
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, trainRequest *pb.TrainingSetVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.TrainingSetVariant, trainRequest.TrainingSetVariant.Name, trainRequest.TrainingSetVariant.Variant).WithProvider(logging.SkipProviderType, trainRequest.TrainingSetVariant.Provider)
	train := trainRequest.TrainingSetVariant
	logger.Infow("Creating Training Set Variant")
	trainRequest.RequestId = requestID

	protoLabel := train.Label
	label, err := serv.client.GetLabelVariant(ctx, metadata.NameVariant{Name: protoLabel.Name, Variant: protoLabel.Variant})
	if err != nil {
		return nil, err
	}
	for _, protoFeature := range train.Features {
		logger.Infow("Get feature variant", "name", protoFeature.Name, "variant", protoFeature.Variant)
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
	logger = logger.WithResource(logging.Model, modelRequest.Model.Name, logging.NoVariant)
	logger.Infow("Creating Model")
	modelRequest.RequestId = requestID

	return serv.meta.CreateModel(ctx, modelRequest)
}

func (serv *MetadataServer) CreateTrigger(ctx context.Context, tr *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Trigger", "trigger", tr.String())
	return serv.meta.CreateTrigger(ctx, tr)
}

func (serv *MetadataServer) AddTrigger(ctx context.Context, tr *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Adding Trigger", "trigger", tr.String())
	return serv.meta.AddTrigger(ctx, tr)
}

func (serv *MetadataServer) RemoveTrigger(ctx context.Context, tr *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Removing Trigger", "trigger", tr.String())
	return serv.meta.RemoveTrigger(ctx, tr)
}

func (serv *MetadataServer) UpdateTrigger(ctx context.Context, tr *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Updating Trigger", "trigger", tr.String())
	return serv.meta.UpdateTrigger(ctx, tr)
}

func (serv *MetadataServer) DeleteTrigger(ctx context.Context, tr *pb.TriggerRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Deleting Trigger", "trigger", tr.String())
	return serv.meta.DeleteTrigger(ctx, tr)
}

func (serv *OnlineServer) FeatureServe(ctx context.Context, req *srv.FeatureServeRequest) (*srv.FeatureRow, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Features", "request", req.String())
	return serv.client.FeatureServe(ctx, req)
}

func (serv *OnlineServer) BatchFeatureServe(req *srv.BatchFeatureServeRequest, stream srv.Feature_BatchFeatureServeServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Serving Batch Features", "request", req.String())
	client, err := serv.client.BatchFeatureServe(ctx, req)
	if err != nil {
		logger.Errorw("Failed to serve batch features", "request", req.String(), "error", err)
		return fmt.Errorf("could not serve batch features: %w", err)
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			logger.Errorw("Failed to receive row from client", "row", row, "error", err)
			return err
		}
		if err := stream.Send(row); err != nil {
			logger.Errorw("Failed to write to stream", "error", err)
			return err
		}
	}

}

func (serv *OnlineServer) TrainingData(req *srv.TrainingDataRequest, stream srv.Feature_TrainingDataServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Serving Training Data", "id", req.Id.String())
	client, err := serv.client.TrainingData(ctx, req)
	if err != nil {
		logger.Errorw("Failed to get Training Data client", "error", err)
		return err
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			logger.Errorw("Failed to receive row from client", "row", row, "error", err)
			return err
		}
		if err := stream.Send(row); err != nil {
			logger.Errorw("Failed to write to stream", "error", err)
			return err
		}
	}
}

func (serv *OnlineServer) TrainTestSplit(stream srv.Feature_TrainTestSplitServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Starting Training Test Split Stream")
	clientStream, err := serv.client.TrainTestSplit(ctx)
	if err != nil {
		logger.Errorw("Failed to serve training test split", "error", err)
		return fmt.Errorf("could not serve training test split: %w", err)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			logger.Infow("Client has closed the stream")
			if err := clientStream.CloseSend(); err != nil {
				logger.Errorw("Failed to close send direction to downstream service", "error", err)
				return fmt.Errorf("failed to close send direction to downstream service: %w", err)
			}
			return nil
		}
		if err != nil {
			logger.Errorw("Error receiving from client stream", "error", err)
			return err
		}
		logger.Infow("Getting request from stream, Training data id", req.Id)
		if err := clientStream.Send(req); err != nil {
			logger.Errorw("Failed to send request to downstream service", "error", err)
			return err
		}

		resp, err := clientStream.Recv()
		if err == io.EOF {
			logger.Infow("Downstream service has closed the stream")
			return nil
		}
		if err != nil {
			logger.Errorw("Error receiving from downstream service", "error", err)
			return err
		}

		if err := stream.Send(resp); err != nil {
			logger.Errorw("Failed to send response to client", "error", err)
			return err
		}

	}
}

func (serv *OnlineServer) TrainingDataColumns(ctx context.Context, req *srv.TrainingDataColumnsRequest) (*srv.TrainingColumns, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Training Set Columns", "id", req.Id.String())
	return serv.client.TrainingDataColumns(ctx, req)
}

func (serv *OnlineServer) SourceData(req *srv.SourceDataRequest, stream srv.Feature_SourceDataServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Serving Source Data", "id", req.Id.String())
	if req.Limit == 0 {
		err := fferr.NewInvalidArgumentError(fmt.Errorf("limit must be greater than 0"))
		logger.Errorw("Limit must be greater than 0", "invalid-argument", req.Limit, "error", err)
		return err
	}
	client, err := serv.client.SourceData(ctx, req)
	if err != nil {
		logger.Errorw("Failed to get Source Data client", "error", err)
		return err
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			logger.Errorw("Failed to receive row from client", "row", row, "error", err)
			return err
		}
		if err := stream.Send(row); err != nil {
			logger.Errorw("failed to write to source data stream", "row", row, "error", err)
			return err
		}
	}
}

func (serv *OnlineServer) SourceColumns(ctx context.Context, req *srv.SourceColumnRequest) (*srv.SourceDataColumns, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Source Columns", "id", req.Id.String())
	return serv.client.SourceColumns(ctx, req)
}

func (serv *OnlineServer) Nearest(ctx context.Context, req *srv.NearestRequest) (*srv.NearestResponse, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Nearest", "id", req.Id.String())
	return serv.client.Nearest(ctx, req)
}

func (serv *OnlineServer) GetResourceLocation(ctx context.Context, req *srv.ResourceIdRequest) (*srv.ResourceLocation, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Resource Location", "resource", req.String())
	return serv.client.GetResourceLocation(ctx, req)
}

func (serv *ApiServer) Serve() error {
	logger := logging.NewLogger("serve")
	logger.Infow("Starting server", "address", serv.address)
	if serv.grpcServer != nil {
		logger.Errorw("Server already running")
		return fferr.NewInternalError(fmt.Errorf("server already running"))
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		logger.Errorw("Failed to listen", "error", err)
		return fferr.NewInternalError(err)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// grpc.WithUnaryInterceptor(fferr.UnaryClientInterceptor()),
		// grpc.WithStreamInterceptor(fferr.StreamClientInterceptor()),
	}
	metaConn, err := grpc.Dial(serv.metadata.address, opts...)
	if err != nil {
		logger.Errorw("Failed to dial metadata server", "error", err)
		return fferr.NewInternalError(err)
	}
	servConn, err := grpc.Dial(serv.online.address, opts...)
	if err != nil {
		logger.Errorw("Failed to dial serving server", "error", err)
		return fferr.NewInternalError(err)
	}
	serv.metadata.meta = pb.NewMetadataClient(metaConn)
	client, err := metadata.NewClient(serv.metadata.address, serv.Logger)
	if err != nil {
		logger.Errorw("Failed to create metadata client", "error", err)
		return err
	}
	serv.metadata.client = client
	serv.online.client = srv.NewFeatureClient(servConn)
	serv.metadata.health = health.NewHealth(client)
	logger.Infof("Created metadata client successfully.")
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
