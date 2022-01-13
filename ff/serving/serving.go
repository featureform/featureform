package main

import (
	"context"
	"fmt"
	"net"

	metrics "github.com/featureform/embeddinghub/metrics"
	"github.com/featureform/serving/dataset"

	pb "github.com/featureform/serving/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type FeatureServer struct {
	pb.UnimplementedFeatureServer
	Metrics          metrics.MetricsHandler
	DatasetProviders map[string]dataset.OfflineProvider
	FeatureProviders map[string]dataset.OnlineProvider
	Metadata         MetadataProvider
	Logger           *zap.SugaredLogger
}

func NewFeatureServer(promMetrics metrics.MetricsHandler, logger *zap.SugaredLogger) (*FeatureServer, error) {
	logger.Debug("Creating new training data server")
	// Manually setup metadata and providers, this will be done by user-provided config files later.
	csvStorageId := "localCSV"
	csvProvider := &LocalCSVProvider{logger}
	metadata, err := NewLocalMemoryMetadata(logger)
	if err != nil {
		logger.Errorw("Failed to create metadata client", "Error", err)
		return nil, err
	}
	trainMetaErr := metadata.SetTrainingSetMetadata("f1", "v1", TrainingSetEntry{
		StorageId: csvStorageId,
		Key: csvProvider.ToKey("testdata/house_price.csv", CSVSchema{
			HasHeader: true,
			Features:  []string{"zip"},
			Label:     "price",
			Types: map[string]dataset.Type{
				"zip":   dataset.String,
				"price": dataset.Int,
			},
		}),
	})
	if trainMetaErr != nil {
		logger.Errorw("Failed to set training set metadata", "Error", trainMetaErr)
		return nil, trainMetaErr
	}

	onlineStorageId := "online-memory"
	onlineProvider := NewMemoryOnlineProvider()
	key := onlineProvider.ToKey("f1", "v1")
	onlineProvider.SetFeature("f1", "v1", "a", 12.34)
	featureMetaErr := metadata.SetFeatureMetadata("f1", "v1", FeatureEntry{
		StorageId: onlineStorageId,
		Entity:    "user",
		Key:       key,
	})
	if featureMetaErr != nil {
		logger.Errorw("Failed to set feature metadata", "Error", trainMetaErr)
		return nil, trainMetaErr
	}
	return &FeatureServer{
		Metrics: promMetrics,
		DatasetProviders: map[string]dataset.OfflineProvider{
			csvStorageId: csvProvider,
		},
		FeatureProviders: map[string]dataset.OnlineProvider{
			onlineStorageId: onlineProvider,
		},
		Metadata: metadata,
		Logger:   logger,
	}, nil
}

func (serv *FeatureServer) TrainingData(req *pb.TrainingDataRequest, stream pb.Feature_TrainingDataServer) error {
	id := req.GetId()
	name, version := id.GetName(), id.GetVersion()
	featureObserver := serv.Metrics.BeginObservingTrainingServe(name, version)
	defer featureObserver.Finish()
	logger := serv.Logger.With("Name", name, "Version", version)
	logger.Info("Serving training data")
	entry, err := serv.Metadata.TrainingSetMetadata(name, version)
	if err != nil {
		logger.Errorw("Metadata lookup failed", "Err", err)
		featureObserver.SetError()
		return err
	}
	logger = logger.With("Entry", entry)
	provider, has := serv.DatasetProviders[entry.StorageId]
	if !has {
		logger.Error("Provider not loaded on server")
		featureObserver.SetError()
		return fmt.Errorf("Unknown provider: %s", entry.StorageId)
	}
	dataset, err := provider.GetDatasetReader(entry.Key)
	if err != nil {
		logger.Errorw("Failed to get dataset reader", "Error", err)
		featureObserver.SetError()
		return err
	}
	for dataset.Scan() {
		if err := stream.Send(dataset.Row().Serialized()); err != nil {
			logger.Errorw("Failed to write to stream", "Error", err)
			featureObserver.SetError()
			return err
		}
		featureObserver.ServeRow()
	}
	if err := dataset.Err(); err != nil {
		logger.Errorw("Dataset error", "Error", err)
		featureObserver.SetError()
		return err
	}
	return nil
}

func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	vals := make([]*pb.Value, len(features))
	entities := make(map[string]string)
	for _, entity := range req.GetEntities() {
		entities[entity.GetName()] = entity.GetValue()
	}
	for i, feature := range req.GetFeatures() {
		name, version := feature.GetName(), feature.GetVersion()
		serv.Logger.Infow("Serving feature", "Name", name, "Version", version, "Entities", entities)
		val, err := serv.getFeatureValue(name, version, entities)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return &pb.FeatureRow{
		Values: vals,
	}, nil
}

func (serv *FeatureServer) getFeatureValue(name, version string, entities map[string]string) (*pb.Value, error) {
	logger := serv.Logger.With("Name", name, "Version", version, "Entities", entities)
	logger.Debug("Getting metadata")
	entry, err := serv.Metadata.FeatureMetadata(name, version)
	if err != nil {
		logger.Errorw("Metadata lookup failed", "Err", err)
		return nil, err
	}
	logger = logger.With("Entry", entry)
	entity, has := entities[entry.Entity]
	if !has {
		logger.Errorw("Entity not found", "Entity Name", entry.Entity)
		return nil, fmt.Errorf("Entity not found: %s", entry.Entity)
	}

	provider, has := serv.FeatureProviders[entry.StorageId]
	if !has {
		logger.Error("Provider not loaded on server")
		return nil, fmt.Errorf("Unknown provider: %s", entry.StorageId)
	}

	lookup, err := provider.GetFeatureLookup(entry.Key)
	if err != nil {
		logger.Errorw("Failed to get feature lookup", "Error", err)
		return nil, err
	}

	val, err := lookup.Get(entity)
	if err != nil {
		logger.Errorw("Entity not found", "Error", err)
		return nil, err
	}
	return val.Serialized(), nil
}

func main() {
	logger := zap.NewExample().Sugar()
	promMetrics := metrics.NewMetrics("test")
	port := ":8080"
	metrics_port := ":2112"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}
	grpcServer := grpc.NewServer()
	serv, err := NewFeatureServer(promMetrics, logger)
	if err != nil {
		logger.Panicw("Failed to create training server", "Err", err)
	}
	pb.RegisterFeatureServer(grpcServer, serv)
	logger.Infow("Serving metrics", "Port", metrics_port)
	go promMetrics.ExposePort(metrics_port)
	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}

}
