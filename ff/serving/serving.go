package main

import (
	"fmt"
	"net"

	metrics "github.com/featureform/embeddinghub/metrics"
	"github.com/featureform/serving/dataset"

	pb "github.com/featureform/serving/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var prom_metrics metrics.MetricsHandler

type TrainingDataServer struct {
	pb.UnimplementedServingServer
	DatasetProviders map[string]dataset.Provider
	Metadata         MetadataProvider
	Logger           *zap.SugaredLogger
}

func NewTrainingDataServer(logger *zap.SugaredLogger) (*TrainingDataServer, error) {
	logger.Debug("Creating new training data server")
	// Manually setup metadata and providers, this will be done by user-provided config files later.
	csvStorageId := "localCSV"
	csvProvider := &LocalCSVProvider{logger}
	metadata, err := NewLocalMemoryMetadata(logger)
	if err != nil {
		logger.Errorw("Failed to create metadata client", "Error", err)
		return nil, err
	}
	metadataErr := metadata.SetTrainingSetMetadata("f1", "v1", MetadataEntry{
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
	if metadataErr != nil {
		logger.Errorw("Failed to set metadata", "Error", metadataErr)
		return nil, metadataErr
	}
	return &TrainingDataServer{
		DatasetProviders: map[string]dataset.Provider{
			csvStorageId: csvProvider,
		},
		Metadata: metadata,
		Logger:   logger,
	}, nil
}

func (serv *TrainingDataServer) TrainingData(req *pb.TrainingDataRequest, stream pb.Serving_TrainingDataServer) error {
	id := req.GetId()
	name, version := id.GetName(), id.GetVersion()
	featureObserver := prom_metrics.BeginObservingTrainingServe(name, version)
	defer featureObserver.Finish()
	logger := serv.Logger.With("Name", name, "Version", version)
	logger.Infow("Serving training data")
	entry, err := serv.Metadata.TrainingSetMetadata(name, version)
	if err != nil {
		logger.Error("Metadata lookup failed")
		featureObserver.SetError()
		return err
	}
	logger = logger.With("Entry", entry)
	provider, has := serv.DatasetProviders[entry.StorageId]
	if !has {
		serv.Logger.Error("Provider not loaded on server")
		featureObserver.SetError()
		return fmt.Errorf("Unknown provider: %s", entry.StorageId)
	}
	dataset, err := provider.GetDatasetReader(entry.Key)
	if err != nil {
		serv.Logger.Errorw("Failed to get dataset reader", "Error", err)
		featureObserver.SetError()
		return err
	}
	for dataset.Scan() {
		if err := stream.Send(dataset.Row().Serialized()); err != nil {
			serv.Logger.Errorw("Failed to write to stream", "Error", err)
			featureObserver.SetError()
			return err
		}
		featureObserver.ServeRow()
	}
	if err := dataset.Err(); err != nil {
		serv.Logger.Errorw("Dataset error", "Error", err)
		featureObserver.SetError()
		return err
	}
	return nil
}

func main() {
	logger := zap.NewExample().Sugar()
	prom_metrics = metrics.NewMetrics("test")
	port := ":8080"
	metrics_port := ":2112"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}
	grpcServer := grpc.NewServer()
	serv, err := NewTrainingDataServer(logger)
	if err != nil {
		logger.Panicw("Failed to create training server", "Err", err)
	}
	pb.RegisterServingServer(grpcServer, serv)
	logger.Infow("Serving metrics", "Port", metrics_port)
	go prom_metrics.ExposePort(metrics_port)
	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}

}
