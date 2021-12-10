package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/featureform/serving/proto"
	"google.golang.org/grpc"
)

type TrainingDataServer struct {
	pb.UnimplementedOfflineServingServer
	DatasetProviders map[string]DatasetProvider
	Metadata         MetadataProvider
}

func NewTrainingDataServer() (*TrainingDataServer, error) {
	// Manually setup metadata and providers, this will be done by user-provided config files later.
	csvStorageId := "localCSV"
	csvProvider := &LocalCSVProvider{}
	metadata, err := NewLocalMemoryMetadata()
	if err != nil {
		return nil, err
	}
	metadataErr := metadata.SetTrainingSetMetadata("f1", "v1", MetadataEntry{
		StorageId: csvStorageId,
		Key: csvProvider.ToKey("testdata/house_price.csv", CSVSchema{
			HasHeader: true,
			Features:  []string{"zip"},
			Label:     "price",
			Types: map[string]Type{
				"zip":   String,
				"price": Int,
			},
		}),
	})
	if metadataErr != nil {
		return nil, metadataErr
	}
	return &TrainingDataServer{
		DatasetProviders: map[string]DatasetProvider{
			csvStorageId: csvProvider,
		},
		Metadata: metadata,
	}, nil
}

func (serv *TrainingDataServer) TrainingData(req *pb.TrainingDataRequest, stream pb.OfflineServing_TrainingDataServer) error {
	id := req.GetId()
	entry, err := serv.Metadata.TrainingSetMetadata(id.GetName(), id.GetVersion())
	if err != nil {
		return err
	}
	provider, has := serv.DatasetProviders[entry.StorageId]
	if !has {
		return fmt.Errorf("Unknown provider: %s", entry.StorageId)
	}
	dataset, err := provider.GetDatasetReader(entry.Key)
	if err != nil {
		return err
	}
	for dataset.Scan() {
		if err := stream.Send(dataset.Row().Serialized()); err != nil {
			return err
		}
	}
	if err := dataset.Err(); err != nil {
		return err
	}
	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("%s", err)
	}
	grpcServer := grpc.NewServer()
	serv, err := NewTrainingDataServer()
	if err != nil {
		log.Fatalf("%s", err)
	}
	pb.RegisterOfflineServingServer(grpcServer, serv)
	grpcServer.Serve(lis)
}
