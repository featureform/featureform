package main

import (
	"context"
	"fmt"
	"io"
	"net"

	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type NameVariant struct {
	Name    string
	Variant string
}

type MetadataServer struct {
	features        map[string]*pb.Feature
	featureVariants map[NameVariant]*pb.FeatureVariant
	Logger          *zap.SugaredLogger
	pb.UnimplementedMetadataServer
}

func NewMetadataServer(logger *zap.SugaredLogger) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	return &MetadataServer{
		features:        make(map[string]*pb.Feature),
		featureVariants: make(map[NameVariant]*pb.FeatureVariant),
		Logger:          logger,
	}, nil
}

func (serv *MetadataServer) ListFeatures(_ *pb.Empty, stream pb.Metadata_ListFeaturesServer) error {
	for _, feature := range serv.features {
		if err := stream.Send(feature); err != nil {
			return err
		}
	}
	return nil
}

func (serv *MetadataServer) SetFeatureVariant(ctx context.Context, variant *pb.FeatureVariant) (*pb.Empty, error) {
	name, variantName := variant.GetName(), variant.GetVariant()
	serv.featureVariants[NameVariant{name, variantName}] = variant
	feature, has := serv.features[name]
	if has {
		feature.Variants = append(feature.Variants, variant)
	} else {
		serv.features[name] = &pb.Feature{
			Name:           name,
			DefaultVariant: variantName,
			Variants:       []*pb.FeatureVariant{variant},
		}
	}
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) GetFeatures(stream pb.Metadata_GetFeaturesServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := req.GetName()
		feature, has := serv.features[name]
		if !has {
			return fmt.Errorf("Feature %s not found", name)
		}
		if err := stream.Send(feature); err != nil {
			return err
		}
	}
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Metadata_GetFeatureVariantsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name, variantName := req.GetName(), req.GetVariant()
		key := NameVariant{name, variantName}
		variant, has := serv.featureVariants[key]
		if !has {
			return fmt.Errorf("FeatureVariant %s %s not found", name, variant)
		}
		if err := stream.Send(variant); err != nil {
			return err
		}
	}
}

func main() {
	logger := zap.NewExample().Sugar()
	port := ":8080"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}
	grpcServer := grpc.NewServer()
	serv, err := NewMetadataServer(logger)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	pb.RegisterMetadataServer(grpcServer, serv)
	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}
}
