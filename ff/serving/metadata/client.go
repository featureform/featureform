package main

import (
	"context"
	"io"
	"time"

	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	Logger   *zap.SugaredLogger
	conn     *grpc.ClientConn
	grpcConn pb.MetadataClient
}

func (client *Client) ListFeatures(ctx context.Context) ([]Feature, error) {
	stream, err := client.grpcConn.ListFeatures(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	features := make([]Feature, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoFeature(serial))
	}
	return features, nil
}

type Feature struct {
	serialized     *pb.Feature
	variants       []FeatureVariant
	defaultVariant FeatureVariant
}

func wrapProtoFeature(serialized *pb.Feature) (feature Feature) {
	feature.serialized = serialized
	variants := make([]FeatureVariant, len(serialized.Variants))
	for i, serialVar := range serialized.Variants {
		wrappedVar := wrapProtoFeatureVariant(serialVar)
		if serialVar.GetName() == serialized.DefaultVariant {
			feature.defaultVariant = wrappedVar
		}
		variants[i] = wrappedVar
	}
	feature.variants = variants
	return
}

func (feature *Feature) Name() string {
	return feature.serialized.GetName()
}

func (feature *Feature) Variants() []FeatureVariant {
	return feature.variants
}

func (feature *Feature) DefaultVariant() FeatureVariant {
	return feature.defaultVariant
}

type FeatureVariant struct {
	serialized *pb.FeatureVariant
}

func wrapProtoFeatureVariant(serialized *pb.FeatureVariant) FeatureVariant {
	return FeatureVariant{
		serialized: serialized,
	}
}

func (variant *FeatureVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *FeatureVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *FeatureVariant) Source() string {
	return variant.serialized.GetSource()
}

func (variant *FeatureVariant) Type() string {
	return variant.serialized.GetType()
}

func (variant *FeatureVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *FeatureVariant) Created() time.Time {
	// TODO
	return time.Now()
}

func (variant *FeatureVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *FeatureVariant) TrainingSetNames() []string {
	return variant.serialized.Trainingsets
}

func (variant *FeatureVariant) TrainingSets() []TrainingSet {
	// TODO
	return nil
}

type TrainingSet struct {
	// TODO
}

func NewClient(host string, logger *zap.SugaredLogger) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewMetadataClient(conn)
	return &Client{
		Logger:   logger,
		conn:     conn,
		grpcConn: client,
	}, nil
}

func (client *Client) Close() {
	client.conn.Close()
}

func main() {
	logger := zap.NewExample().Sugar()
	client, err := NewClient("localhost:8080", logger)
	if err != nil {
		logger.Panicw("Failed to connect", "Err", err)
	}
	features, err := client.ListFeatures(context.Background())
	logger.Infow("Listed features", "Features", features, "Err", err)
}
