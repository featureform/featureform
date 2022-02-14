package main

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type NameVariant struct {
	Name    string
	Variant string
}

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
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeatures(ctx context.Context, features []string) ([]Feature, error) {
	stream, err := client.grpcConn.GetFeatures(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, feature := range features {
			stream.Send(&pb.Name{Name: feature})
		}
	}()
	return client.parseFeatureStream(stream)
}

type FeatureDef struct {
	Name        string
	Variant     string
	Source      string
	Type        string
	Entity      string
	Owner       string
	Description string
}

func (client *Client) CreateFeatureVariant(ctx context.Context, def FeatureDef) error {
	serialized := &pb.FeatureVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Source:      def.Source,
		Type:        def.Type,
		Entity:      def.Entity,
		Owner:       def.Owner,
		Description: def.Description,
	}
	_, err := client.grpcConn.CreateFeatureVariant(ctx, serialized)
	return err
}

type featureStream interface {
	Recv() (*pb.Feature, error)
}

func (client *Client) parseFeatureStream(stream featureStream) ([]Feature, error) {
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
	serialized *pb.Feature
}

func wrapProtoFeature(serialized *pb.Feature) (feature Feature) {
	feature.serialized = serialized
	return
}

func (feature *Feature) Name() string {
	return feature.serialized.GetName()
}

func (feature *Feature) Variants() []string {
	return feature.serialized.Variants
}

func (feature *Feature) Default() string {
	return feature.serialized.DefaultVariant
}

func (feature Feature) String() string {
	bytes, err := protojson.Marshal(feature.serialized)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
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
	t, err := time.Parse(variant.serialized.GetCreated(), time.RFC1123)
	if err != nil {
		// This means that the time was serialized differently.
		panic(err)
	}
	return t
}

func (variant *FeatureVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *FeatureVariant) TrainingSets() []NameVariant {
	serialized := variant.serialized
	parsed := make([]NameVariant, len(serialized.Trainingsets))
	for i, trainingSet := range serialized.Trainingsets {
		parsed[i] = NameVariant{
			Name:    trainingSet.Name,
			Variant: trainingSet.Variant,
		}
	}
	return parsed
}

func (variant *FeatureVariant) FetchTrainingSets() []TrainingSet {
	// TODO
	return nil
}

func (variant FeatureVariant) String() string {
	bytes, err := protojson.Marshal(variant.serialized)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
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
	err = client.CreateFeatureVariant(context.Background(), FeatureDef{
		Name:        "f1",
		Variant:     "v1",
		Source:      "Users",
		Type:        "int",
		Entity:      "users",
		Owner:       "simba",
		Description: "Our first feature",
	})
	if err != nil {
		logger.Panicw("Failed to create feature", "Err", err)
	}
	features, err := client.ListFeatures(context.Background())
	fmt.Printf("%+v\n", features)
	logger.Infow("Listed features", "Features", features, "Err", err)
}
