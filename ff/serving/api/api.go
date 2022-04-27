package main

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/featureform/serving/metadata"
	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ApiServer struct {
	Logger     *zap.SugaredLogger
	address    string
	metaAddr   string
	meta       pb.MetadataClient
	metaClient *metadata.Client
	grpcServer *grpc.Server
	listener   net.Listener
	pb.UnimplementedApiServer
}

func NewApiServer(logger *zap.SugaredLogger, address string, metaAddr string) (*ApiServer, error) {
	return &ApiServer{
		Logger:   logger,
		address:  address,
		metaAddr: metaAddr,
	}, nil
}

func (serv *ApiServer) CreateUser(ctx context.Context, user *pb.User) (*pb.Empty, error) {
	serv.Logger.Infow("Creating user", "Proto", user)
	return serv.meta.CreateUser(ctx, user)
}

func (serv *ApiServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
	serv.Logger.Infow("Creating provider", "Proto", provider)
	return serv.meta.CreateProvider(ctx, provider)
}

func (serv *ApiServer) CreateSourceVariant(ctx context.Context, source *pb.SourceVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Create SourceVariant Request")
	switch casted := source.Definition.(type) {
	case *pb.SourceVariant_Transformation:
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
	serv.Logger.Infow("Writing Sources", "Proto", source)
	return serv.meta.CreateSourceVariant(ctx, source)
}

func (serv *ApiServer) CreateEntity(ctx context.Context, entity *pb.Entity) (*pb.Empty, error) {
	serv.Logger.Infow("Creating entity", "Proto", entity)
	return serv.meta.CreateEntity(ctx, entity)
}

func (serv *ApiServer) CreateFeatureVariant(ctx context.Context, feature *pb.FeatureVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating feature", "Proto", feature)
	return serv.meta.CreateFeatureVariant(ctx, feature)
}

func (serv *ApiServer) CreateLabelVariant(ctx context.Context, label *pb.LabelVariant) (*pb.Empty, error) {
	protoSource := label.Source
	source, err := serv.metaClient.GetSourceVariant(ctx, metadata.NameVariant{protoSource.Name, protoSource.Variant})
	if err != nil {
		serv.Logger.Infow("Failed to get source", "error", err)
		return nil, err
	}
	label.Provider = source.Provider()
	serv.Logger.Infow("Creating label", "Proto", label)
	return serv.meta.CreateLabelVariant(ctx, label)
}

func (serv *ApiServer) CreateTrainingSetVariant(ctx context.Context, train *pb.TrainingSetVariant) (*pb.Empty, error) {
	protoLabel := train.Label
	label, err := serv.metaClient.GetLabelVariant(ctx, metadata.NameVariant{protoLabel.Name, protoLabel.Variant})
	if err != nil {
		serv.Logger.Infow("Failed to get label", "error", err)
		return nil, err
	}
	train.Provider = label.Provider()
	serv.Logger.Infow("Creating training set", "Proto", label)
	return serv.meta.CreateTrainingSetVariant(ctx, train)
}

func (serv *ApiServer) Serve() error {
	serv.Logger.Infow("Starting API Server", "Addr", serv.address, "Metadata", serv.metaAddr)
	if serv.grpcServer != nil {
		return fmt.Errorf("Server already running")
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		return err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(serv.metaAddr, opts...)
	if err != nil {
		return err
	}
	serv.meta = pb.NewMetadataClient(conn)
	client, err := metadata.NewClient(serv.metaAddr, serv.Logger)
	if err != nil {
		return err
	}
	serv.metaClient = client
	return serv.ServeOnListener(lis)
}

func (serv *ApiServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	grpcServer := grpc.NewServer()
	pb.RegisterApiServer(grpcServer, serv)
	serv.grpcServer = grpcServer
	serv.Logger.Infow("Server starting", "Address", serv.listener.Addr().String())
	return grpcServer.Serve(lis)
}

func (serv *ApiServer) GracefulStop() error {
	if serv.grpcServer == nil {
		return fmt.Errorf("Server not running")
	}
	serv.grpcServer.GracefulStop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

func main() {
	logger := zap.NewExample().Sugar()
	serv, err := NewApiServer(logger, "0.0.0.0:7878", "0.0.0.0:8888")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(serv.Serve())
}
