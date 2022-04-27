package main

import (
	"context"
	"fmt"
	"net"

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
	return serv.meta.CreateUser(ctx, user)
}

func (serv *ApiServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
	return serv.meta.CreateProvider(ctx, provider)
}

func (serv *ApiServer) CreateSourceVariant(ctx context.Context, source *pb.SourceVariant) (*pb.Empty, error) {
	return serv.meta.CreateSourceVariant(ctx, source)
}

func (serv *ApiServer) CreateEntity(ctx context.Context, entity *pb.Entity) (*pb.Empty, error) {
	return serv.meta.CreateEntity(ctx, entity)
}

func (serv *ApiServer) CreateFeatureVariant(ctx context.Context, feature *pb.FeatureVariant) (*pb.Empty, error) {
	return serv.meta.CreateFeatureVariant(ctx, feature)
}

func (serv *ApiServer) CreateLabelVariant(ctx context.Context, label *pb.LabelVariant) (*pb.Empty, error) {
	protoSource := label.Source
	source, err := serv.metaClient.GetSourceVariant(ctx, metadata.NameVariant{protoSource.Name, protoSource.Variant})
	if err != nil {
		return nil, err
	}
	label.Provider = source.Provider()
	return serv.meta.CreateLabelVariant(ctx, label)
}

func (serv *ApiServer) CreateTrainingSetVariant(ctx context.Context, train *pb.TrainingSetVariant) (*pb.Empty, error) {
	protoLabel := train.Label
	label, err := serv.metaClient.GetLabelVariant(ctx, metadata.NameVariant{protoLabel.Name, protoLabel.Variant})
	if err != nil {
		return nil, err
	}
	train.Provider = label.Provider()
	return serv.meta.CreateTrainingSetVariant(ctx, train)
}

func (serv *ApiServer) Serve() error {
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
