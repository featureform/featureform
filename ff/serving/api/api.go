package main

import (
    "context"
	"fmt"
	"net"

	pb "github.com/featureform/serving/metadata/proto"
	"google.golang.org/grpc/credentials/insecure"
	"go.uber.org/zap"
	"google.golang.org/grpc"

)

type ApiServer struct {
	Logger     *zap.SugaredLogger
	address    string
    metaAddr string
    meta pb.MetadataClient
	grpcServer *grpc.Server
	listener   net.Listener
	pb.UnimplementedApiServer
}

func NewApiServer(logger *zap.SugaredLogger, address string, metaAddr string) (*ApiServer, error) {
	return &ApiServer{
		Logger:  logger,
		address: address,
        metaAddr: metaAddr,
	}, nil
}

func (serv *ApiServer) CreateUser(ctx context.Context, user *pb.User) (*pb.Empty, error) {
   return serv.meta.CreateUser(ctx, user)
}

func (serv *ApiServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
   return serv.meta.CreateProvider(ctx, provider)
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
