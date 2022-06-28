package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	srv "github.com/featureform/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ApiServer struct {
	Logger     *zap.SugaredLogger
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	metadata   MetadataServer
	online     OnlineServer
}

type MetadataServer struct {
	address string
	Logger  *zap.SugaredLogger
	meta    pb.MetadataClient
	client  *metadata.Client
	pb.UnimplementedApiServer
}

type OnlineServer struct {
	Logger  *zap.SugaredLogger
	address string
	client  srv.FeatureClient
	srv.UnimplementedFeatureServer
}

func NewApiServer(logger *zap.SugaredLogger, address string, metaAddr string, srvAddr string) (*ApiServer, error) {
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

func (serv *MetadataServer) CreateUser(ctx context.Context, user *pb.User) (*pb.Empty, error) {
	serv.Logger.Infow("Creating User", "user", user.Name)
	return serv.meta.CreateUser(ctx, user)
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Provider", "name", provider.Name)
	return serv.meta.CreateProvider(ctx, provider)
}

func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, source *pb.SourceVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Source Variant", "name", source.Name, "variant", source.Variant)
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
	return serv.meta.CreateSourceVariant(ctx, source)
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entity *pb.Entity) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Entity", "entity", entity.Name)
	return serv.meta.CreateEntity(ctx, entity)
}

func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	serv.Logger.Infow("Requesting Schedule Change", "resource", req.ResourceId, "new schedule", req.Schedule)
	return serv.meta.RequestScheduleChange(ctx, req)
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, feature *pb.FeatureVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Feature Variant", "name", feature.Name, "variant", feature.Variant)
	return serv.meta.CreateFeatureVariant(ctx, feature)
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, label *pb.LabelVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Label Variant", "name", label.Name, "variant", label.Variant)
	protoSource := label.Source
	serv.Logger.Debugw("Finding label source", "name", protoSource.Name, "variant", protoSource.Variant)
	source, err := serv.client.GetSourceVariant(ctx, metadata.NameVariant{protoSource.Name, protoSource.Variant})
	if err != nil {
		serv.Logger.Errorw("Could not create label source variant", "error", err)
		return nil, err
	}
	label.Provider = source.Provider()
	resp, err := serv.meta.CreateLabelVariant(ctx, label)
	serv.Logger.Debugw("Created label variant", "response", resp)
	if err != nil {
		serv.Logger.Errorw("Could not create label variant", "response", resp, "error", err)
	}
	return resp, err
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, train *pb.TrainingSetVariant) (*pb.Empty, error) {
	serv.Logger.Infow("Creating Training Set Variant", "name", train.Name, "variant", train.Variant)
	protoLabel := train.Label
	label, err := serv.client.GetLabelVariant(ctx, metadata.NameVariant{protoLabel.Name, protoLabel.Variant})
	if err != nil {
		return nil, err
	}
	train.Provider = label.Provider()
	return serv.meta.CreateTrainingSetVariant(ctx, train)
}

func (serv *OnlineServer) FeatureServe(ctx context.Context, req *srv.FeatureServeRequest) (*srv.FeatureRow, error) {
	serv.Logger.Infow("Serving Features", "request", req.String())
	return serv.client.FeatureServe(ctx, req)
}

func (serv *OnlineServer) TrainingData(req *srv.TrainingDataRequest, stream srv.Feature_TrainingDataServer) error {
	serv.Logger.Infow("Serving Training Data", "id", req.Id.String())
	client, err := serv.client.TrainingData(context.Background(), req)
	if err != nil {
		return fmt.Errorf("training data: %w", err)
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("receive error: %w", err)
		}
		if err := stream.Send(row); err != nil {
			serv.Logger.Errorw("Failed to write to stream", "Error", err)
			return fmt.Errorf("training send row: %w", err)
		}
	}
}

func (serv *ApiServer) Serve() error {
	if serv.grpcServer != nil {
		return fmt.Errorf("Server already running")
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	metaConn, err := grpc.Dial(serv.metadata.address, opts...)
	if err != nil {
		return fmt.Errorf("metdata connection: %w", err)
	}
	servConn, err := grpc.Dial(serv.online.address, opts...)
	if err != nil {
		return fmt.Errorf("serving connection: %w", err)
	}
	serv.metadata.meta = pb.NewMetadataClient(metaConn)
	client, err := metadata.NewClient(serv.metadata.address, serv.Logger)
	if err != nil {
		return fmt.Errorf("metdata new client: %w", err)
	}
	serv.metadata.client = client
	serv.online.client = srv.NewFeatureClient(servConn)
	return serv.ServeOnListener(lis)
}

func (serv *ApiServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	grpcServer := grpc.NewServer()
	pb.RegisterApiServer(grpcServer, &serv.metadata)
	srv.RegisterFeatureServer(grpcServer, &serv.online)
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
	apiPort := os.Getenv("API_PORT")
	metadataHost := os.Getenv("METADATA_HOST")
	metadataPort := os.Getenv("METADATA_PORT")
	servingHost := os.Getenv("SERVING_HOST")
	servingPort := os.Getenv("SERVING_PORT")
	apiConn := fmt.Sprintf("0.0.0.0:%s", apiPort)
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	servingConn := fmt.Sprintf("%s:%s", servingHost, servingPort)
	logger := zap.NewExample().Sugar()
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
