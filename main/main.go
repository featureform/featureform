package main

import (
	"fmt"
	"github.com/featureform/api"
	"github.com/featureform/coordinator"
	"github.com/featureform/ffsync"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
	"github.com/featureform/serving"
	ss "github.com/featureform/storage"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"time"
)

func main() {
	/******************************************    Vars    ************************************************************/
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("could not fetch .env file: %s", err.Error())
	}
	apiPort := help.GetEnv("API_PORT", "7878")
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	servingHost := help.GetEnv("SERVING_HOST", "0.0.0.0")
	servingPort := help.GetEnv("SERVING_PORT", "8081")
	apiConn := fmt.Sprintf("0.0.0.0:%s", apiPort)
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	servingConn := fmt.Sprintf("%s:%s", servingHost, servingPort)
	local := help.GetEnvBool("FEATUREFORM_LOCAL", true)

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		panic(err.Error())
	}
	mstorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		panic(err.Error())
	}
	storage := ss.MetadataStorage{
		Locker:  &locker,
		Storage: &mstorage,
	}

	meta, err := scheduling.NewMemoryTaskMetadataManager()
	if err != nil {
		panic(err.Error())
	}

	/****************************************** API Server ************************************************************/

	logger := logging.NewLogger("api")
	go func() {
		err := api.StartHttpsServer(":8443")
		if err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("health check HTTP server failed: %+v", err))
		}
	}()

	/******************************************** Metadata ************************************************************/

	mLogger := logging.NewLogger("metadata")

	config := &metadata.Config{
		Logger:          mLogger,
		Address:         fmt.Sprintf(":%s", metadataPort),
		StorageProvider: storage,
		TaskManager:     meta,
	}

	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}

	/******************************************** Coordinator ************************************************************/

	fmt.Printf("connecting to metadata: %s\n", metadataConn)

	if err := runner.RegisterFactory(runner.COPY_TO_ONLINE, runner.MaterializedChunkRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Copy to Online' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.MATERIALIZE, runner.MaterializeRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Materialize' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.CREATE_TRANSFORMATION, runner.CreateTransformationRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Create Transformation' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.CREATE_TRAINING_SET, runner.TrainingSetRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Create Training Set' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.S3_IMPORT_DYNAMODB, runner.S3ImportDynamoDBRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register S3 import to DynamoDB runner factory: %v", err))
	}
	cLogger := logging.NewLogger("coordinator")
	defer cLogger.Sync()
	cLogger.Debug("Connected to ETCD")

	client, err := metadata.NewClient(metadataConn, cLogger)
	if err != nil {
		cLogger.Errorw("Failed to connect: %v", err)
		panic(err)
	}
	cLogger.Debug("Connected to Metadata")
	var spawner coordinator.JobSpawner
	spawner = &coordinator.MemoryJobSpawner{}

	coord, err := coordinator.NewCoordinator(client, cLogger, spawner, &locker)
	if err != nil {
		logger.Errorw("Failed to set up coordinator: %v", err)
		panic(err)
	}
	cLogger.Debug("Begin Job Watch")

	/**************************************** Dashboard Backend *******************************************************/
	dbLogger := zap.NewExample().Sugar()

	dbLogger.Infof("Looking for metadata at: %s\n", metadataConn)

	metadataServer, err := dm.NewMetadataServer(dbLogger, client, storage)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}

	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	dbLogger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)

	/**************************************** Serving *******************************************************/

	sLogger := logging.NewLogger("serving")

	lis, err := net.Listen("tcp", servingConn)
	if err != nil {
		sLogger.Panicw("Failed to listen on port", "Err", err)
	}
	metricsHandler := &metrics.NoOpMetricsHandler{}
	serv, err := serving.NewFeatureServer(client, metricsHandler, sLogger)
	if err != nil {
		sLogger.Panicw("Failed to create training server", "Err", err)
	}
	grpcServer := grpc.NewServer()

	pb.RegisterFeatureServer(grpcServer, serv)
	sLogger.Infow("Server starting", "Port", servingConn)

	/******************************************** Start Servers *******************************************************/

	go func() {
		serv, err := api.NewApiServer(logger, apiConn, metadataConn, servingConn)
		if err != nil {
			panic(err)
		}
		fmt.Println(serv.Serve())
	}()

	go func() {
		if err := server.Serve(); err != nil {
			logger.Errorw("Serve failed with error", "Err", err)
			if err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		if err := coord.WatchForNewJobs(); err != nil {
			cLogger.Errorw(err.Error())
			panic(err)
		}
	}()

	go func() {
		err := metadataServer.Start(metadataServingPort, local)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		serveErr := grpcServer.Serve(lis)
		if serveErr != nil {
			logger.Errorw("Serve failed with error", "Err", serveErr)
			panic(err)
		}
	}()
	for {
		time.Sleep(1 * time.Second)
	}
}
