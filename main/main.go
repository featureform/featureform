// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/featureform/api"
	"github.com/featureform/config"
	"github.com/featureform/config/bootstrap"
	"github.com/featureform/coordinator"
	"github.com/featureform/coordinator/spawner"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/serving"

	"google.golang.org/grpc"
)

func main() {
	/******************************************    Vars    ************************************************************/
	err := os.Setenv("SPARK_LOCAL_SCRIPT_PATH", "provider/scripts/spark/offline_store_spark_runner.py")
	if err != nil {
		log.Fatalf("err %v", err)
	}
	err = os.Setenv("MATERIALIZE_NO_TIMESTAMP_QUERY_PATH", "provider/queries/materialize_no_ts.sql")
	if err != nil {
		log.Fatalf("err %v", err)
	}
	err = os.Setenv("MATERIALIZE_WITH_TIMESTAMP_QUERY_PATH", "provider/queries/materialize_ts.sql")
	if err != nil {
		log.Fatalf("err %v", err)
	}
	if _, set := os.LookupEnv("FF_STATE_PROVIDER"); !set {
		log.Println("FF_STATE_PROVIDER not set, defaulting to memory")
		if err := os.Setenv("FF_STATE_PROVIDER", "memory"); err != nil {
			log.Fatalf("err %v", err)
		}
	} else {
		log.Println("FF_STATE_PROVIDER set to", os.Getenv("FF_STATE_PROVIDER"))
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
	initTimeout := time.Second * 10

	logger := logging.NewLogger("init-logger")
	defer logger.Sync()
	logger.Info("Parsing Featureform App Config")
	appConfig, err := config.Get(logger)
	if err != nil {
		logger.Errorw("Invalid App Config", "err", err)
		panic(err)
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), initTimeout)
	defer cancelFn()
	initCtx := logger.AttachToContext(ctx)
	logger.Info("Created initialization context with timeout", "timeout", initTimeout)
	logger.Debug("Creating initializer")
	init, err := bootstrap.NewInitializer(appConfig)
	if err != nil {
		logger.Errorw("Failed to bootstrap service from config", "err", err)
		panic(err)
	}
	defer logger.LogIfErr("Failed to close service-level resources", init.Close())

	logger.Debug("Getting task metadata manager")
	manager, err := init.GetOrCreateTaskMetadataManager(initCtx)
	if err != nil {
		panic(err.Error())
	}

	// PPROF
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Printf("pprof server failed to start: %v", err)
		}
	}()

	/****************************************** API Server ************************************************************/

	go func() {
		err := api.StartHttpsServer(":8443")
		if err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("health check HTTP server failed: %+v", err))
		}
	}()

	/******************************************** Metadata ************************************************************/

	mLogger := logging.NewLogger("metadata")

	config := &metadata.Config{
		Logger:      mLogger,
		Address:     fmt.Sprintf(":%s", metadataPort),
		TaskManager: manager,
	}

	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}

	/******************************************** Coordinator ************************************************************/

	cLogger := logging.NewLogger("coordinator")
	defer cLogger.Sync()
	cLogger.Debug("Connected to ETCD")

	client, err := metadata.NewClient(metadataConn, cLogger)
	if err != nil {
		cLogger.Errorw("Failed to connect: %v", err)
		panic(err)
	}

	sconfig := coordinator.SchedulerConfig{
		TaskPollInterval:       1 * time.Second,
		TaskStatusSyncInterval: 1 * time.Minute,
		DependencyPollInterval: 1 * time.Second,
	}
	scheduler := coordinator.NewScheduler(client, cLogger, &spawner.MemoryJobSpawner{}, manager.Storage.Locker, sconfig)

	/**************************************** Dashboard Backend *******************************************************/
	dbLogger := logging.NewLogger("dashboard-metadata")

	dbLogger.Infof("Serving metadata at: %s\n", metadataConn)

	metadataServer, err := dm.NewMetadataServer(dbLogger, client, manager.Storage)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}

	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	dbLogger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)

	/**************************************** Serving *******************************************************/

	sLogger := logging.NewLogger("serving")

	sLogger.Infof("Serving feature serving at: %s\n", servingConn)

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
		logger = logging.NewLogger("api")
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
		err = scheduler.Start()
		if err != nil {
			cLogger.Errorw(err.Error())
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
