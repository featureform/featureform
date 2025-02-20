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

	"github.com/featureform/config"
	"github.com/featureform/config/bootstrap"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
)

func main() {
	logger := logging.NewLogger("dashboard-metadata")
	defer logger.Sync()
	logger.Info("Parsing Featureform App Config")
	appConfig, err := config.Get(logger)
	if err != nil {
		logger.Errorw("Invalid App Config", "err", err)
		panic(err)
	}
	initTimeout := appConfig.InitTimeout
	logger.Info("Created initialization context with timeout", "timeout", initTimeout)
	ctx, cancelFn := context.WithTimeout(context.Background(), initTimeout)
	defer cancelFn()
	initCtx := logger.AttachToContext(ctx)
	logger.Debug("Creating initializer")
	init, err := bootstrap.NewInitializer(appConfig)
	if err != nil {
		logger.Errorw("Failed to bootstrap service from config", "err", err)
		panic(err)
	}
	defer logger.LogIfErr("Failed to close service-level resources", init.Close())
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataAddress := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infof("Looking for metadata at: %s\n", metadataAddress)
	client, err := metadata.NewClient(metadataAddress, logger)
	if err != nil {
		logger.Panicw("Failed to connect", "error", err)
	}

	logger.Info("Getting task metadata manager")
	manager, err := init.GetOrCreateTaskMetadataManager(initCtx)
	if err != nil {
		panic(err.Error())
	}

	metadataServer, err := dm.NewMetadataServer(logger, client, manager.Storage)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	logger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)
	disableCors := help.GetEnvBool("DISABLE_CORS", false)
	err = metadataServer.Start(metadataServingPort, disableCors)
	if err != nil {
		panic(err)
	}
}
