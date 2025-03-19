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
	"github.com/featureform/db"
	"github.com/featureform/health"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
)

func main() {
	addr := helpers.GetEnv("METADATA_PORT", "8080")

	logger := logging.NewLogger("metadata")
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
	ctx = logger.AttachToContext(ctx)
	logger.Debug("Creating initializer")
	init, err := bootstrap.NewInitializer(appConfig)
	if err != nil {
		logger.Errorw("Failed to bootstrap service from config", "err", err)
		panic(err)
	}
	defer logger.LogIfErr("Failed to close service-level resources", init.Close())

	if config.ShouldRunGooseMigrationMetadata() {
		logger.Info("Running goose migrations for metadata")
		pgPool, err := init.GetOrCreatePostgresPool(ctx)
		if err != nil {
			logger.Errorw("Failed to get postgres pool", "err", err)
			panic(err)
		}
		if err := db.RunMigrations(ctx, pgPool, config.GetMigrationPath()); err != nil {
			logger.Errorw("Failed to run goose migrations for metadata", "err", err)
			panic(err)
		}
	}

	logger.Info("Getting task metadata manager")
	manager, err := init.GetOrCreateTaskMetadataManager(ctx)
	if err != nil {
		panic(err.Error())
	}

	apiStatusPort := helpers.GetEnv("API_STATUS_PORT", "8443")
	logger.Infow("Retrieved API status port from ENV", "port", apiStatusPort)
	if err = health.StartHttpServer(logger, apiStatusPort); err != nil {
		logger.Errorw("Failed to start health check", "err", err)
		panic(err)
	}

	config := &metadata.Config{
		Logger:      logger,
		Address:     fmt.Sprintf(":%s", addr),
		TaskManager: manager,
	}

	server, err := metadata.NewMetadataServer(ctx, config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	if err := server.Serve(); err != nil {
		logger.Errorw("Serve failed with error", "Err", err)
	}
}
