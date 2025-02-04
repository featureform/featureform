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
	"os"

	"github.com/featureform/config"
	"github.com/featureform/config/bootstrap"
	"github.com/featureform/db"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/metadata/search"
)

func main() {
	addr := helpers.GetEnv("METADATA_PORT", "8080")
	enableSearch := helpers.GetEnv("ENABLE_SEARCH", "true")

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
	initCtx := logger.AttachToContext(ctx)
	logger.Debug("Creating initializer")
	init, err := bootstrap.NewInitializer(appConfig)
	if err != nil {
		logger.Errorw("Failed to bootstrap service from config", "err", err)
		panic(err)
	}
	defer logger.LogIfErr("Failed to close service-level resources", init.Close())

	if config.ShouldRunGooseMigrationMetadata() {
		logger.Info("Running goose migrations for metadata")
		if err := db.RunMigrations(initCtx, appConfig.Postgres, config.GetMigrationPath()); err != nil {
			logger.Errorw("Failed to run goose migrations for metadata", "err", err)
			panic(err)
		}
	}

	logger.Info("Getting task metadata manager")
	manager, err := init.GetOrCreateTaskMetadataManager(initCtx)
	if err != nil {
		panic(err.Error())
	}

	config := &metadata.Config{
		Logger:      logger,
		Address:     fmt.Sprintf(":%s", addr),
		TaskManager: manager,
	}
	if enableSearch == "true" {
		logger.Infow("Connecting to search", "host", os.Getenv("MEILISEARCH_HOST"), "port", os.Getenv("MEILISEARCH_PORT"))
		config.SearchParams = &search.MeilisearchParams{
			Port:   helpers.GetEnv("MEILISEARCH_PORT", "7700"),
			Host:   helpers.GetEnv("MEILISEARCH_HOST", "localhost"),
			ApiKey: helpers.GetEnv("MEILISEARCH_APIKEY", ""),
		}
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	if err := server.Serve(); err != nil {
		logger.Errorw("Serve failed with error", "Err", err)
	}
}
