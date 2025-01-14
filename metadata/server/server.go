// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"fmt"
	"os"

	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/metadata/search"
	"github.com/featureform/scheduling"

	"github.com/featureform/helpers"
)

func main() {
	logger := logging.NewLogger("metadata")
	addr := helpers.GetEnv("METADATA_PORT", "8080")
	enableSearch := helpers.GetEnv("ENABLE_SEARCH", "true")

	managerType := helpers.GetEnv("FF_STATE_PROVIDER", string(scheduling.ETCDMetadataManager))

	manager, err := scheduling.NewTaskMetadataManagerFromEnv(scheduling.TaskMetadataManagerType(managerType))
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
