// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"fmt"

	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
	"github.com/featureform/metadata/search"
	"github.com/featureform/scheduling"
	"go.uber.org/zap"
)

func main() {
	logger := logging.WrapZapLogger(zap.NewExample().Sugar())
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	searchHost := help.GetEnv("MEILISEARCH_HOST", "localhost")
	searchPort := help.GetEnv("MEILISEARCH_PORT", "7700")
	searchEndpoint := fmt.Sprintf("http://%s:%s", searchHost, searchPort)
	searchApiKey := help.GetEnv("MEILISEARCH_APIKEY", "")
	logger.Infof("Connecting to typesense at: %s\n", searchEndpoint)
	sc, err := search.NewMeilisearch(&search.MeilisearchParams{
		Host:   searchHost,
		Port:   searchPort,
		ApiKey: searchApiKey,
	})
	if err != nil {
		logger.Panicw("Failed to create new meil search", err)
	}
	dm.SearchClient = sc
	metadataAddress := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infof("Looking for metadata at: %s\n", metadataAddress)
	client, err := metadata.NewClient(metadataAddress, logger)
	if err != nil {
		logger.Panicw("Failed to connect", "error", err)
	}

	managerType := help.GetEnv("FF_STATE_PROVIDER", "ETCD")

	manager, err := scheduling.NewTaskMetadataManagerFromEnv(scheduling.TaskMetadataManagerType(managerType))
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
