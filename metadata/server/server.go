// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"github.com/featureform/metadata/search"
	"os"

	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	"go.uber.org/zap"
)

func main() {

	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	logger := zap.NewExample().Sugar()
	addr := help.GetEnv("METADATA_PORT", "8080")
	enableSearch := help.GetEnv("ENABLE_SEARCH", "true")
	storageProvider := metadata.EtcdStorageProvider{
		metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{etcdHost, etcdPort},
			},
		},
	}
	config := &metadata.Config{
		Logger:          logger,
		Address:         fmt.Sprintf(":%s", addr),
		StorageProvider: storageProvider,
	}
	if enableSearch == "true" {
		logger.Infow("Connecting to search", "host", os.Getenv("MEILISEARCH_HOST"), "port", os.Getenv("MEILISEARCH_PORT"))
		config.SearchParams = &search.MeilisearchParams{
			Port:   help.GetEnv("MEILISEARCH_PORT", "7700"),
			Host:   help.GetEnv("MEILISEARCH_HOST", "localhost"),
			ApiKey: help.GetEnv("MEILISEARCH_APIKEY", ""),
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
