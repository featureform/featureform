// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/search"
	"github.com/featureform/storage"
	"os"

	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
)

func main() {

	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	logger := logging.NewLogger("metadata")
	addr := help.GetEnv("METADATA_PORT", "8080")
	enableSearch := help.GetEnv("ENABLE_SEARCH", "true")

	etcdConfig := help.ETCDConfig{
		Host: etcdHost,
		Port: etcdPort,
	}

	etcdStore, err := storage.NewETCDStorageImplementation(etcdConfig)
	if err != nil {
		logger.Panicw("Failed to create storage implementation", "error", err)
	}
	locker, err := ffsync.NewETCDLocker(etcdConfig)
	if err != nil {
		logger.Panicw("Failed to create locker implementation", "error", err)
	}
	store := storage.MetadataStorage{
		Locker:  locker,
		Storage: etcdStore,
	}

	config := &metadata.Config{
		Logger:          logger,
		Address:         fmt.Sprintf(":%s", addr),
		StorageProvider: store,
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
