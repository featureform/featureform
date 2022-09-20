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
	disableTypesense := help.GetEnv("DISABLE_TYPESENSE", "false")
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
	if disableTypesense == "true" {
		fmt.Println("TS Port", os.Getenv("TYPESENSE_PORT"), "TS HOST", os.Getenv("TYPESENSE_HOST"), "TS KEY", os.Getenv("TYPESENSE_APIKEY"))
		config.TypeSenseParams = &search.TypeSenseParams{
			Port:   help.GetEnv("TYPESENSE_PORT", "8108"),
			Host:   help.GetEnv("TYPESENSE_HOST", "localhost"),
			ApiKey: help.GetEnv("TYPESENSE_APIKEY", "xyz"),
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
